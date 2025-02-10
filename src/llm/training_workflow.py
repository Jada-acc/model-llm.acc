from typing import Dict, Any, Optional, List, Tuple
import logging
import os
from pathlib import Path
import torch
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel
from torch.utils.data import DataLoader, DistributedSampler
from transformers import (
    PreTrainedModel,
    PreTrainedTokenizer,
    get_linear_schedule_with_warmup,
    get_cosine_schedule_with_warmup
)
from tqdm import tqdm
import wandb
from datetime import datetime

from .data_preparation import DataPreparationPipeline
from .model_registry import ModelRegistry

logger = logging.getLogger(__name__)

class TrainingWorkflow:
    """Workflow for training LLM models."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize training workflow."""
        self.config = config
        self.output_dir = Path(config.get('output_dir', 'outputs'))
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Training settings
        self.max_epochs = config.get('max_epochs', 3)
        self.learning_rate = config.get('learning_rate', 5e-5)
        self.warmup_steps = config.get('warmup_steps', 0)
        self.gradient_accumulation_steps = config.get('gradient_accumulation_steps', 1)
        self.max_grad_norm = config.get('max_grad_norm', 1.0)
        self.fp16 = config.get('fp16', False)
        
        # Additional optimization settings
        self.gradient_checkpointing = config.get('gradient_checkpointing', False)
        self.optimizer_type = config.get('optimizer_type', 'adamw')
        self.loss_scaling = config.get('loss_scaling', 'dynamic')
        self.zero_optimization = config.get('zero_optimization', False)
        self.memory_efficient_fp16 = config.get('memory_efficient_fp16', False)
        
        # Initialize components
        self.model_registry = ModelRegistry(config.get('model_registry', {}))
        self.data_pipeline = DataPreparationPipeline(config.get('data_pipeline', {}))
        
        # Setup distributed training
        self.distributed = config.get('distributed', False)
        if self.distributed:
            self._setup_distributed()
        
        # Initialize tracking
        self.use_wandb = config.get('use_wandb', False)
        if self.use_wandb:
            self._setup_wandb()
        
        # Training state
        self.global_step = 0
        self.epoch = 0
        self.best_loss = float('inf')
    
    def _setup_distributed(self):
        """Setup distributed training."""
        if not dist.is_initialized():
            dist.init_process_group(backend='nccl')
        self.world_size = dist.get_world_size()
        self.rank = dist.get_rank()
        self.local_rank = int(os.environ.get('LOCAL_RANK', 0))
        
        # Set device
        torch.cuda.set_device(self.local_rank)
    
    def _setup_wandb(self):
        """Setup Weights & Biases tracking."""
        wandb.init(
            project=self.config.get('wandb_project', 'llm-training'),
            name=self.config.get('wandb_run_name'),
            config=self.config
        )
    
    def train(
        self,
        model: PreTrainedModel,
        tokenizer: PreTrainedTokenizer,
        train_data: Optional[DataLoader] = None,
        eval_data: Optional[DataLoader] = None
    ) -> Dict[str, Any]:
        """Run training workflow."""
        try:
            # Prepare data if not provided
            if train_data is None:
                train_data = self.data_pipeline.prepare_data(tokenizer)
            
            # Setup model for training
            model = self._setup_model(model)
            optimizer = self._setup_optimizer(model)
            scheduler = self._setup_scheduler(optimizer, len(train_data))
            
            # Setup mixed precision training
            scaler = torch.cuda.amp.GradScaler() if self.fp16 else None
            
            # Training loop
            logger.info("Starting training...")
            for epoch in range(self.max_epochs):
                self.epoch = epoch
                train_loss = self._train_epoch(
                    model,
                    train_data,
                    optimizer,
                    scheduler,
                    scaler
                )
                
                # Evaluation
                if eval_data:
                    eval_loss = self._evaluate(model, eval_data)
                    self._save_checkpoint(model, optimizer, train_loss, eval_loss)
                else:
                    self._save_checkpoint(model, optimizer, train_loss)
                
                # Log metrics
                self._log_metrics({
                    'epoch': epoch,
                    'train_loss': train_loss,
                    'eval_loss': eval_loss if eval_data else None,
                    'learning_rate': scheduler.get_last_lr()[0]
                })
            
            # Save final model
            if self.rank == 0:
                self._save_model(model, tokenizer)
            
            return self.get_training_info()
            
        except Exception as e:
            logger.error(f"Error in training: {e}")
            raise
        finally:
            if self.use_wandb:
                wandb.finish()
    
    def _train_epoch(
        self,
        model: PreTrainedModel,
        dataloader: DataLoader,
        optimizer: torch.optim.Optimizer,
        scheduler: torch.optim.lr_scheduler._LRScheduler,
        scaler: Optional[torch.cuda.amp.GradScaler]
    ) -> float:
        """Train for one epoch."""
        model.train()
        total_loss = 0
        optimizer.zero_grad()
        
        with tqdm(total=len(dataloader), desc=f"Epoch {self.epoch}") as pbar:
            for step, batch in enumerate(dataloader):
                # Move batch to device
                batch = {k: v.to(model.device) for k, v in batch.items()}
                
                # Forward pass with mixed precision
                if scaler:
                    with torch.cuda.amp.autocast():
                        outputs = model(**batch)
                        loss = outputs.loss / self.gradient_accumulation_steps
                    
                    # Backward pass with gradient scaling
                    scaler.scale(loss).backward()
                    
                    if (step + 1) % self.gradient_accumulation_steps == 0:
                        scaler.unscale_(optimizer)
                        torch.nn.utils.clip_grad_norm_(model.parameters(), self.max_grad_norm)
                        scaler.step(optimizer)
                        scaler.update()
                        scheduler.step()
                        optimizer.zero_grad()
                else:
                    outputs = model(**batch)
                    loss = outputs.loss / self.gradient_accumulation_steps
                    loss.backward()
                    
                    if (step + 1) % self.gradient_accumulation_steps == 0:
                        torch.nn.utils.clip_grad_norm_(model.parameters(), self.max_grad_norm)
                        optimizer.step()
                        scheduler.step()
                        optimizer.zero_grad()
                
                # Update metrics
                total_loss += loss.item()
                self.global_step += 1
                
                # Update progress bar
                pbar.update(1)
                pbar.set_postfix({'loss': loss.item()})
                
                # Log step metrics
                if self.use_wandb and step % 100 == 0:
                    self._log_metrics({
                        'train/loss': loss.item(),
                        'train/lr': scheduler.get_last_lr()[0]
                    })
        
        return total_loss / len(dataloader)
    
    def _evaluate(
        self,
        model: PreTrainedModel,
        dataloader: DataLoader
    ) -> float:
        """Evaluate model."""
        model.eval()
        total_loss = 0
        
        with torch.no_grad():
            for batch in tqdm(dataloader, desc="Evaluating"):
                batch = {k: v.to(model.device) for k, v in batch.items()}
                outputs = model(**batch)
                total_loss += outputs.loss.item()
        
        return total_loss / len(dataloader)
    
    def _setup_model(self, model: PreTrainedModel) -> PreTrainedModel:
        """Setup model for training with optimizations."""
        device = torch.device(f'cuda:{self.local_rank}' if torch.cuda.is_available() else 'cpu')
        model = model.to(device)
        
        # Enable gradient checkpointing
        if self.gradient_checkpointing and hasattr(model, 'gradient_checkpointing_enable'):
            model.gradient_checkpointing_enable()
        
        # Memory optimizations
        if self.memory_efficient_fp16:
            from torch.cuda.amp import custom_fwd, custom_bwd
            model.forward = custom_fwd(custom_bwd(model.forward))
        
        if self.distributed:
            model = DistributedDataParallel(
                model,
                device_ids=[self.local_rank],
                output_device=self.local_rank,
                find_unused_parameters=False  # Optimization
            )
        
        return model
    
    def _setup_optimizer(self, model: PreTrainedModel) -> torch.optim.Optimizer:
        """Setup optimizer with advanced options."""
        no_decay = ['bias', 'LayerNorm.weight']
        optimizer_grouped_parameters = [
            {
                'params': [p for n, p in model.named_parameters() 
                          if not any(nd in n for nd in no_decay)],
                'weight_decay': self.config.get('weight_decay', 0.01)
            },
            {
                'params': [p for n, p in model.named_parameters() 
                          if any(nd in n for nd in no_decay)],
                'weight_decay': 0.0
            }
        ]
        
        if self.optimizer_type == 'adamw':
            optimizer = torch.optim.AdamW(
                optimizer_grouped_parameters,
                lr=self.learning_rate,
                eps=self.config.get('adam_epsilon', 1e-8),
                betas=self.config.get('adam_betas', (0.9, 0.999))
            )
        elif self.optimizer_type == 'adafactor':
            from transformers import Adafactor
            optimizer = Adafactor(
                optimizer_grouped_parameters,
                lr=self.learning_rate,
                scale_parameter=True,
                relative_step=False
            )
        elif self.optimizer_type == 'lion':
            try:
                from lion_pytorch import Lion
                optimizer = Lion(
                    optimizer_grouped_parameters,
                    lr=self.learning_rate,
                    weight_decay=self.config.get('weight_decay', 0.01)
                )
            except ImportError:
                logger.warning("Lion optimizer not available, falling back to AdamW")
                optimizer = torch.optim.AdamW(optimizer_grouped_parameters, lr=self.learning_rate)
        
        # ZeRO optimizer if enabled
        if self.zero_optimization:
            try:
                from deepspeed.runtime.zero.stage2 import DeepSpeedZeroOptimizer
                optimizer = DeepSpeedZeroOptimizer(optimizer, static_loss_scale=None)
            except ImportError:
                logger.warning("DeepSpeed not available, skipping ZeRO optimization")
        
        return optimizer
    
    def _setup_scheduler(
        self,
        optimizer: torch.optim.Optimizer,
        num_training_steps: int
    ) -> torch.optim.lr_scheduler._LRScheduler:
        """Setup learning rate scheduler."""
        scheduler_type = self.config.get('scheduler_type', 'linear')
        
        if scheduler_type == 'linear':
            return get_linear_schedule_with_warmup(
                optimizer,
                num_warmup_steps=self.warmup_steps,
                num_training_steps=num_training_steps
            )
        elif scheduler_type == 'cosine':
            return get_cosine_schedule_with_warmup(
                optimizer,
                num_warmup_steps=self.warmup_steps,
                num_training_steps=num_training_steps
            )
        else:
            raise ValueError(f"Unsupported scheduler type: {scheduler_type}")
    
    def _save_checkpoint(
        self,
        model: PreTrainedModel,
        optimizer: torch.optim.Optimizer,
        train_loss: float,
        eval_loss: Optional[float] = None
    ):
        """Save training checkpoint."""
        if self.rank != 0:
            return
            
        loss_for_save = eval_loss if eval_loss is not None else train_loss
        if loss_for_save < self.best_loss:
            self.best_loss = loss_for_save
            checkpoint = {
                'epoch': self.epoch,
                'global_step': self.global_step,
                'model_state_dict': model.state_dict(),
                'optimizer_state_dict': optimizer.state_dict(),
                'loss': loss_for_save,
                'config': self.config
            }
            
            path = self.output_dir / f'checkpoint-{self.global_step}.pt'
            torch.save(checkpoint, path)
            logger.info(f"Saved checkpoint: {path}")
    
    def _save_model(
        self,
        model: PreTrainedModel,
        tokenizer: PreTrainedTokenizer
    ):
        """Save final model."""
        output_path = self.output_dir / f'model-{self.global_step}'
        model.save_pretrained(output_path)
        tokenizer.save_pretrained(output_path)
        logger.info(f"Saved model: {output_path}")
    
    def _log_metrics(self, metrics: Dict[str, Any]):
        """Log metrics."""
        if self.use_wandb:
            wandb.log(metrics, step=self.global_step)
    
    def get_training_info(self) -> Dict[str, Any]:
        """Get training information."""
        return {
            'config': self.config,
            'epochs_completed': self.epoch + 1,
            'global_steps': self.global_step,
            'best_loss': self.best_loss,
            'output_dir': str(self.output_dir),
            'timestamp': datetime.now().isoformat()
        } 