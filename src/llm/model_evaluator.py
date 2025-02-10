from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import numpy as np
from sklearn.metrics import accuracy_score, precision_recall_fscore_support
import torch
from .inference_pipeline import InferencePipeline

logger = logging.getLogger(__name__)

class ModelEvaluator:
    """Component for evaluating model performance and quality."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize model evaluator."""
        self.config = config
        self.pipeline = InferencePipeline(config.get('inference_pipeline', {}))
        
        # Evaluation settings
        self.metrics = config.get('metrics', ['accuracy', 'perplexity', 'latency'])
        self.num_samples = config.get('num_samples', 1000)
        self.batch_size = config.get('batch_size', 32)
        
        # Evaluation results
        self.results: Dict[str, Dict[str, Any]] = {}
    
    async def evaluate_model(
        self,
        model_name: str,
        version: str,
        eval_data: List[Dict[str, Any]],
        metrics: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Evaluate model on specified metrics."""
        try:
            metrics = metrics or self.metrics
            start_time = datetime.now()
            
            # Initialize results
            evaluation_id = f"{model_name}_{version}_{start_time.isoformat()}"
            self.results[evaluation_id] = {
                'model_name': model_name,
                'version': version,
                'metrics': {},
                'samples_evaluated': len(eval_data),
                'start_time': start_time.isoformat()
            }
            
            # Evaluate each metric
            for metric in metrics:
                metric_value = await self._evaluate_metric(
                    metric,
                    model_name,
                    version,
                    eval_data
                )
                self.results[evaluation_id]['metrics'][metric] = metric_value
            
            # Add completion time
            self.results[evaluation_id]['end_time'] = datetime.now().isoformat()
            self.results[evaluation_id]['duration_seconds'] = (
                datetime.now() - start_time
            ).total_seconds()
            
            return self.results[evaluation_id]
            
        except Exception as e:
            logger.error(f"Error evaluating model: {e}")
            raise
    
    async def _evaluate_metric(
        self,
        metric: str,
        model_name: str,
        version: str,
        eval_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Evaluate a specific metric."""
        if metric == 'accuracy':
            return await self._evaluate_accuracy(model_name, version, eval_data)
        elif metric == 'perplexity':
            return await self._evaluate_perplexity(model_name, version, eval_data)
        elif metric == 'latency':
            return await self._evaluate_latency(model_name, version, eval_data)
        else:
            raise ValueError(f"Unsupported metric: {metric}")
    
    async def _evaluate_accuracy(
        self,
        model_name: str,
        version: str,
        eval_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Evaluate model accuracy."""
        predictions = []
        references = []
        
        for batch in self._batch_data(eval_data):
            prompts = [item['prompt'] for item in batch]
            results = await self.pipeline.generate(
                prompts,
                model_name=model_name,
                version=version
            )
            
            for i, result in enumerate(results):
                predictions.append(result['generated'])
                references.append(batch[i]['expected'])
        
        # Calculate metrics
        accuracy = accuracy_score(references, predictions)
        precision, recall, f1, _ = precision_recall_fscore_support(
            references,
            predictions,
            average='weighted'
        )
        
        return {
            'accuracy': float(accuracy),
            'precision': float(precision),
            'recall': float(recall),
            'f1': float(f1)
        }
    
    async def _evaluate_perplexity(
        self,
        model_name: str,
        version: str,
        eval_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Evaluate model perplexity."""
        total_loss = 0
        total_tokens = 0
        
        model_data = self.pipeline.model_registry.load_model(
            model_name,
            version,
            self.pipeline.device
        )
        
        if not model_data:
            raise ValueError(f"Failed to load model {model_name}:{version}")
        
        model = model_data['model']
        tokenizer = model_data['tokenizer']
        
        try:
            for batch in self._batch_data(eval_data):
                # Tokenize inputs
                inputs = tokenizer(
                    [item['prompt'] for item in batch],
                    padding=True,
                    truncation=True,
                    return_tensors='pt'
                )
                if hasattr(inputs, 'to'):
                    inputs = inputs.to(self.pipeline.device)
                elif isinstance(inputs, dict):
                    inputs = {k: v.to(self.pipeline.device) if hasattr(v, 'to') else v 
                            for k, v in inputs.items()}
                
                # Get target outputs
                labels = tokenizer(
                    [item['expected'] for item in batch],
                    padding=True,
                    truncation=True,
                    return_tensors='pt'
                )
                if hasattr(labels, 'to'):
                    labels = labels.to(self.pipeline.device)
                elif isinstance(labels, dict):
                    labels = {k: v.to(self.pipeline.device) if hasattr(v, 'to') else v 
                            for k, v in labels.items()}
                
                # Calculate loss
                with torch.inference_mode():
                    if isinstance(labels, dict):
                        outputs = model(**inputs, labels=labels['input_ids'])
                    else:
                        outputs = model(**inputs, labels=labels)
                    loss = outputs.loss.item()
                
                # Update totals
                if isinstance(labels, dict):
                    token_count = labels['input_ids'].ne(tokenizer.pad_token_id).sum().item()
                else:
                    token_count = labels.ne(tokenizer.pad_token_id).sum().item()
                    
                total_loss += loss * token_count
                total_tokens += token_count
            
            perplexity = np.exp(total_loss / total_tokens)
            
            return {
                'perplexity': float(perplexity),
                'avg_loss': float(total_loss / total_tokens)
            }
            
        except Exception as e:
            logger.error(f"Error calculating perplexity: {e}")
            raise
    
    async def _evaluate_latency(
        self,
        model_name: str,
        version: str,
        eval_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Evaluate model latency."""
        latencies = []
        token_throughputs = []
        
        for batch in self._batch_data(eval_data):
            start_time = datetime.now()
            
            results = await self.pipeline.generate(
                [item['prompt'] for item in batch],
                model_name=model_name,
                version=version
            )
            
            duration = (datetime.now() - start_time).total_seconds()
            total_tokens = sum(r['tokens'] for r in results)
            
            latencies.append(duration)
            token_throughputs.append(total_tokens / duration)
        
        return {
            'avg_latency': float(np.mean(latencies)),
            'p50_latency': float(np.percentile(latencies, 50)),
            'p90_latency': float(np.percentile(latencies, 90)),
            'p99_latency': float(np.percentile(latencies, 99)),
            'avg_throughput': float(np.mean(token_throughputs)),
            'samples_processed': len(eval_data)
        }
    
    def _batch_data(self, data: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Split evaluation data into batches."""
        for i in range(0, len(data), self.batch_size):
            yield data[i:i + self.batch_size]
    
    def get_evaluation_results(
        self,
        evaluation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get evaluation results."""
        if evaluation_id:
            return self.results.get(evaluation_id, {})
        return self.results 