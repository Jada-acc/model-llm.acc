from typing import Dict, Any, Optional
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
import logging

logger = logging.getLogger(__name__)

class ModelManager:
    """Manage LLM models and inference."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.model = None
        self.tokenizer = None
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    
    async def load_model(self, model_name: str) -> bool:
        """Load model and tokenizer."""
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.model = AutoModelForCausalLM.from_pretrained(
                model_name,
                device_map='auto',
                torch_dtype=torch.float16
            )
            logger.info(f"Successfully loaded model: {model_name}")
            return True
        except Exception as e:
            logger.error(f"Error loading model: {str(e)}")
            return False
    
    async def generate(
        self,
        prompt: str,
        max_length: int = 100,
        temperature: float = 0.7
    ) -> Optional[str]:
        """Generate text from prompt."""
        try:
            if not self.model or not self.tokenizer:
                raise ValueError("Model not loaded")
            
            inputs = self.tokenizer(prompt, return_tensors="pt").to(self.device)
            outputs = self.model.generate(
                **inputs,
                max_length=max_length,
                temperature=temperature,
                do_sample=True
            )
            
            return self.tokenizer.decode(outputs[0], skip_special_tokens=True)
            
        except Exception as e:
            logger.error(f"Error generating text: {str(e)}")
            return None 