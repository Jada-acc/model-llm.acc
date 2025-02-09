from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

class BatchProcessor:
    """Handle batch processing for large datasets."""
    
    def __init__(self, etl_pipeline, batch_size: int = 1000, max_workers: int = 4):
        self.etl_pipeline = etl_pipeline
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.processing_queue = Queue()
        self.results = Queue()
        self.stop_event = threading.Event()
    
    def process_large_dataset(
        self,
        source: str,
        target: str,
        query: Dict[str, Any],
        transformations: List[str]
    ) -> bool:
        """Process a large dataset in batches."""
        try:
            # Start worker threads
            workers = []
            for _ in range(self.max_workers):
                worker = threading.Thread(
                    target=self._process_batch_worker,
                    args=(source, target, transformations)
                )
                worker.start()
                workers.append(worker)
            
            # Process data in batches
            offset = 0
            while not self.stop_event.is_set():
                batch_query = self._modify_query_for_batch(query, offset)
                batch = self.etl_pipeline.extract(source, batch_query)
                
                if not batch:
                    break
                
                self.processing_queue.put(batch)
                offset += len(batch)
            
            # Wait for all batches to complete
            self.processing_queue.join()
            self.stop_event.set()
            
            # Wait for workers to finish
            for worker in workers:
                worker.join()
            
            # Check results
            failed_batches = []
            while not self.results.empty():
                result = self.results.get()
                if not result['success']:
                    failed_batches.append(result['batch_id'])
            
            if failed_batches:
                logger.error(f"Failed batches: {failed_batches}")
                return False
            
            logger.info(f"Successfully processed {offset} records")
            return True
            
        except Exception as e:
            logger.error(f"Batch processing failed: {str(e)}")
            self.stop_event.set()
            return False
    
    def _process_batch_worker(self, source: str, target: str, transformations: List[str]):
        """Worker thread for processing batches."""
        while not self.stop_event.is_set():
            try:
                batch = self.processing_queue.get(timeout=1)
                batch_id = id(batch)
                
                success = self.etl_pipeline.process_batch(
                    source=source,
                    target=target,
                    query={'data': batch},  # Pass batch directly
                    transformations=transformations
                )
                
                self.results.put({
                    'batch_id': batch_id,
                    'success': success,
                    'size': len(batch)
                })
                
                self.processing_queue.task_done()
                
            except Queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Worker error: {str(e)}")
                self.stop_event.set()
    
    def _modify_query_for_batch(self, query: Dict[str, Any], offset: int) -> Dict[str, Any]:
        """Modify query to include batch limits."""
        batch_query = query.copy()
        if isinstance(batch_query['query'], str):
            batch_query['query'] = f"{batch_query['query']} LIMIT {self.batch_size} OFFSET {offset}"
        return batch_query 