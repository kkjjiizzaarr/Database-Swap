"""Rate limiting functionality for database operations."""

import time
import asyncio
from typing import Optional
from threading import Lock


class RateLimiter:
    """Rate limiter to control the speed of database operations."""
    
    def __init__(self, delay: float = 0.1, max_concurrent: int = 1):
        """
        Initialize rate limiter.
        
        Args:
            delay: Delay in seconds between operations
            max_concurrent: Maximum concurrent operations (for future async support)
        """
        self.delay = delay
        self.max_concurrent = max_concurrent
        self.last_operation_time = 0.0
        self.lock = Lock()
    
    def wait(self) -> None:
        """Wait for the appropriate delay before next operation."""
        if self.delay <= 0:
            return
        
        with self.lock:
            current_time = time.time()
            elapsed = current_time - self.last_operation_time
            
            if elapsed < self.delay:
                sleep_time = self.delay - elapsed
                time.sleep(sleep_time)
            
            self.last_operation_time = time.time()
    
    def set_delay(self, delay: float) -> None:
        """Update the delay between operations."""
        with self.lock:
            self.delay = max(0.0, delay)
    
    def get_delay(self) -> float:
        """Get current delay setting."""
        return self.delay
    
    def reset(self) -> None:
        """Reset the rate limiter state."""
        with self.lock:
            self.last_operation_time = 0.0


class AdaptiveRateLimiter(RateLimiter):
    """Rate limiter that adapts based on error rates."""
    
    def __init__(self, initial_delay: float = 0.1, min_delay: float = 0.01, 
                 max_delay: float = 5.0, error_threshold: float = 0.1):
        """
        Initialize adaptive rate limiter.
        
        Args:
            initial_delay: Initial delay between operations
            min_delay: Minimum delay (won't go below this)
            max_delay: Maximum delay (won't go above this)
            error_threshold: Error rate threshold to trigger adaptation
        """
        super().__init__(initial_delay)
        self.initial_delay = initial_delay
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.error_threshold = error_threshold
        
        # Error tracking
        self.total_operations = 0
        self.total_errors = 0
        self.recent_operations = []
        self.recent_errors = []
        self.window_size = 100  # Track last 100 operations
    
    def record_operation(self, success: bool) -> None:
        """Record the result of an operation."""
        with self.lock:
            self.total_operations += 1
            self.recent_operations.append(success)
            
            if not success:
                self.total_errors += 1
                self.recent_errors.append(self.total_operations)
            
            # Keep only recent operations in memory
            if len(self.recent_operations) > self.window_size:
                self.recent_operations.pop(0)
            
            # Clean old errors
            cutoff = self.total_operations - self.window_size
            self.recent_errors = [e for e in self.recent_errors if e > cutoff]
            
            # Adapt rate based on recent error rate
            self._adapt_rate()
    
    def _adapt_rate(self) -> None:
        """Adapt the rate limit based on recent error rates."""
        if len(self.recent_operations) < 10:  # Not enough data
            return
        
        # Calculate recent error rate
        recent_error_count = len(self.recent_errors)
        recent_operation_count = len(self.recent_operations)
        error_rate = recent_error_count / recent_operation_count
        
        if error_rate > self.error_threshold:
            # Increase delay (slow down)
            self.delay = min(self.delay * 1.5, self.max_delay)
        elif error_rate < self.error_threshold / 2:
            # Decrease delay (speed up)
            self.delay = max(self.delay * 0.8, self.min_delay)
    
    def get_stats(self) -> dict:
        """Get rate limiter statistics."""
        with self.lock:
            total_error_rate = (self.total_errors / self.total_operations 
                              if self.total_operations > 0 else 0.0)
            
            recent_error_count = len(self.recent_errors)
            recent_operation_count = len(self.recent_operations)
            recent_error_rate = (recent_error_count / recent_operation_count 
                               if recent_operation_count > 0 else 0.0)
            
            return {
                'current_delay': self.delay,
                'total_operations': self.total_operations,
                'total_errors': self.total_errors,
                'total_error_rate': total_error_rate,
                'recent_error_rate': recent_error_rate,
                'recent_operations': recent_operation_count
            }
    
    def reset(self) -> None:
        """Reset the rate limiter to initial state."""
        with self.lock:
            super().reset()
            self.delay = self.initial_delay
            self.total_operations = 0
            self.total_errors = 0
            self.recent_operations.clear()
            self.recent_errors.clear()


class BatchRateLimiter:
    """Rate limiter for batch operations."""
    
    def __init__(self, batch_delay: float = 1.0, operations_per_batch: int = 1000):
        """
        Initialize batch rate limiter.
        
        Args:
            batch_delay: Delay between batches
            operations_per_batch: Number of operations per batch
        """
        self.batch_delay = batch_delay
        self.operations_per_batch = operations_per_batch
        self.current_batch_count = 0
        self.last_batch_time = 0.0
        self.lock = Lock()
    
    def should_wait(self) -> bool:
        """Check if we should wait before next operation."""
        with self.lock:
            self.current_batch_count += 1
            
            if self.current_batch_count >= self.operations_per_batch:
                return True
            
            return False
    
    def wait_for_batch(self) -> None:
        """Wait for batch delay and reset counter."""
        with self.lock:
            if self.batch_delay > 0:
                current_time = time.time()
                elapsed = current_time - self.last_batch_time
                
                if elapsed < self.batch_delay:
                    sleep_time = self.batch_delay - elapsed
                    time.sleep(sleep_time)
            
            self.current_batch_count = 0
            self.last_batch_time = time.time()
    
    def reset(self) -> None:
        """Reset batch counter."""
        with self.lock:
            self.current_batch_count = 0
            self.last_batch_time = 0.0