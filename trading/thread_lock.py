import asyncio
from threading import Lock
from functools import wraps
import logging
from typing import Callable
import time

class ThreadLock:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ThreadLock, cls).__new__(cls)
            cls._instance.lock = Lock()
            cls._instance.current_thread = None
            cls._instance.logger = logging.getLogger(__name__)
        return cls._instance

    def acquire_lock(self, thread_id: int, operation: str) -> bool:
        """락 획득 시도"""
        if self.lock.acquire(blocking=False):
            self.current_thread = thread_id
            self.logger.info(f"Thread {thread_id} acquired lock for {operation}")
            return True
        return False

    def release_lock(self, thread_id: int):
        """락 해제"""
        if self.current_thread == thread_id:
            self.lock.release()
            self.current_thread = None
            self.logger.info(f"Thread {thread_id} released lock")

def with_thread_lock(operation: str):
    """데코레이터: 스레드 락 적용"""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            thread_id = getattr(self, 'thread_id', 0)
            lock_manager = ThreadLock()
            
            # 락 획득 시도 (최대 3회)
            for attempt in range(3):
                if lock_manager.acquire_lock(thread_id, operation):
                    try:
                        return await func(self, *args, **kwargs)
                    finally:
                        lock_manager.release_lock(thread_id)
                else:
                    await asyncio.sleep(1)
            
            raise RuntimeError(f"Thread {thread_id} failed to acquire lock for {operation}")
        return wrapper
    return decorator 