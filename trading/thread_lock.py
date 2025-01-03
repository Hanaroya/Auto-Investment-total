import asyncio
from threading import Lock
from functools import wraps
import logging
from typing import Callable
import time

class ThreadLock:
    """
    스레드 락 관리자
    
    여러 스레드에서 동일한 락 객체를 공유하기 위해 싱글톤 패턴을 사용합니다.
    """
    _instance = None
    
    def __new__(cls):
        """
        싱글톤 패턴 구현
        - 하나의 ThreadLock 인스턴스만 존재하도록 보장
        - 여러 스레드에서 동일한 락 객체를 공유하기 위함
        """
        if cls._instance is None:
            cls._instance = super(ThreadLock, cls).__new__(cls)
            cls._instance.lock = Lock()  # 스레드 간 동기화를 위한 락 객체
            cls._instance.current_thread = None  # 현재 락을 보유한 스레드 ID
            cls._instance.logger = logging.getLogger('investment_center')  # InvestmentCenter의 logger 사용
        return cls._instance

    def acquire_lock(self, thread_id: int, operation: str) -> bool:
        """
        락 획득을 시도하는 메서드
        Args:
            thread_id: 락을 요청하는 스레드의 ID
            operation: 수행하려는 작업의 설명
        Returns:
            bool: 락 획득 성공 여부
        """
        if self.lock.acquire(blocking=False):  # non-blocking 방식으로 락 획득 시도
            self.current_thread = thread_id
            self.logger.debug(f"Thread {thread_id} acquired lock for {operation}")
            return True
        return False

    def release_lock(self, thread_id: int):
        """
        락을 해제하는 메서드
        Args:
            thread_id: 락을 해제하려는 스레드의 ID
        Note:
            현재 락을 보유한 스레드만 해제 가능
        """
        if self.current_thread == thread_id:
            self.lock.release()
            self.current_thread = None
            self.logger.debug(f"Thread {thread_id} released lock")

def with_thread_lock(operation: str):
    """
    스레드 락을 적용하는 데코레이터
    Args:
        operation: 보호하려는 작업의 설명
    
    동작 방식:
    1. 데코레이터가 적용된 함수 호출 시 락 획득 시도
    2. 최대 3회까지 락 획득 재시도
    3. 락 획득 실패 시 RuntimeError 발생
    4. 락 획득 성공 시 원본 함수 실행 후 락 해제
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            thread_id = getattr(self, 'thread_id', 0)  # 객체에서 thread_id 가져오기
            lock_manager = ThreadLock()  # 싱글톤 락 매니저 인스턴스 획득
            
            # 락 획득 시도 (최대 3회)
            for attempt in range(3):
                if lock_manager.acquire_lock(thread_id, operation):
                    try:
                        return await func(self, *args, **kwargs)  # 원본 함수 실행
                    finally:
                        lock_manager.release_lock(thread_id)  # 항상 락 해제 보장
                else:
                    await asyncio.sleep(1)  # 재시도 전 1초 대기
            
            # 3회 시도 후에도 락 획득 실패 시 예외 발생
            raise RuntimeError(f"Thread {thread_id} failed to acquire lock for {operation}")
        return wrapper
    return decorator 