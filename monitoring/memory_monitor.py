from memory_profiler import profile
import logging
from functools import wraps
import os
from datetime import datetime
from utils.logger_config import setup_logger

class MemoryProfiler:
    def __init__(self):
        self.logger = setup_logger('memory-monitor')
        
    def profile_memory(self, func):
        """메모리 프로파일링 데코레이터"""
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 프로파일링 시작
            @profile(precision=4, stream=open(self._get_profile_path(), 'w+'))
            def wrapped_func():
                return func(*args, **kwargs)
            
            return wrapped_func()
        return wrapper
    
    def _get_profile_path(self):
        """프로파일 결과 저장 경로"""
        profile_dir = 'log/memory_profiles'
        os.makedirs(profile_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f'{profile_dir}/memory_profile_{timestamp}.log' 