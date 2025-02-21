from memory_profiler import profile as memory_profiler
import logging
from functools import wraps
import os
from datetime import datetime, timedelta
from utils.logger_config import setup_logger
from utils.time_utils import TimeUtils

class MemoryProfiler:
    def __init__(self):
        self.logger = setup_logger('memory-monitor')
        self.data_retention_days = 7  # 7일간 데이터 보관
        self.sampling_interval = 3600  # 1시간 간격으로 샘플링
        self.max_samples = 168  # 7일 * 24시간 = 168 샘플
        
    def profile_memory(self, func):
        """메모리 프로파일링 데코레이터"""
        profile_path = self._get_profile_path()
        return memory_profiler(precision=4, stream=open(profile_path, 'w+'))(func)
    
    def _get_profile_path(self):
        """프로파일 결과 저장 경로"""
        profile_dir = 'log/memory_profiles'
        os.makedirs(profile_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f'{profile_dir}/memory_profile_{timestamp}.log'
    
    def cleanup_old_data(self):
        """오래된 프로파일링 데이터 정리"""
        cutoff_date = TimeUtils.get_current_kst() - timedelta(days=self.data_retention_days)
        
        try:
            self.db.memory_profiles.delete_many({
                'timestamp': {'$lt': cutoff_date}
            })
        except Exception as e:
            self.logger.error(f"메모리 프로파일 데이터 정리 중 오류: {str(e)}") 