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
        self.critical_methods = {
            'process_buy_signal',  # 매수 처리
            'process_sell_signal', # 매도 처리
            'analyze_market',      # 시장 분석
            'update_portfolio',     # 포트폴리오 업데이트
            'process_single_market'# 단일 마켓 처리
        }
        
    def profile_memory(self, func):
        """중요 메소드만 프로파일링"""
        if func.__name__ in self.critical_methods:
            profile_path = self._get_profile_path()
            return memory_profiler(precision=4, stream=open(profile_path, 'w+', encoding='utf-8'))(func)
        return func  # 중요하지 않은 메소드는 프로파일링 스킵
    
    def _get_profile_path(self):
        """프로파일 결과 저장 경로"""
        try:
            profile_dir = 'log/memory_profiles'
            os.makedirs(profile_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            path = f'{profile_dir}/memory_profile_{timestamp}.log'
            
            # UTF-8 인코딩으로 파일 생성
            with open(path, 'w+', encoding='utf-8') as f:
                pass
                
            return path
            
        except Exception as e:
            self.logger.error(f"프로파일 경로 생성 중 오류: {str(e)}")
            # 폴백 경로 반환
            return f'log/memory_profiles/profile_{timestamp}_fallback.log'
    
    def cleanup_old_data(self):
        """오래된 프로파일링 데이터 정리"""
        cutoff_date = TimeUtils.get_current_kst() - timedelta(days=self.data_retention_days)
        
        try:
            self.db.memory_profiles.delete_many({
                'timestamp': {'$lt': cutoff_date}
            })
        except Exception as e:
            self.logger.error(f"메모리 프로파일 데이터 정리 중 오류: {str(e)}") 