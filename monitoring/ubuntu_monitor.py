import psutil
import logging
from utils.logger_config import setup_logger
import subprocess
from datetime import datetime

class LinuxMemoryMonitor:
    def __init__(self, process_name='python'):
        self.logger = setup_logger('memory-monitor')
        self.process_name = process_name
        
    def monitor_memory(self):
        try:
            # 시스템 전체 메모리 정보
            mem = psutil.virtual_memory()
            swap = psutil.swap_memory()
            
            self.logger.info(f"""
                시스템 메모리 사용량:
                전체: {mem.total / 1024 / 1024:.2f} MB
                사용: {mem.used / 1024 / 1024:.2f} MB
                여유: {mem.available / 1024 / 1024:.2f} MB
                사용률: {mem.percent}%
                
                Swap 메모리:
                전체: {swap.total / 1024 / 1024:.2f} MB
                사용: {swap.used / 1024 / 1024:.2f} MB
                여유: {swap.free / 1024 / 1024:.2f} MB
            """)
            
            # 프로세스별 메모리 사용량
            for proc in psutil.process_iter(['pid', 'name', 'memory_info']):
                if self.process_name in proc.info['name'].lower():
                    mem = proc.info['memory_info']
                    self.logger.info(f"""
                        프로세스: {proc.info['name']}
                        PID: {proc.info['pid']}
                        RSS: {mem.rss / 1024 / 1024:.2f} MB
                        VMS: {mem.vms / 1024 / 1024:.2f} MB
                    """)
                    
        except Exception as e:
            self.logger.error(f"메모리 모니터링 오류: {str(e)}") 