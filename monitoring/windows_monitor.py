import psutil
import logging
from memory_profiler import profile
import time
from utils.logger_config import setup_logger

class WindowsMemoryMonitor:
    def __init__(self, process_name='python'):
        self.logger = setup_logger('memory-monitor')
        self.process_name = process_name
        
    def monitor_memory(self):
        try:
            for proc in psutil.process_iter(['pid', 'name', 'memory_info']):
                if self.process_name in proc.info['name'].lower():
                    mem = proc.info['memory_info']
                    self.logger.info(f"""
                        프로세스: {proc.info['name']}
                        PID: {proc.info['pid']}
                        사용 메모리: {mem.rss / 1024 / 1024:.2f} MB
                        가상 메모리: {mem.vms / 1024 / 1024:.2f} MB
                    """)
        except Exception as e:
            self.logger.error(f"메모리 모니터링 오류: {str(e)}") 