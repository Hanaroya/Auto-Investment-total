import schedule
import time
import logging
from datetime import datetime
import asyncio
from typing import Callable, Dict, Any
from database.mongodb_manager import MongoDBManager

class SimpleScheduler:
    def __init__(self):
        self.db = MongoDBManager()
        self.logger = logging.getLogger('investment-center')
        self.running = False
        self.tasks = {}

    def start(self):
        """스케줄러 시작"""
        self.running = True
        self.logger.info("스케줄러가 시작되었습니다")

    def stop(self):
        """스케줄러 중지"""
        self.running = False
        self.logger.info("스케줄러가 중지되었습니다")

    def schedule_task(self, task_name: str, task_func: Callable, hour: int = -1, minute: int = 0):
        """작업 스케줄링
        
        Args:
            task_name: 작업 이름
            task_func: 실행할 함수
            hour: 실행할 시간 (0-23, -1인 경우 매시간 실행)
            minute: 실행할 분 (0-59)
        """
        try:
            def sync_wrapper():
                """동기식 래퍼 함수"""
                try:
                    if asyncio.iscoroutinefunction(task_func):
                        # 새로운 이벤트 루프 생성 및 실행
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            loop.run_until_complete(task_func())
                        finally:
                            loop.close()
                    else:
                        task_func()
                except Exception as e:
                    self.logger.error(f"작업 실행 오류 {task_name}: {str(e)}")

            # 매시간 실행
            if hour == -1:
                schedule.every().hour.at(f":{minute:02d}").do(sync_wrapper)
                self.logger.info(f"매시 {minute:02d}분에 실행되도록 작업 '{task_name}' 등록됨")
            # 특정 시간 실행
            else:
                schedule.every().day.at(f"{hour:02d}:{minute:02d}").do(sync_wrapper)
                self.logger.info(f"매일 {hour:02d}:{minute:02d}에 실행되도록 작업 '{task_name}' 등록됨")

            # 작업 정보 저장
            collection = self.db.get_sync_collection('scheduled_tasks')
            collection.update_one(
                {'_id': task_name},
                {
                    '$set': {
                        'type': 'hourly' if hour == -1 else 'daily',
                        'hour': hour,
                        'minute': minute,
                        'last_updated': datetime.now(),
                        'status': 'active'
                    }
                },
                upsert=True
            )

            self.tasks[task_name] = {'func': task_func, 'hour': hour, 'minute': minute}

        except Exception as e:
            self.logger.error(f"작업 스케줄링 오류 {task_name}: {str(e)}")
            raise

    async def run(self):
        """스케줄러 실행 루프"""
        self.start()
        while self.running:
            try:
                schedule.run_pending()
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.error(f"스케줄러 실행 중 오류: {str(e)}")
                await asyncio.sleep(5) 