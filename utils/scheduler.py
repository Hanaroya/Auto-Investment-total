import asyncio
import logging
from datetime import datetime, time, timedelta
from typing import Callable, Dict, Any
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from database.mongodb_manager import MongoDBManager

class Scheduler:
    def __init__(self):
        self.db = MongoDBManager()
        self.logger = logging.getLogger('investment-center')
        self.running = False
        self.scheduler = AsyncIOScheduler()
        self.tasks = {}

    async def start(self):
        """스케줄러 시작"""
        try:
            self.running = True
            self.scheduler.start()
            self.logger.info("Scheduler started successfully")
            
            # 스케줄러가 실행 중인 동안 대기
            while self.running:
                await asyncio.sleep(1)
                
        except Exception as e:
            self.logger.error(f"Error starting scheduler: {str(e)}")
            raise

    async def stop(self):
        """스케줄러 중지"""
        try:
            self.running = False
            self.scheduler.shutdown()
            self.logger.info("Scheduler stopped successfully")
        except Exception as e:
            self.logger.error(f"Error stopping scheduler: {str(e)}")

    async def schedule_task(self, task_name: str, task_func, interval: int = None, cron: str = None, immediate: bool = False):
        """태스크 스케줄링
        
        Args:
            task_name: 태스크 이름
            task_func: 실행할 함수
            interval: 실행 간격 (초)
            cron: Cron 표현식 (예: "0 */1 * * *")
            immediate: 즉시 실행 여부 (기본값: False)
        """
        try:
            if cron:
                # Cron 표현식으로 스케줄링
                self.scheduler.add_job(
                    task_func,
                    'cron',
                    id=task_name,
                    # immediate가 False면 next_run_time을 설정하지 않음
                    next_run_time=datetime.now() if immediate else None,
                    **self._parse_cron(cron)
                )
                self.logger.info(f"Scheduled cron task '{task_name}' with expression: {cron}")
            else:
                # interval 스케줄링
                self.scheduler.add_job(
                    task_func,
                    'interval',
                    seconds=interval or 3600,
                    id=task_name,
                    # immediate가 False면 next_run_time을 설정하지 않음
                    next_run_time=datetime.now() if immediate else None
                )
                self.logger.info(f"Scheduled interval task '{task_name}' with interval: {interval}s")
            
            # 작업 정보 저장
            self.tasks[task_name] = {
                'func': task_func,
                'type': 'cron' if cron else 'interval',
                'schedule': cron if cron else interval
            }
            
        except Exception as e:
            self.logger.error(f"Error scheduling task {task_name}: {str(e)}")
            raise

    def _parse_cron(self, cron_expression: str) -> dict:
        """Cron 표현식을 APScheduler 파라미터로 변환
        
        Args:
            cron_expression: "분 시 일 월 요일" 형식의 cron 표현식
            
        Returns:
            dict: APScheduler cron 파라미터
        """
        try:
            minute, hour, day, month, day_of_week = cron_expression.split()
            return {
                'minute': minute,
                'hour': hour,
                'day': day,
                'month': month,
                'day_of_week': day_of_week
            }
        except Exception as e:
            self.logger.error(f"Invalid cron expression: {cron_expression}")
            raise ValueError(f"Invalid cron expression: {cron_expression}")

    async def cancel_task(self, task_id: str):
        """예약된 작업 취소
        
        Args:
            task_id: 취소할 작업의 ID
        """
        try:
            if task_id in self.tasks:
                self.scheduler.remove_job(task_id)
                del self.tasks[task_id]
                self.logger.info(f"Task {task_id} cancelled successfully")
        except Exception as e:
            self.logger.error(f"Error cancelling task {task_id}: {str(e)}")

    async def check_missed_tasks(self):
        """누락된 작업 확인 및 재실행
        Args:
            task_id: 취소할 작업의 ID
        """
        try:
            tasks = await self.db.get_collection('scheduled_tasks').find(
                {'status': 'active'}
            ).to_list(None)

            current_time = datetime.now()
            for task in tasks:
                last_run = task['last_run']
                
                if task['interval']:
                    next_run = last_run + timedelta(seconds=task['interval'])
                    if current_time > next_run:
                        await self.execute_missed_task(task)
                
                elif task['at_time']:
                    task_time = datetime.strptime(task['at_time'], "%H:%M").time()
                    if (current_time.time() > task_time and 
                        last_run.date() < current_time.date()):
                        await self.execute_missed_task(task)

        except Exception as e:
            self.logger.error(f"Error checking missed tasks: {e}")

    async def execute_missed_task(self, task: Dict[str, Any]):
        """누락된 작업 실행"""
        try:
            if task['_id'] in self.tasks:
                await self.tasks[task['_id']]['func']()
                
                await self.db.get_collection('scheduled_tasks').update_one(
                    {'_id': task['_id']},
                    {'$set': {'last_run': datetime.now()}}
                )
                
                self.logger.info(f"Executed missed task {task['_id']}")

        except Exception as e:
            self.logger.error(f"Error executing missed task {task['_id']}: {e}")

    async def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """작업 상태 조회"""
        try:
            task = await self.db.get_collection('scheduled_tasks').find_one(
                {'_id': task_id}
            )
            return task if task else {}

        except Exception as e:
            self.logger.error(f"Error getting task status for {task_id}: {e}")
            return {}

    def is_market_open(self) -> bool:
        """거래소 운영 시간 확인 (24시간 운영이므로 항상 True)"""
        return True

    async def initialize_tasks(self):
        """기존 작업 초기화"""
        try:
            tasks = await self.db.get_collection('scheduled_tasks').find(
                {'status': 'active'}
            ).to_list(None)

            for task in tasks:
                if task['interval']:
                    self.scheduler.add_job(
                        self.tasks.get(task['_id'], {}).get('func'),
                        'interval',
                        seconds=task['interval'],
                        id=str(task['_id'])
                    )
                elif task['at_time']:
                    hour, minute = task['at_time'].split(':')
                    self.scheduler.add_job(
                        self.tasks.get(task['_id'], {}).get('func'),
                        'cron',
                        hour=hour,
                        minute=minute,
                        id=str(task['_id'])
                    )

            self.logger.info(f"Initialized {len(tasks)} tasks")

        except Exception as e:
            self.logger.error(f"Error initializing tasks: {e}") 