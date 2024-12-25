import asyncio
import logging
from datetime import datetime, time, timedelta
from typing import Callable, Dict, Any
import aioschedule as schedule
from database.mongodb_manager import MongoDBManager
from apscheduler.schedulers.asyncio import AsyncIOScheduler

class Scheduler:
    def __init__(self):
        self.db = MongoDBManager()
        self.logger = logging.getLogger(__name__)
        self.running = False
        self.tasks = {}

    async def start(self):
        """스케줄러 시작"""
        self.running = True
        while self.running:
            await schedule.run_pending()
            await asyncio.sleep(1)

    async def stop(self):
        """스케줄러 중지"""
        self.running = False
        schedule.clear()

    def schedule_daily_report(self, report_func: Callable):
        """일일 리포트 스케줄링 (매일 오후 8시)"""
        try:
            schedule.every().day.at("20:00").do(report_func)
            self.logger.info("Daily report scheduled for 20:00")
        except Exception as e:
            self.logger.error(f"Error scheduling daily report: {e}")

    async def schedule_task(self, task_name: str, task_func, interval: int = None, cron: str = None):
        """태스크 스케줄링
        
        Args:
            task_name: 태스크 이름
            task_func: 실행할 함수
            interval: 실행 간격 (초)
            cron: Cron 표현식 (예: "0 */1 * * *")
        """
        try:
            scheduler = AsyncIOScheduler()
            
            if cron:
                # Cron 표현식으로 스케줄링
                scheduler.add_job(
                    task_func,
                    'cron',
                    id=task_name,
                    next_run_time=datetime.now(),
                    **self._parse_cron(cron)
                )
            else:
                # 기존 interval 스케줄링
                scheduler.add_job(
                    task_func,
                    'interval',
                    seconds=interval or 3600,
                    id=task_name,
                    next_run_time=datetime.now()
                )
            
            scheduler.start()
            self.logger.info(f"Task {task_name} scheduled successfully")
            
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
                schedule.cancel_job(self.tasks[task_id]['func'])
                del self.tasks[task_id]

                await self.db.get_collection('scheduled_tasks').update_one(
                    {'_id': task_id},
                    {'$set': {'status': 'cancelled'}}
                )

                self.logger.info(f"Task {task_id} cancelled successfully")

        except Exception as e:
            self.logger.error(f"Error cancelling task {task_id}: {e}")

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
                    schedule.every(task['interval']).seconds.do(
                        self.tasks.get(task['_id'], {}).get('func')
                    )
                elif task['at_time']:
                    schedule.every().day.at(task['at_time']).do(
                        self.tasks.get(task['_id'], {}).get('func')
                    )

            self.logger.info(f"Initialized {len(tasks)} tasks")

        except Exception as e:
            self.logger.error(f"Error initializing tasks: {e}") 