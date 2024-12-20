from typing import Dict, List, Optional
import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import logging
from pathlib import Path
from datetime import datetime
import aiohttp
import aiosmtplib

class Messenger:
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """로깅 설정
        
        Returns:
            logging.Logger: 설정된 로거 인스턴스
            
        Notes:
            - 로그 파일은 /log 디렉토리에 날짜별로 저장
            - 메시지 전송 관련 로그는 WARNING 레벨로 처리
        """
        logger = logging.getLogger('Messenger')
        logger.setLevel(logging.DEBUG if self.config.get('debug', False) else logging.INFO)
        
        # 로그 디렉토리 생성
        log_dir = Path(self.config.get('logging', {}).get('directory', 'log'))
        log_dir.mkdir(exist_ok=True)
        
        # 날짜별 로그 파일 설정
        today = datetime.now().strftime('%Y-%m-%d')
        handler = logging.FileHandler(f'{log_dir}/{today}-messenger.log')
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
        
    async def send_message(self, message: str, messenger_type: str = "slack") -> bool:
        """비동기 메시지 전송"""
        try:
            if messenger_type.lower() == "slack":
                return await self._send_slack(message)
            elif messenger_type.lower() == "email":
                return await self._send_email(message)
            else:
                self.logger.error(f"지원하지 않는 메신저 타입: {messenger_type}")
                return False
        except Exception as e:
            self.logger.error(f"메시지 전송 실패: {str(e)}")
            return False

    async def _send_slack(self, message: str) -> bool:
        """Slack으로 비동기 메시지 전송"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://slack.com/api/chat.postMessage",
                    headers={
                        "Authorization": f"Bearer {self.config.get('messenger', {}).get('slack', {}).get('bot_token')}"
                    },
                    json={
                        "channel": self.config.get('messenger', {}).get('slack', {}).get('channel'),
                        "text": message
                    }
                ) as response:
                    if response.status == 200:
                        self.logger.info("Slack 메시지 전송 성공")
                        return True
                    else:
                        self.logger.error(f"Slack API 오류: {await response.text()}")
                        return False
                
        except Exception as e:
            self.logger.error(f"Slack 메시지 전송 실패: {str(e)}")
            return False

    async def _send_email(self, message: str, subject: str = "Auto Investment 알림") -> bool:
        """이메일로 비동기 메시지 전송"""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.config.get('messenger', {}).get('gmail', {}).get('sender')
            msg['To'] = self.config.get('messenger', {}).get('gmail', {}).get('address')
            msg['Date'] = formatdate(localtime=True)
            msg['Subject'] = subject

            msg.attach(MIMEText(message))

            async with aiosmtplib.SMTP('smtp.gmail.com', 465, use_tls=True) as smtp:
                await smtp.login(
                    self.config.get('messenger', {}).get('gmail', {}).get('address'),
                    self.config.get('messenger', {}).get('gmail', {}).get('api_key')
                )
                await smtp.send_message(msg)
                
            self.logger.info("이메일 전송 성공")
            return True
            
        except Exception as e:
            self.logger.error(f"이메일 전송 실패: {str(e)}")
            return False

    async def send_alert(self, message: str, is_emergency: bool = False) -> None:
        """중요도에 따른 비동기 메시지 전송"""
        if is_emergency:
            # 긴급 메시지는 모든 채널로 전송
            await self._send_slack(f"🚨 긴급: {message}")
            await self._send_email(message, subject="[긴급] Auto Investment 알림")
        else:
            # 일반 메시지는 Slack으로만 전송
            await self._send_slack(message) 