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
        """로깅 설정"""
        logger = logging.getLogger(__name__)
        return logger

    async def send_message(self, message: str) -> bool:
        """메시지 전송"""
        try:
            # 테스트 모드일 경우 콘솔에만 출력
            if self.config.get('mode') == 'test':
                self.logger.info(f"[TEST] 메시지: {message}")
                return True

            # Slack으로 메시지 전송
            if 'slack' in self.config.get('messenger', {}):
                await self._send_slack(message)

            # 이메일로 메시지 전송
            if 'email' in self.config.get('messenger', {}):
                await self._send_email(message)

            return True

        except Exception as e:
            self.logger.error(f"메시지 전송 실패: {str(e)}")
            return False

    async def _send_slack(self, message: str) -> bool:
        """Slack으로 비시지 전송"""
        try:
            webhook_url = self.config.get('messenger', {}).get('slack', {}).get('webhook_url')
            if not webhook_url:
                return False

            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json={'text': message}) as response:
                    return response.status == 200

        except Exception as e:
            self.logger.error(f"Slack 전송 실패: {str(e)}")
            return False

    async def _send_email(self, message: str, subject: str = "Auto Investment 알림") -> bool:
        """이메일로 비시지 전송"""
        try:
            email_config = self.config.get('messenger', {}).get('email', {})
            if not email_config:
                return False

            msg = MIMEMultipart()
            msg['From'] = email_config.get('from')
            msg['To'] = COMMASPACE.join(email_config.get('to', []))
            msg['Date'] = formatdate(localtime=True)
            msg['Subject'] = subject

            msg.attach(MIMEText(message))

            async with aiosmtplib.SMTP(
                hostname=email_config.get('smtp_server'),
                port=email_config.get('smtp_port', 587),
                use_tls=True
            ) as smtp:
                await smtp.login(
                    email_config.get('username'),
                    email_config.get('password')
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
            await self._send_slack(f"🚨 긴급: {message}")
            await self._send_email(message, subject="[긴급] Auto Investment 알림")
        else:
            await self._send_slack(message)