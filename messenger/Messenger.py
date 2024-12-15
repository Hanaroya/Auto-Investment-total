from typing import Dict, List, Optional
import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import logging

class Messenger:
    def __init__(self, config: Dict):
        self.config = config
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('Messenger')
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler('messenger.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
        
    def send(self, message: str, messenger_type: str = "slack") -> bool:
        """메시지 전송
        Args:
            message: 전송할 메시지
            messenger_type: "slack" 또는 "email"
        """
        try:
            if messenger_type.lower() == "slack":
                return self._send_slack(message)
            elif messenger_type.lower() == "email":
                return self._send_email(message)
            else:
                self.logger.error(f"지원하지 않는 메신저 타입: {messenger_type}")
                return False
        except Exception as e:
            self.logger.error(f"메시지 전송 실패: {str(e)}")
            return False

    def _send_slack(self, message: str) -> bool:
        """Slack으로 메시지 전송"""
        try:
            response = requests.post(
                "https://slack.com/api/chat.postMessage",
                headers={
                    "Authorization": f"Bearer {self.config['messenger']['slack']['bot_token']}"
                },
                json={
                    "channel": "#auto-trade",
                    "text": message
                }
            )
            
            if response.status_code == 200:
                self.logger.info("Slack 메시지 전송 성공")
                return True
            else:
                self.logger.error(f"Slack API 오류: {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Slack 메시지 전송 실패: {str(e)}")
            return False

    def _send_email(self, message: str, subject: str = "Auto Investment 알림") -> bool:
        """이메일로 메시지 전송"""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.config['messenger']['gmail']['sender']
            msg['To'] = self.config['messenger']['gmail']['address']
            msg['Date'] = formatdate(localtime=True)
            msg['Subject'] = subject

            msg.attach(MIMEText(message))

            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(
                    self.config['messenger']['gmail']['address'],
                    self.config['messenger']['gmail']['api_key']
                )
                smtp.send_message(msg)
                
            self.logger.info("이메일 전송 성공")
            return True
            
        except Exception as e:
            self.logger.error(f"이메일 전송 실패: {str(e)}")
            return False

    def send_alert(self, message: str, is_emergency: bool = False) -> None:
        """중요도에 따른 메시지 전송
        Args:
            message: 전송할 메시지
            is_emergency: True인 경우 모든 채널로 전송
        """
        if is_emergency:
            # 긴급 메시지는 모든 채널로 전송
            self._send_slack(f"🚨 긴급: {message}")
            self._send_email(message, subject="[긴급] Auto Investment 알림")
        else:
            # 일반 메시지는 Slack으로만 전송
            self._send_slack(message) 