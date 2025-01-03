"""
메시지 전송을 담당하는 Messenger 모듈
여러 메신저 플랫폼(Slack, Email 등)을 통합 관리합니다.
"""

from typing import Dict
import logging
import aiohttp
import aiosmtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
import os
from email.mime.application import MIMEApplication

class Messenger:
    """
    메시지 전송을 처리하는 클래스
    
    Attributes:
        config (Dict): 메신저 설정 정보를 담은 딕셔너리
        logger (logging.Logger): 로깅을 위한 Logger 인스턴스
    """

    def __init__(self, config: Dict = None):
        """
        Messenger 클래스 초기화
        
        Args:
            config (Dict, optional): 메신저 설정 정보. Defaults to None.
        """
        self.config = config or {}
        # InvestmentCenter의 logger 사용
        self.logger = logging.getLogger('investment_center')
        
    def send_message(self, message: str, messenger_type: str = "slack", **kwargs) -> bool:
        """
        지정된 메신저로 메시지를 비동기 전송
        
        Args:
            message (str): 전송할 메시지 내용
            messenger_type (str, optional): 사용할 메신저 타입. Defaults to "slack".
            **kwargs: 추가 파라미터 (예: attachment_path, subject 등)
        
        Returns:
            bool: 전송 성공 여부
        
        Raises:
            ValueError: 지원하지 않는 메신저 타입일 경우
        """
        if not messenger_type:
            self.logger.warning("messenger_type이 지정되지 않음. 기본값 'slack' 사용")
            messenger_type = "slack"
        
        messenger_type = messenger_type.lower()
        messenger_handlers = {
            "slack": self._send_slack,
            "email": self._send_email
        }
        
        try:
            if messenger_type not in messenger_handlers:
                self.logger.error(f"지원하지 않는 메신저 타입: {messenger_type}")
                return False
            
            self.logger.debug(f"메시지 전송 시도: type={messenger_type}, message={message[:100]}...")
            result = messenger_handlers[messenger_type](message, **kwargs)
            
            if result:
                self.logger.debug(f"{messenger_type} 메시지 전송 성공")
            else:
                self.logger.error(f"{messenger_type} 메시지 전송 실패")
            
            return result
            
        except Exception as e:
            self.logger.error(f"메시지 전송 중 오류 발생: {str(e)}")
            return False

    def _send_slack(self, message: str, channel: str = None) -> bool:
        """
        Slack으로 메시지를 동기식 전송
        
        Args:
            message (str): 전송할 메시지 내용
            
        Returns:
            bool: Slack 전송 성공 여부
        """
        try:
            import requests
            
            response = requests.post(
                "https://slack.com/api/chat.postMessage",
                headers={
                    "Authorization": f"Bearer {self.config.get('messenger', {}).get('slack', {}).get('bot_token')}"
                },
                json={
                    "channel": channel or self.config.get('messenger', {}).get('slack', {}).get('channel'),
                    "text": message
                }
            )
            
            if response.status_code == 200:
                self.logger.debug("Slack 메시지 전송 성공")
                return True
            else:
                self.logger.error(f"Slack API 오류: {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Slack 메시지 전송 실패: {str(e)}")
            return False

    def _send_email(self, message: str, subject: str = "Auto Investment 알림", attachment_path: str = None) -> bool:
        """
        이메일로 메시지를 동기식 전송
        
        Args:
            message (str): 전송할 메시지 내용
            subject (str, optional): 이메일 제목. Defaults to "Auto Investment 알림".
            attachment_path (str, optional): 첨부할 파일 경로. Defaults to None.
            
        Returns:
            bool: 이메일 전송 성공 여부
        """
        try:
            import smtplib
            
            email_config = self.config.get('messenger', {}).get('gmail', {})
            sender = email_config.get('sender')
            address = email_config.get('address')
            api_key = email_config.get('api_key')

            if not all([sender, address, api_key]):
                self.logger.error(f"이메일 설정 누락: sender={bool(sender)}, address={bool(address)}, api_key={bool(api_key)}")
                return False

            msg = MIMEMultipart()
            msg['From'] = sender
            msg['To'] = address
            msg['Date'] = formatdate(localtime=True)
            msg['Subject'] = subject

            message = message.replace("'", "")
            # 메시지가 None인 경우 처리
            if message is None:
                message = "내용 없음"
            msg.attach(MIMEText(message))

            # 첨부 파일이 있는 경우 처리
            if attachment_path and os.path.exists(attachment_path):
                with open(attachment_path, 'rb') as f:
                    part = MIMEApplication(f.read(), Name=os.path.basename(attachment_path))
                part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'
                msg.attach(part)

            # SMTP 연결 및 전송
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(address, api_key)
                smtp.send_message(msg)
            
            self.logger.info("이메일 전송 성공")
            return True
            
        except Exception as e:
            self.logger.error(f"이메일 전송 실패: {str(e)}")
            self.logger.error(f"Config 상태: {self.config.get('messenger', {}).get('gmail', {})}")
            return False

    def send_alert(self, message: str, is_emergency: bool = False) -> None:
        """
        중요도에 따른 알림 메시지 전송
        
        Args:
            message (str): 전송할 메시지 내용
            is_emergency (bool, optional): 긴급 메시지 여부. Defaults to False.
            
        Notes:
            - 긴급 메시지는 Slack과 이메일 모두로 전송
            - 일반 메시지는 Slack으로만 전송
        """
        if is_emergency:
            # 긴급 메시지는 모든 채널로 전송
            self._send_slack(f"🚨 긴급: {message}")
            self._send_email(message, subject="[긴급] Auto Investment 알림")
        else:
            # 일반 메시지는 Slack으로만 전송
            self._send_slack(message)

    def _setup_logger(self) -> logging.Logger:
        """
        로거 설정 및 초기화
        
        Returns:
            logging.Logger: 설정된 로거 인스턴스
            
        Notes:
            - 로그 레벨은 config에서 지정된 값 또는 기본값(INFO) 사용
            - 로그 포맷에는 시간, 로거 이름, 로그 레벨, 메시지 포함
        """
        logger = logging.getLogger('investment-center')
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(self.config.get('logging', {}).get('level', 'INFO'))
        return logger