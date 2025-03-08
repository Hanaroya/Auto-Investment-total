import logging
from pathlib import Path
import yaml
from utils.time_utils import TimeUtils

def setup_logger(logger_name: str = 'investment_center') -> logging.Logger:
    """로깅 설정
    
    Args:
        logger_name (str): 로거 이름
        
    Returns:
        logging.Logger: 설정된 로거 인스턴스
    """
    try:
        # 설정 파일 로드
        with open('resource/application.yml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        logger = logging.getLogger(logger_name)
        
        # 로그 레벨 설정
        log_level = config.get('logging', {}).get('level', 'INFO')
        logger.setLevel(getattr(logging, log_level.upper()))
        
        # 이미 핸들러가 있다면 제거
        if logger.handlers:
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)
        
        # 로그 포맷 설정
        log_format = config.get('logging', {}).get('format', 
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        formatter = logging.Formatter(log_format)
        
        # 콘솔 핸들러 설정
        if config.get('logging', {}).get('console', {}).get('enabled', True):
            console_handler = logging.StreamHandler()
            console_level = config.get('logging', {}).get('console', {}).get('level', 'INFO')
            console_handler.setLevel(getattr(logging, console_level.upper()))
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        
        # 파일 핸들러 설정
        if config.get('logging', {}).get('file', {}).get('enabled', True):
            log_dir = Path(config.get('logging', {}).get('file', {}).get('path', 'log'))
            log_dir.mkdir(exist_ok=True)
            
            # 파일명 패턴 설정
            filename_pattern = config.get('logging', {}).get('file', {}).get(
                'filename', '{date}-investment.log')
            today = TimeUtils.get_current_kst().strftime('%Y-%m-%d') 
            filename = filename_pattern.format(date=today)
            
            file_handler = logging.FileHandler(
                log_dir / filename,
                encoding='utf-8'  # UTF-8 인코딩 명시
            )
            file_level = config.get('logging', {}).get('file', {}).get('level', 'DEBUG')
            file_handler.setLevel(getattr(logging, file_level.upper()))
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        return logger
        
    except Exception as e:
        # 기본 로거 설정 (설정 파일 로드 실패 시)
        print(f"로거 설정 파일 로드 실패: {str(e)}, 기본 설정 사용")
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        # 파일 핸들러 추가 (기본 설정)
        file_handler = logging.FileHandler(
            f'log/{TimeUtils.get_current_kst().strftime("%Y-%m-%d")}-investment.log',    
            encoding='utf-8'  # UTF-8 인코딩 명시
        )
        file_handler.setFormatter(formatter)
        
        logger.addHandler(handler)
        logger.addHandler(file_handler)
        
        return logger 