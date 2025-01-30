from datetime import datetime
import pytz

class TimeUtils:
    """시간 관련 유틸리티 클래스"""
    
    KST = pytz.timezone('Asia/Seoul')
    
    @classmethod
    def get_current_kst(cls) -> datetime:
        """현재 한국 시간을 반환"""
        return datetime.now(cls.KST)
    
    @classmethod
    def convert_to_kst(cls, dt: datetime) -> datetime:
        """주어진 datetime을 KST로 변환
        
        Args:
            dt (datetime): 변환할 datetime 객체
            
        Returns:
            datetime: KST로 변환된 datetime 객체
        """
        if dt.tzinfo is None:  # naive datetime인 경우
            dt = pytz.utc.localize(dt)  # UTC로 가정하고 변환
        return dt.astimezone(cls.KST)
    
    @classmethod
    def format_kst(cls, dt: datetime, format_str: str = '%Y-%m-%d %H:%M:%S %Z') -> str:
        """datetime을 KST 문자열로 포맷팅
        
        Args:
            dt (datetime): 포맷팅할 datetime 객체
            format_str (str): 포맷 문자열
            
        Returns:
            str: 포맷팅된 시간 문자열
        """
        kst_time = cls.convert_to_kst(dt)
        return kst_time.strftime(format_str)
    
    @classmethod
    def is_same_day(cls, dt1: datetime, dt2: datetime) -> bool:
        """두 datetime이 같은 날짜인지 확인 (KST 기준)
        
        Args:
            dt1 (datetime): 첫 번째 datetime 객체
            dt2 (datetime): 두 번째 datetime 객체
            
        Returns:
            bool: 같은 날짜면 True, 다르면 False
        """
        kst_dt1 = cls.convert_to_kst(dt1)
        kst_dt2 = cls.convert_to_kst(dt2)
        return kst_dt1.date() == kst_dt2.date() 