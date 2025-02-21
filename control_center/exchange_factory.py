
from typing import Dict, Any
from trade_market_api.UpbitCall import UpbitCall

class ExchangeFactory:
    """
    거래소 객체 생성을 담당하는 팩토리 클래스
    각 거래소별 구현체를 생성하고 설정을 주입
    """
    @staticmethod
    def create_exchange(exchange_name: str, config: Dict) -> Any:
        mode = config.get('mode', 'market')  # 기본 모드는 실제 거래
        if exchange_name.lower() == "upbit":
            if mode == 'test':
                # 테스트 환경: 더미 API 키 사용
                return UpbitCall(
                    access_key="test_access_key",
                    secret_key="test_secret_key"
                )
            # 실제 환경: 설정 파일의 API 키 사용
            return UpbitCall(
                access_key=config['api_keys']['upbit']['access_key'],
                secret_key=config['api_keys']['upbit']['secret_key']
            )
        else:
            raise ValueError(f"지원하지 않는 거래소입니다: {exchange_name}")