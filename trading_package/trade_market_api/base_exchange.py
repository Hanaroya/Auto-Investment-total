from abc import ABC, abstractmethod

class BaseExchange(ABC):
    @abstractmethod
    async def validate_market_data(self, market_index: dict) -> bool:
        """필수 데이터 존재 여부만 확인"""
        pass

    def _check_required_data(self, market_index: dict, required_fields: list) -> bool:
        """필수 필드 존재 확인"""
        return all(market_index.get(field) is not None for field in required_fields) 