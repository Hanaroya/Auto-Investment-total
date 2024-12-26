from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Dict, Any
import logging
from config.mongodb_config import MONGODB_CONFIG, INITIAL_SYSTEM_CONFIG

class AsyncMongoDBManager:
    """
    MongoDB 비동기 연결 및 작업을 관리하는 싱글톤 클래스
    """
    _instance = None

    def __new__(cls):
        """
        싱글톤 패턴 구현
        한 번만 인스턴스를 생성하고 이후에는 동일한 인스턴스를 반환합니다.
        """
        if cls._instance is None:
            cls._instance = super(AsyncMongoDBManager, cls).__new__(cls)
        return cls._instance

    async def initialize(self):
        """
        MongoDB 데이터베이스 연결 초기화
        """
        if not hasattr(self, 'client'):
            try:
                connection_url = (
                    f"mongodb://{MONGODB_CONFIG['username']}:{MONGODB_CONFIG['password']}"
                    f"@{MONGODB_CONFIG['host']}:{MONGODB_CONFIG['port']}/{MONGODB_CONFIG['db_name']}"
                )
                self.client = AsyncIOMotorClient(
                    connection_url,
                    authSource=MONGODB_CONFIG['db_name']
                )
                self.db = self.client[MONGODB_CONFIG['db_name']]
                
                # 컬렉션 초기화
                self.trades = self.db['trades']
                self.market_data = self.db[MONGODB_CONFIG['collections']['market_data']]
                self.thread_status = self.db[MONGODB_CONFIG['collections']['thread_status']]
                self.system_config = self.db['system_config']
                self.strategy_data = self.db['strategy_data']  # strategy_data 컬렉션 추가
                
                # 시스템 설정 초기화 확인
                await self._ensure_system_config()
                
                logging.info("Async MongoDB 연결 성공")
            except Exception as e:
                logging.error(f"Async MongoDB 연결 실패: {str(e)}")
                raise

    async def _ensure_system_config(self):
        """시스템 설정이 없는 경우 초기 설정을 생성합니다."""
        config_collection = self.db[MONGODB_CONFIG['collections']['system_config']]
        if await config_collection.count_documents({}) == 0:
            await config_collection.insert_one(INITIAL_SYSTEM_CONFIG)
            logging.info("시스템 초기 설정이 생성되었습니다.")

    async def get_active_trades(self):
        """
        활성 상태인 거래 내역 조회
        
        Returns:
            list: 상태가 'active'인 모든 거래 목록
        """
        cursor = self.db.trades.find({'status': 'active'})
        return await cursor.to_list(length=None)

    async def get_thread_status(self, thread_id: int):
        """
        특정 스레드의 상태 정보 조회
        
        Args:
            thread_id (int): 조회할 스레드의 ID
            
        Returns:
            dict: 스레드 상태 정보를 담은 문서
            None: 해당 스레드 ID가 존재하지 않는 경우
        """
        return await self.db.thread_status.find_one({'thread_id': thread_id}) 

    async def save_strategy_data(self, coin: str, strategy_data: Dict[str, Any]) -> bool:
        """코인별 전략 데이터 저장"""
        try:
            document = {
                'coin': coin,
                'timestamp': datetime.utcnow(),
                'current_price': strategy_data.get('current_price', 0),
                'strategies': {
                    'rsi': {
                        'value': strategy_data.get('rsi', 0),
                        'signal': strategy_data.get('rsi_signal', 0),
                        'buy_threshold': strategy_data.get('rsi_buy_threshold', 30),
                        'sell_threshold': strategy_data.get('rsi_sell_threshold', 70)
                    },
                    'macd': {
                        'macd': strategy_data.get('macd', 0),
                        'signal': strategy_data.get('macd_signal', 0),
                        'histogram': strategy_data.get('macd_hist', 0),
                        'buy_threshold': strategy_data.get('macd_buy_threshold', 0),
                        'sell_threshold': strategy_data.get('macd_sell_threshold', 0)
                    },
                    'bollinger': {
                        'upper': strategy_data.get('bb_upper', 0),
                        'middle': strategy_data.get('bb_middle', 0),
                        'lower': strategy_data.get('bb_lower', 0),
                        'buy_threshold': strategy_data.get('bb_buy_threshold', 0),
                        'sell_threshold': strategy_data.get('bb_sell_threshold', 0)
                    },
                    'volume': {
                        'current': strategy_data.get('current_volume', 0),
                        'average': strategy_data.get('average_volume', 0),
                        'change_rate': strategy_data.get('volume_change_rate', 0)
                    },
                    'price_change': {
                        'rate': strategy_data.get('price_change_rate', 0),
                        'threshold': strategy_data.get('price_change_threshold', 0.02)
                    },
                    'moving_average': {
                        'ma5': strategy_data.get('ma5', 0),
                        'ma20': strategy_data.get('ma20', 0)
                    },
                    'momentum': {
                        'value': strategy_data.get('momentum', 0)
                    },
                    'stochastic': {
                        'k': strategy_data.get('stoch_k', 0),
                        'd': strategy_data.get('stoch_d', 0),
                        'buy_threshold': strategy_data.get('stoch_buy_threshold', 20),
                        'sell_threshold': strategy_data.get('stoch_sell_threshold', 80)
                    },
                    'ichimoku': {
                        'cloud_top': strategy_data.get('ichimoku_cloud_top', 0),
                        'cloud_bottom': strategy_data.get('ichimoku_cloud_bottom', 0)
                    },
                    'market_sentiment': {
                        'value': strategy_data.get('market_sentiment', 0)
                    },
                    'downtrend_end': {
                        'trend_strength': strategy_data.get('trend_strength', 0),
                        'volume_change': strategy_data.get('volume_change_24h', 0)
                    },
                    'uptrend_end': {
                        'trend_strength': strategy_data.get('trend_strength', 0),
                        'resistance_level': strategy_data.get('resistance_level', 0)
                    },
                    'divergence': {
                        'price_rsi': strategy_data.get('price_rsi_divergence', 0),
                        'price_macd': strategy_data.get('price_macd_divergence', 0)
                    }
                },
                'signals': {
                    'buy_strength': strategy_data.get('buy_signal', 0),
                    'sell_strength': strategy_data.get('sell_signal', 0),
                    'overall_signal': strategy_data.get('overall_signal', 0),
                    'combined_threshold': {
                        'buy': strategy_data.get('combined_buy_threshold', 0.7),
                        'sell': strategy_data.get('combined_sell_threshold', 0.3)
                    }
                },
                'market_metrics': {
                    'volume': strategy_data.get('volume', 0),
                    'market_cap': strategy_data.get('market_cap', 0),
                    'rank': strategy_data.get('coin_rank', 0),
                    'price_change_24h': strategy_data.get('price_change_24h', 0),
                    'volume_change_24h': strategy_data.get('volume_change_24h', 0)
                },
                'thresholds': {
                    'price_change': strategy_data.get('price_change_threshold', 0.02),
                    'volume_change': strategy_data.get('volume_change_threshold', 0.5),
                    'trend_strength': strategy_data.get('trend_strength_threshold', 0.6)
                }
            }

            result = await self.strategy_data.insert_one(document)
            success = bool(result.inserted_id)
            
            if success:
                logging.debug(f"전략 데이터 저장 성공 - 코인: {coin}, ID: {result.inserted_id}")
            else:
                logging.warning(f"전략 데이터 저장 실패 - 코인: {coin}")
                
            return success

        except Exception as e:
            logging.error(f"전략 데이터 저장 실패 - 코인: {coin}, 오류: {str(e)}")
            return False

    async def get_latest_strategy_data(self, coin: str) -> Dict:
        """특정 코인의 최신 전략 데이터 조회"""
        try:
            result = await self.strategy_data.find_one(
                {'coin': coin},
                sort=[('timestamp', -1)]
            )
            return result or {}
        except Exception as e:
            logging.error(f"전략 데이터 조회 실패 - 코인: {coin}, 오류: {str(e)}")
            return {} 