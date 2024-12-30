import unittest
from unittest.mock import Mock, patch
from datetime import datetime, timezone, timedelta
from trading.trading_manager import TradingManager

class TestTradingManager(unittest.TestCase):
    def setUp(self):
        """각 테스트 전에 실행되는 설정"""
        self.trading_manager = TradingManager()
        
        # MongoDB 매니저 모의 객체 설정
        self.trading_manager.db = Mock()
        self.trading_manager.db.insert_trade = Mock(return_value="test_trade_id")
        self.trading_manager.db.update_trade = Mock(return_value=True)
        self.trading_manager.db.get_active_trade = Mock()
        
        # Messenger 모의 객체 설정
        self.trading_manager.messenger = Mock()
        self.trading_manager.messenger.send_message = Mock()
        
        # Upbit API 모의 객체 설정
        self.trading_manager.upbit = Mock()
        self.trading_manager.upbit.place_order = Mock()
        
        # 테스트 데이터 설정
        self.test_coin = "KRW-BTC"
        self.test_thread_id = 1
        self.test_signal_strength = 0.8
        self.test_price = 50000000
        self.test_strategy_data = {
            "investment_amount": 100000,
            "rsi": 30,
            "rsi_signal": 0.8,
            "stochastic_k": 20,
            "stochastic_d": 15,
            "stochastic_signal": 0.7,
            "coin_rank": 1
        }

    def test_process_buy_signal_test_mode(self):
        """테스트 모드에서 매수 신호 처리 테스트"""
        # 테스트 모드 설정
        self.trading_manager.config = {'mode': 'test'}
        
        # 투자 한도 체크 모의 설정
        self.trading_manager.check_investment_limit = Mock(return_value=True)
        
        # 매수 신호 처리 실행
        result = self.trading_manager.process_buy_signal(
            self.test_coin,
            self.test_thread_id,
            self.test_signal_strength,
            self.test_price,
            self.test_strategy_data
        )
        
        # 검증
        self.assertTrue(result)
        self.trading_manager.db.insert_trade.assert_called_once()
        self.trading_manager.messenger.send_message.assert_called_once()
        
        # DB에 저장된 거래 데이터 검증
        saved_trade = self.trading_manager.db.insert_trade.call_args[0][0]
        self.assertEqual(saved_trade['coin'], self.test_coin)
        self.assertEqual(saved_trade['type'], 'buy')
        self.assertEqual(saved_trade['price'], self.test_price)
        self.assertEqual(saved_trade['signal_strength'], self.test_signal_strength)
        self.assertEqual(saved_trade['investment_amount'], 100000)
        self.assertTrue(saved_trade['test_mode'])

    def test_process_sell_signal_test_mode(self):
        """테스트 모드에서 매도 신호 처리 테스트"""
        # 테스트 모드 설정
        self.trading_manager.config = {'mode': 'test'}
        
        # 활성 거래 모의 데이터 설정
        active_trade = {
            '_id': 'test_id',
            'coin': self.test_coin,
            'price': self.test_price * 0.9,  # 10% 상승 가정
            'thread_id': self.test_thread_id,
            'strategy_data': self.test_strategy_data,
            'executed_volume': 0.001
        }
        self.trading_manager.db.get_active_trade.return_value = active_trade
        
        # 매도 신호 처리 실행
        result = self.trading_manager.process_sell_signal(
            self.test_coin,
            self.test_thread_id,
            self.test_signal_strength,
            self.test_price,
            self.test_strategy_data
        )
        
        # 검증
        self.assertTrue(result)
        self.trading_manager.db.update_trade.assert_called_once()
        self.trading_manager.messenger.send_message.assert_called_once()
        
        # DB 업데이트 데이터 검증
        update_data = self.trading_manager.db.update_trade.call_args[0][1]
        self.assertEqual(update_data['status'], 'closed')
        self.assertEqual(update_data['sell_price'], self.test_price)
        self.assertEqual(update_data['sell_signal_strength'], self.test_signal_strength)
        self.assertTrue(update_data['test_mode'])
        self.assertAlmostEqual(update_data['profit_rate'], 10.0)  # 10% 수익률 검증

    def test_process_buy_signal_investment_limit_exceeded(self):
        """투자 한도 초과 시 매수 신호 처리 테스트"""
        # 투자 한도 초과 설정
        self.trading_manager.check_investment_limit = Mock(return_value=False)
        
        # 매수 신호 처리 실행
        result = self.trading_manager.process_buy_signal(
            self.test_coin,
            self.test_thread_id,
            self.test_signal_strength,
            self.test_price,
            self.test_strategy_data
        )
        
        # 검증
        self.assertFalse(result)
        self.trading_manager.db.insert_trade.assert_not_called()
        self.trading_manager.messenger.send_message.assert_not_called()

    def test_process_sell_signal_no_active_trade(self):
        """활성 거래가 없을 때 매도 신호 처리 테스트"""
        # 활성 거래 없음 설정
        self.trading_manager.db.get_active_trade.return_value = None
        
        # 매도 신호 처리 실행
        result = self.trading_manager.process_sell_signal(
            self.test_coin,
            self.test_thread_id,
            self.test_signal_strength,
            self.test_price,
            self.test_strategy_data
        )
        
        # 검증
        self.assertFalse(result)
        self.trading_manager.db.update_trade.assert_not_called()
        self.trading_manager.messenger.send_message.assert_not_called()

if __name__ == '__main__':
    unittest.main() 