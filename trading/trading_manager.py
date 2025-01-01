from typing import Dict, List
import logging
from database.mongodb_manager import MongoDBManager
from messenger.Messenger import Messenger
from datetime import datetime, timezone, timedelta
import pandas as pd
import yaml
from strategy.StrategyBase import StrategyManager
from trade_market_api.UpbitCall import UpbitCall
import os

class TradingManager:
    """
    거래 관리자
    
    거래 신호 처리 및 거래 데이터 관리를 담당합니다.
    """
    def __init__(self):
        self.db = MongoDBManager()
        self.config = self._load_config()
        self.messenger = Messenger(self.config)
        self.logger = logging.getLogger('investment-center')
        self.upbit = UpbitCall(
            self.config['api_keys']['upbit']['access_key'],
            self.config['api_keys']['upbit']['secret_key'],
            is_test=True
        )
    def _load_config(self) -> Dict:
        """설정 파일 로드"""
        try:
            with open("resource/application.yml", 'r', encoding='utf-8') as file:
                return yaml.safe_load(file)
        except Exception as e:
            self.logger.error(f"설정 파일 로드 실패: {str(e)}")
            return {}

    def process_buy_signal(self, coin: str, thread_id: int, signal_strength: float, 
                               price: float, strategy_data: Dict):
        """매수 신호 처리"""
        try:
            # 투자 가능 금액 확인
            if not self.check_investment_limit(thread_id):
                self.logger.warning(f"투자 한도 초과: thread_id={thread_id}")
                return False

            # KST 시간으로 통일
            kst_now = datetime.now(timezone(timedelta(hours=9)))
            
            # 테스트 모드 확인 (config의 mode와 upbit test_mode 모두 확인)
            is_test = (
                self.config.get('mode') == 'test' or 
                self.config.get('api_keys', {}).get('upbit', {}).get('test_mode', True)
            )
            
            order_result = None
            if not is_test:
                # 실제 매수 주문 실행
                order_result = self.upbit.place_order(
                    market=coin,
                    side='bid',
                    price=price,
                    volume=strategy_data.get('investment_amount', 0) / price
                )

                if not order_result:
                    self.logger.error(f"매수 주문 실패: {coin}")
                    return False
            else:
                # 테스트 모드 로그
                self.logger.info(f"[TEST MODE] 가상 매수 신호 처리: {coin} @ {price:,}원")
                order_result = {
                    'uuid': f'test_buy_{kst_now.timestamp()}',
                    'executed_volume': strategy_data.get('investment_amount', 0) / price,
                    'price': price
                }

            trade_data = {
                'coin': coin,
                'type': 'buy',
                'timestamp': kst_now,
                'price': price,
                'signal_strength': signal_strength,
                'thread_id': thread_id,
                'strategy_data': strategy_data,
                'status': 'active',
                'investment_amount': strategy_data.get('investment_amount', 0),
                'order_id': order_result.get('uuid'),
                'executed_volume': order_result.get('executed_volume', 0),
                'test_mode': is_test
            }

            # 거래 데이터 저장
            trade_id = self.db.insert_trade(trade_data)
            
            # 메신저로 매수 알림
            message = f"{'[TEST MODE] ' if is_test else ''}" + self.create_buy_message(trade_data)
            self.messenger.send_message(message=message, messenger_type="slack")
            
            return True

        except Exception as e:
            self.logger.error(f"Error in process_buy_signal: {e}")
            return False

    def process_sell_signal(self, coin: str, thread_id: int, signal_strength: float,
                                price: float, strategy_data: Dict):
        """매도 신호 처리
        
        개선사항:
        - current_strategy_data 추가하여 매도 시점의 전략 데이터 저장
        - 수익률 계산 및 기록
        """
        try:
            # 활성 거래 조회
            active_trade = self.db.get_active_trade(coin, thread_id)

            if not active_trade:
                return False

            # KST 시간으로 통일
            kst_now = datetime.now(timezone(timedelta(hours=9)))

            # 테스트 모드 확인
            is_test = (
                self.config.get('mode') == 'test' or 
                self.config.get('api_keys', {}).get('upbit', {}).get('test_mode', True)
            )

            order_result = None
            if not is_test:
                # 실제 매도 주문 실행
                order_result = self.upbit.place_order(
                    market=coin,
                    side='ask',
                    price=price,
                    volume=active_trade.get('executed_volume', 0)
                )

                if not order_result:
                    self.logger.error(f"매도 주문 실패: {coin}")
                    return False
            else:
                # 테스트 모드 로그
                self.logger.info(f"[TEST MODE] 가상 매도 신호 처리: {coin} @ {price:,}원")
                order_result = {
                    'uuid': f'test_sell_{kst_now.timestamp()}',
                    'executed_volume': active_trade.get('executed_volume', 0),
                    'price': price
                }

            # 수익률 계산
            profit_rate = ((price - active_trade['price']) / active_trade['price']) * 100
            
            update_data = {
                'status': 'closed',
                'sell_price': price,
                'sell_timestamp': kst_now,
                'sell_signal_strength': signal_strength,
                'current_strategy_data': strategy_data,
                'profit_rate': profit_rate,
                'sell_order_id': order_result.get('uuid'),
                'final_executed_volume': order_result.get('executed_volume', 0),
                'test_mode': is_test
            }
            
            # 거래 데이터 업데이트
            self.db.update_trade(active_trade['_id'], update_data)

            # 메신저로 매도 알림
            message = self.create_sell_message(active_trade, price, signal_strength)
            self.messenger.send_message(message=message, messenger_type="slack")

            return True

        except Exception as e:
            self.logger.error(f"Error in process_sell_signal: {e}")
            return False

    async def generate_daily_report(self):
        """일일 리포트 생성
        
        Note:
        - 예외 처리 강화
        - 파일 처리 후 정리
        """
        try:
            trades = self.db.trades.find({}).to_list(None)
            
            if not trades:
                self.logger.info("거래 데이터가 없습니다.")
                self.messenger.send_message(message="거래 데이터가 없습니다.", messenger_type="slack")
                return
            
            # 전체 거래 기록용 DataFrame
            trades_df = pd.DataFrame(trades)
            
            # 현재 보유 현황 계산
            holdings_df = pd.DataFrame([
                trade for trade in trades 
                if trade['status'] == 'active'
            ])
            
            if not holdings_df.empty:
                holdings_df = holdings_df[['coin', 'investment_amount', 'status', 'price', 'timestamp']]
                total_investment = holdings_df['investment_amount'].sum()
                
                # 투자 비중 계산
                holdings_df['investment_ratio'] = holdings_df['investment_amount'] / total_investment * 100
            
            # Excel 파일 생성
            filename = f"투자현황-{datetime.now().strftime('%Y%m%d')}.xlsx"
            with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
                # 거래 기록 시트
                trades_df.to_excel(writer, sheet_name='거래기록', index=False)
                
                # 보유 현황 시트
                if not holdings_df.empty:
                    holdings_df.to_excel(writer, sheet_name='거래현황', index=False)
                    
                    # 원형 그래프 생성
                    workbook = writer.book
                    worksheet = writer.sheets['거래현황']
                    
                    chart = workbook.add_chart({'type': 'pie'})
                    
                    # 데이터 범위 설정
                    last_row = len(holdings_df) + 1
                    chart.add_series({
                        'name': '투자 비중',
                        'categories': f'=거래현황!$A$2:$A${last_row}',  # 코인명
                        'values': f'=거래현황!$B$2:$B${last_row}',      # 투자금액
                    })
                    
                    chart.set_title({'name': '코인별 투자 비중'})
                    chart.set_style(10)
                    
                    # 차트 삽입
                    worksheet.insert_chart('G2', chart)
            
            try:
                # 이메일 전송
                await self.messenger.send_message(
                    message=f"{datetime.now().strftime('%Y-%m-%d')} 일일 리포트입니다.",
                    messenger_type="email",
                    subject=f"{datetime.now().strftime('%Y-%m-%d')} 투자 리포트",
                    attachment_path=filename  # Excel 파일 경로
                )
                self.logger.info("일일 리포트 생성 및 전송 완료")
                
                # 메신저 알림
                await self.messenger.send_message(message=f"{filename} 파일이 전달되었습니다.", messenger_type="slack")
            finally:
                # 파일 정리
                import os
                if os.path.exists(filename):
                    os.remove(filename)
                    
        except Exception as e:
            self.logger.error(f"일일 리포트 생성 중 오류 발생: {str(e)}")

    def create_buy_message(self, trade_data: Dict) -> str:
        """매수 메시지 생성
        
        매수 시점의 전략 데이터를 기반으로 메시지를 생성합니다.

        Args:
            trade_data: 거래 데이터
        Returns:
            매수 메시지
        """
        strategy_data = trade_data['strategy_data']
        
        # 구매 경로 확인
        buy_reason = "상승세 감지" if strategy_data.get('uptrend_signal', 0) > 0.5 else "하락세 종료"
        
        message = (
            f"------------------------------------------------\n"
            f"Coin: {trade_data['coin']}, 구매 ({buy_reason})\n"
            f" 구매 시간: {trade_data['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}\n"
            f" 구매 가격: {trade_data['price']:,}\n"
            f" 구매 신호: {trade_data['signal_strength']:.2f}\n"
            f" Coin-rank: {strategy_data.get('coin_rank', 'N/A')}\n"
            f" 투자 금액: W{trade_data.get('investment_amount', 0):,}\n"
        )

        # 전략별 결과 추가
        if 'rsi' in strategy_data:
            message += f" RSI: [{strategy_data['rsi']:.2f} - 결과: {strategy_data['rsi_signal']:.1f}]\n"
        
        if 'stochastic_k' in strategy_data and 'stochastic_d' in strategy_data:
            message += (f" Stochastic RSI: [K: {strategy_data['stochastic_k']:.0f}, "
                       f"D: {strategy_data['stochastic_d']:.0f} - "
                       f"결과: {strategy_data.get('stochastic_signal', 0):.1f}]\n")

        # 기타 전략 결과들 추가
        for key, value in strategy_data.items():
            if key not in ['rsi', 'stochastic_k', 'stochastic_d', 'coin_rank'] and '_signal' in key:
                strategy_name = key.replace('_signal', '').upper()
                message += f" {strategy_name}: [{value:.1f}]\n"

        message += "\n------------------------------------------------"
        return message

    def create_sell_message(self, trade_data: Dict, sell_price: float, 
                           sell_signal: float) -> str:
        """매도 메시지 생성
        
        매도 시점의 전략 데이터를 기반으로 메시지를 생성합니다.

        Args:
            trade_data: 거래 데이터
            sell_price: 판매 가격
            sell_signal: 판매 신호
        Returns:
            매도 메시지
        """
        strategy_data = trade_data['strategy_data']
        total_investment = trade_data.get('total_investment', 0)
        
        message = (
            f"------------------------------------------------\n"
            f"Coin: {trade_data['coin']}, 판매\n"
            f" 판매 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f" 판매 가격: {sell_price:,}\n"
            f" 판매 신호: {sell_signal:.2f}\n"
            f" Coin-rank: {strategy_data.get('coin_rank', 'N/A')}\n"
            f" 총 투자 금액: W{total_investment:,}\n"
        )

        # 전략별 결과 추가 (판매 시점의 지표들)
        current_strategy_data = trade_data.get('current_strategy_data', {})
        
        if 'rsi' in current_strategy_data:
            message += f" RSI: [{current_strategy_data['rsi']:.2f} - 결과: {current_strategy_data['rsi_signal']:.1f}]\n"
        
        if 'stochastic_k' in current_strategy_data and 'stochastic_d' in current_strategy_data:
            message += (f" Stochastic RSI: [K: {current_strategy_data['stochastic_k']:.0f}, "
                       f"D: {current_strategy_data['stochastic_d']:.0f} - "
                       f"결과: {current_strategy_data.get('stochastic_signal', 0):.1f}]\n")

        # 기타 전략 결과들 추가
        for key, value in current_strategy_data.items():
            if key not in ['rsi', 'stochastic_k', 'stochastic_d', 'coin_rank'] and '_signal' in key:
                strategy_name = key.replace('_signal', '').upper()
                message += f" {strategy_name}: [{value:.1f}]\n"

        # 수익률 정보 추가
        profit_rate = ((sell_price - trade_data['price']) / trade_data['price']) * 100
        message += f" 수익률: {profit_rate:.2f}%\n"

        message += "\n------------------------------------------------"
        return message

    async def close_all_positions(self):
        """
        모든 활성 포지션 종료
        
        Args:
            coin: 종목명
            thread_id: 스레드 ID
            signal_strength: 신호 강도
            price: 현재 가격
            strategy_data: 전략 데이터

        Returns:
            True: 성공
            False: 실패
        
        Notes:
        - 포지션 정리 시 강제 매도 신호를 사용하여 모든 포지션을 종료합니다.
        - 강제 매도 신호는 1.0으로 설정되어 있으며, 이는 강제적인 매도를 의미합니다.
        """
        try:
            # 활성 거래 조회
            active_trades = await self.db.get_collection('trades').find({
                'status': 'active'
            }).to_list(None)

            for trade in active_trades:
                # 각 거래에 대해 매도 처리
                await self.process_sell_signal(
                    coin=trade['coin'],
                    thread_id=trade['thread_id'],
                    signal_strength=1.0,  # 강제 매도 신호
                    price=trade['price'],  # 현재 가격 필요
                    strategy_data={'forced_sell': True}
                )
            
            return True
        except Exception as e:
            self.logger.error(f"포지션 정리 중 오류 발생: {e}")
            return False

    async def generate_hourly_report(self):
        """시간별 리포트 생성
        
        매 시간 정각에 실행되며 현재 보유 포지션과 투자 현황을 보고합니다.
        - 현재 보유 코인 목록
        - 각 코인별 매수 시간과 임계값
        - 총 투자금액
        """
        try:
            # 활성 거래(현재 보유 중인 포지션) 조회
            collection = self.db.get_collection('trades')
            active_trades = await collection.find({'status': 'active'}).to_list(None)
            
            if not active_trades:
                self.logger.info("현재 보유 중인 코인이 없습니다.")
                await self.messenger.send_message(message="현재 보유 중인 코인이 없습니다.", messenger_type="slack")
                return
            
            # 총 투자금액과 현재 가치 계산을 위한 변수
            total_investment = 0
            total_current_value = 0
            
            # 메시지 생성
            current_time = datetime.now().strftime('%Y-%m-%d %H:00')
            message = (
                f"📊 시간별 투자 현황 ({current_time})\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            )
            
            # 각 코인별 상세 정보
            for trade in active_trades:
                hold_time = datetime.utcnow() - trade['timestamp']
                hours = hold_time.total_seconds() / 3600
                
                # 현재 가격 조회 (이 부분은 거래소 API를 통해 구현 필요)
                current_price = self.upbit.get_current_price(trade['coin'])
                investment_amount = trade.get('investment_amount', 0)
                
                # 수익률 계산
                profit_rate = ((current_price - trade['price']) / trade['price']) * 100
                profit_amount = investment_amount * (profit_rate / 100)
                
                # 총계 계산
                total_investment += investment_amount
                total_current_value += (investment_amount + profit_amount)
                
                coin_info = (
                    f"• {trade['coin']}\n"
                    f"  └ 매수가: ₩{trade['price']:,}\n"
                    f"  └ 현재가: ₩{current_price:,}\n"
                    f"  └ 수익률: {profit_rate:+.2f}% (₩{profit_amount:+,.0f})\n"
                    f"  └ 매수시간: {trade['timestamp'].strftime('%Y-%m-%d %H:%M')}"
                    f" ({hours:.1f}시간 전)\n"
                    f"  └ 매수 임계값: {trade['strategy_data'].get('buy_threshold', 'N/A')}\n"
                    f"  └ 투자금액: ₩{investment_amount:,}\n"
                )
                message += coin_info + "\n"
            
            # 전체 포트폴리오 수익률
            total_profit_rate = ((total_current_value - total_investment) / total_investment * 100) if total_investment > 0 else 0
            total_profit_amount = total_current_value - total_investment
            
            portfolio_summary = (
                f"📈 포트폴리오 요약\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 총 투자금액: ₩{total_investment:,}\n"
                f"💵 현재 평가금액: ₩{total_current_value:,.0f}\n"
                f"📊 총 수익률: {total_profit_rate:+.2f}% (₩{total_profit_amount:+,.0f})\n"
                f"🔢 보유 코인: {len(active_trades)}개\n"
            )
            
            message = portfolio_summary + "\n" + message + "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            
            # Slack으로 메시지 전송
            await self.messenger.send_message(message=message, messenger_type="slack")
            
            self.logger.info(f"시간별 리포트 생성 완료: {current_time}")
            
        except Exception as e:
            self.logger.error(f"시간별 리포트 생성 중 오류 발생: {e}")
            raise

    def update_strategy_data(self, coin: str, price: float, strategy_results: Dict):
        """전략 분석 결과 업데이트
        
        Args:
            coin: 코인 정보
            price: 현재 가격
            strategy_results: 전략 분석 결과
        """
        try:
            # 입력값 로깅
            self.logger.debug(f"{coin} - 입력된 전략 결과: {strategy_results}")
            
            # 전략 결과 검증
            if not isinstance(strategy_results, dict):
                self.logger.error(f"{coin} - 유효하지 않은 전략 결과 형식: {type(strategy_results)}")
                return
            
            if 'strategy_data' not in strategy_results:
                self.logger.error(f"{coin} - strategy_data 없음: {strategy_results}")
                return
            
            # 전략 데이터 구성
            strategy_data = {
                'current_price': strategy_results.get('price', price),
                'timestamp': datetime.now(timezone(timedelta(hours=9))),  # KST 시간
                'coin': coin,
                'price': strategy_results.get('price', price),
                'action': strategy_results.get('action', 'hold'),
                'signal_strength': strategy_results.get('overall_signal', 0),
                'market_data': strategy_results.get('market_data', {}),  # 시장 데이터
                'strategies': {
                    name: {
                        'signal': data.get('signal', 'hold'),
                        'signal_strength': data.get('overall_signal', 0),
                        'value': data.get('value', 0),
                    }
                    for name, data in strategy_results.get('strategy_data', {}).items()
                }
            }

            # MongoDB에 전략 데이터 저장 (upsert 사용)
            try:
                result = self.db.strategy_data.update_one(
                    {'coin': coin}, # 코인 이름으로 조회
                    {'$set': strategy_data}, # 전략 데이터 업데이트
                    upsert=True # 데이터가 없으면 생성
                )
                
                if result.modified_count > 0 or result.upserted_id:
                    self.logger.debug(f"{coin} 전략 데이터 저장/업데이트 성공")
                else:
                    self.logger.warning(f"{coin} 전략 데이터 변경 없음")
                
            except Exception as db_error:
                self.logger.error(f"MongoDB 저장 중 오류 발생: {str(db_error)}")
                
        except Exception as e:
            self.logger.error(f"전략 데이터 업데이트 중 오류 발생: {str(e)}", exc_info=True)

    def get_active_trades(self) -> List[Dict]:
        """
        현재 활성화된 거래 목록을 조회합니다.
        
        Returns:
            List[Dict]: 활성 거래 목록
        """
        try:
            # 직접 컬렉션 접근
            active_trades = self.db.trades.find({"status": "active"})
            return list(active_trades)
        except Exception as e:
            self.logger.error(f"활성 거래 조회 중 오류: {str(e)}")
            return []

    def check_investment_limit(self, thread_id: int) -> bool:
        """
        스레드별 투자 한도를 확인합니다.
        TradingThread에서 이미 max_investment를 체크하므로,
        여기서는 전체 투자 한도만 추가로 확인합니다.
        
        Args:
            thread_id: 스레드 ID
            
        Returns:
            bool: 투자 가능 여부 (True: 투자 가능, False: 한도 초과)
        """
        try:
            # 환경 변수에서 설정값 가져오기
            max_thread_investment = float(os.getenv('MAX_THREAD_INVESTMENT', 80000))  # 스레드당 8만원
            total_max_investment = float(os.getenv('TOTAL_MAX_INVESTMENT', 800000))   # 전체 80만원
            min_trade_amount = float(os.getenv('MIN_TRADE_AMOUNT', 5000))            # 최소 거래금액
            reserve_amount = float(os.getenv('RESERVE_AMOUNT', 200000))              # 예비금
            
            # 현재 스레드의 활성 거래들 조회
            thread_trades = self.db.trades.find({
                'thread_id': thread_id,
                'status': 'active'
            })
            
            # 스레드별 투자 총액 계산
            thread_investment = sum(trade.get('investment_amount', 0) for trade in thread_trades)
            
            # 스레드별 한도 체크
            if thread_investment >= max_thread_investment:
                self.logger.warning(f"Thread {thread_id}의 투자 한도 초과: {thread_investment:,}원/{max_thread_investment:,}원")
                return False
            
            # 전체 활성 거래들 조회
            all_trades = self.db.trades.find({'status': 'active'})
            total_investment = sum(trade.get('investment_amount', 0) for trade in all_trades)
            
            # 예비금을 제외한 실제 투자 가능 금액 계산
            available_investment = total_max_investment - reserve_amount
            
            # 최소 거래금액 이상의 여유가 있고, 전체 투자한도 내인지 확인
            if available_investment - total_investment >= min_trade_amount and total_investment < available_investment:
                return True
            
            self.logger.warning(f"전체 투자 한도 초과: {total_investment:,}원/{available_investment:,}원")
            return False

        except Exception as e:
            self.logger.error(f"투자 한도 확인 중 오류 발생: {str(e)}")
            return False  # 오류 발생 시 안전을 위해 False 반환