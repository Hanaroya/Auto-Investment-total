import time
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
from math import floor

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

            # KST 시간대 설정
            KST = timezone(timedelta(hours=9))
            kst_now = datetime.now(KST)
            
            # 시간대 확인 로깅
            self.logger.debug(f"현재 KST 시간: {kst_now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            
            # 테스트 모드 확인
            is_test = (
                self.config.get('mode') == 'test' or 
                self.config.get('api_keys', {}).get('upbit', {}).get('test_mode', True)
            )
            
            # 수수료 계산
            fee_rate = self.config['api_keys']['upbit'].get('fee', 0.05) / 100  # 0.05% -> 0.0005
            investment_amount = strategy_data.get('investment_amount', 0)
            fee_amount = investment_amount * fee_rate
            actual_investment = investment_amount - fee_amount

            # 물타기 여부 확인 및 기존 거래 정보 조회
            is_averaging_down = strategy_data.get('is_averaging_down', False)
            existing_trade = None
            if is_averaging_down:
                existing_trade = self.db.trades.find_one({
                    'coin': coin,
                    'status': 'active'
                })
                self.logger.info(f"물타기 신호 감지: {coin} - 현재 수익률: {existing_trade.get('profit_rate', 0):.2f}%")

            order_result = None
            if not is_test:
                # 실제 매수 주문 실행
                order_result = self.upbit.place_order(
                    market=coin,
                    side='bid',
                    price=price,
                    volume=actual_investment / price
                )

                if not order_result:
                    self.logger.error(f"매수 주문 실패: {coin}")
                    return False
            else:
                # 테스트 모드 로그
                self.logger.info(f"[TEST MODE] 가상 매수 신호 처리: {coin} @ {price:,}원 (수수료: {fee_amount:,.0f}원)")
                order_result = {
                    'uuid': f'test_buy_{kst_now.timestamp()}',
                    'executed_volume': actual_investment / price,  # 수수료를 제외한 수량
                    'price': price
                }

            if is_averaging_down and existing_trade:
                # 기존 거래 정보 업데이트 (물타기)
                total_investment = existing_trade['investment_amount'] + investment_amount
                total_volume = existing_trade['executed_volume'] + order_result['executed_volume']
                average_price = (existing_trade['price'] * existing_trade['executed_volume'] + 
                               price * order_result['executed_volume']) / total_volume

                update_data = {
                    'investment_amount': total_investment,
                    'actual_investment': existing_trade['actual_investment'] + actual_investment,
                    'executed_volume': total_volume,
                    'price': round(average_price, 9),
                    'averaging_down_count': existing_trade.get('averaging_down_count', 0) + 1,
                    'last_averaging_down': {
                        'price': price,
                        'amount': investment_amount,
                        'timestamp': kst_now.strftime('%Y-%m-%d %H:%M:%S %Z')
                    }
                }
                
                self.db.trades.update_one(
                    {'_id': existing_trade['_id']},
                    {'$set': update_data}
                )
                
                trade_data = {**existing_trade, **update_data}
            else:
                # 새로운 거래 데이터 생성
                trade_data = {
                    'coin': coin,
                    'type': 'buy',
                    'price': price,
                    'signal_strength': signal_strength,
                    'current_price': price,
                    'profit_rate': 0,
                    'thread_id': thread_id,
                    'strategy_data': strategy_data,
                    'status': 'active',
                    'investment_amount': investment_amount,
                    'fee_amount': floor(fee_amount),
                    'actual_investment': floor(actual_investment),
                    'fee_rate': fee_rate,
                    'order_id': order_result.get('uuid'),
                    'executed_volume': order_result.get('executed_volume', 0),
                    'test_mode': is_test,
                    'timestamp': kst_now.strftime('%Y-%m-%d %H:%M:%S %Z'),
                    'averaging_down_count': 0,
                    'user_call': False
                }
                
                # 새 거래 데이터 저장
                self.db.insert_trade(trade_data)

            # 메신저로 매수 알림
            message = f"{'[TEST MODE] ' if is_test else ''}" + self.create_buy_message(trade_data)
            self.messenger.send_message(message=message, messenger_type="slack")
            
            # 포트폴리오 업데이트
            if order_result:
                portfolio = self.db.get_portfolio()
                
                portfolio['coin_list'][coin] = {
                    'amount': trade_data['executed_volume'],
                    'price': trade_data['price'],
                    'timestamp': kst_now
                }
                portfolio['available_investment'] -= investment_amount
                portfolio['current_amount'] = portfolio.get('current_amount', 0)
                
                self.db.update_portfolio(portfolio)
            
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
            active_trades = self.get_active_trades()
            
            # 해당 코인의 활성 거래 찾기
            active_trade = next((trade for trade in active_trades 
                               if trade['coin'] == coin), None)

            if not active_trade:
                return False

            # KST 시간으로 통일
            kst_now = datetime.now(timezone(timedelta(hours=9)))
            
            # 수익률 계산
            profit_rate = ((price - active_trade['price']) / active_trade['price']) * 100

            # 수수료 계산
            fee_rate = self.config['api_keys']['upbit'].get('fee', 0.05) / 100
            sell_amount = active_trade.get('executed_volume', 0) * price
            fee_amount = sell_amount * fee_rate
            actual_sell_amount = sell_amount - fee_amount  # 수수료를 제외한 실제 판매금액

            if actual_sell_amount < active_trade.get('investment_amount', 0) and active_trade.get('profit_rate', 0) >= 0.1:
                self.logger.warning(f"매도 금액이 투자 금액보다 작습니다: {coin} - 매도 금액: {actual_sell_amount:,.0f}원, 투자 금액: {active_trade.get('investment_amount', 0):,.0f}원")
                return False
            
            # 수익률 계산 (수수료 포함)
            total_fees = active_trade.get('fee_amount', 0) + fee_amount  # 매수/매도 수수료 합계
            profit_amount = actual_sell_amount - active_trade.get('investment_amount', 0)
            profit_rate = (profit_amount / active_trade.get('investment_amount', 0)) * 100

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

            update_data = {
                'status': 'closed',
                'sell_price': price,
                'sell_timestamp': kst_now,
                'sell_signal_strength': signal_strength,
                'current_strategy_data': strategy_data,
                'profit_rate': profit_rate,
                'sell_order_id': order_result.get('uuid'),
                'final_executed_volume': order_result.get('executed_volume', 0),
                'test_mode': is_test,
                'sell_fee_amount': floor(fee_amount),
                'actual_sell_amount': floor(actual_sell_amount),
                'total_fees': floor(total_fees),
                'profit_amount': floor(profit_amount),
                'profit_rate': round(profit_rate, 2),
            }
            
            # 거래 데이터 업데이트
            self.db.update_trade(active_trade['_id'], update_data)

            # 거래 내역을 trading_history 컬렉션에 저장
            trade_history = {
                'coin': coin,
                'thread_id': thread_id,
                'buy_timestamp': active_trade['timestamp'],
                'sell_timestamp': kst_now,
                'buy_price': active_trade['price'],
                'sell_price': price,
                'quantity': active_trade.get('executed_volume', 0),
                'investment_amount': active_trade.get('investment_amount', 0),
                'fee_amount': fee_amount,
                'actual_sell_amount': floor(actual_sell_amount),
                'total_fees': floor(total_fees),
                'profit_amount': floor(profit_amount),
                'profit_rate': round(profit_rate, 2),
                'buy_signal': active_trade.get('signal_strength', 0),
                'sell_signal': signal_strength,
                'strategy_data': {
                    'buy': active_trade.get('strategy_data', {}),
                    'sell': strategy_data
                },
                'test_mode': is_test
            }
            
            # trading_history에 거래 내역 저장
            self.db.trading_history.insert_one(trade_history)
            
            # trades 컬렉션에서 완료된 거래 삭제
            self.db.trades.delete_one({'_id': active_trade['_id']})
            self.logger.info(f"거래 내역 기록 완료 및 활성 거래 삭제: {coin}")

            if order_result:
                # 포트폴리오 업데이트
                portfolio = self.db.get_portfolio()
                sell_amount = floor((active_trade.get('executed_volume', 0) * price))
                profit_amount = sell_amount - active_trade.get('investment_amount', 0)
                
                # coin_list에서 판매된 코인 제거
                if coin in portfolio.get('coin_list', {}):
                    del portfolio['coin_list'][coin]
                
                # 가용 투자금액과 현재 금액 업데이트
                portfolio['available_investment'] += sell_amount
                portfolio['current_amount'] = floor(
                    (portfolio.get('current_amount', 0) - active_trade.get('investment_amount', 0) + sell_amount)
                )
                
                # 누적 수익 업데이트
                portfolio['profit_earned'] = floor(
                    portfolio.get('profit_earned', 0) + profit_amount
                )
                
                self.db.update_portfolio(portfolio)

            # 메신저로 매도 알림
            message = f"{'[TEST MODE] ' if is_test else ''}" + self.create_sell_message(
                trade_data=active_trade, 
                sell_price=price,
                buy_price=active_trade['price'],
                sell_signal=signal_strength,
                fee_amount=fee_amount,
                total_fees=total_fees
            )
            self.messenger.send_message(message=message, messenger_type="slack")
            
            return True

        except Exception as e:
            self.logger.error(f"Error in process_sell_signal: {e}")
            return False

    def generate_daily_report(self):
        """일일 리포트 생성
        
        Note:
        - 예외 처리 강화
        - 파일 처리 후 정리
        """
        try:
            # 오늘 날짜 기준으로 거래 내역 조회
            kst_today = datetime.now(timezone(timedelta(hours=9))).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            kst_tomorrow = kst_today + timedelta(days=1)

            portfolio = self.db.get_portfolio()
        
            # 거래 내역 조회
            trading_history = list(self.db.trading_history.find({
                'sell_timestamp': {
                    '$gte': kst_today,
                    '$lt': kst_tomorrow
                }
            }))
            
            # 현재 활성 거래 조회
            active_trades = list(self.db.trades.find({"status": "active"}))
            
            filename = f"투자현황-{kst_today.strftime('%Y%m%d')}.xlsx"
            with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
                # 1. 거래 내역 시트
                if trading_history:
                    history_df = pd.DataFrame(trading_history)
                    history_df['거래일자'] = history_df['sell_timestamp'].dt.strftime('%Y-%m-%d %H:%M')
                    history_df['매수가'] = history_df['buy_price'].map('{:,.0f}'.format)
                    history_df['매도가'] = history_df['sell_price'].map('{:,.0f}'.format)
                    history_df['수익률'] = history_df['profit_rate'].map('{:+.2f}%'.format)
                    history_df['투자금액'] = history_df['investment_amount'].map('{:,.0f}'.format)
                    history_df['수익금액'] = history_df['profit_amount'].map('{:+,.0f}'.format)
                    
                    # 필요한 컬럼만 선택하여 저장
                    display_columns = [
                        'coin', '거래일자', '매수가', '매도가', '수익률', 
                        '투자금액', '수익금액', 'test_mode'
                    ]
                    history_df[display_columns].to_excel(
                        writer, 
                        sheet_name='거래내역',
                        index=False
                    )
                    
                    # 거래 통계 계산
                    total_trades = len(trading_history)
                    profitable_trades = sum(1 for trade in trading_history if trade['profit_rate'] > 0)
                    total_profit = sum(trade['profit_amount'] for trade in trading_history)
                    
                    # 통계 시트 추가
                    stats_data = {
                        '항목': ['총 거래 수', '수익 거래 수', '승률', '총 수익금'],
                        '값': [
                            total_trades,
                            profitable_trades,
                            f"{(profitable_trades/total_trades*100):.1f}%" if total_trades > 0 else "0%",
                            f"₩{total_profit:,.0f}"
                        ]
                    }
                    pd.DataFrame(stats_data).to_excel(
                        writer,
                        sheet_name='거래통계',
                        index=False
                    )
                # 2. portfolio 시트
                # 포트폴리오 현황 시트 추가
                if portfolio:
                    portfolio_data = {
                        '항목': ['총 투자금액', '사용 가능 금액', '현재 평가금액', '수익 금액', '수익률'],
                        '금액': [
                            f"₩{portfolio.get('investment_amount', 0):,.0f}",
                            f"₩{portfolio.get('available_investment', 0):,.0f}",
                            f"₩{portfolio.get('current_amount', 0):,.0f}",
                            f"₩{portfolio.get('profit_earned', 0):,.0f}",
                            f"{((portfolio.get('profit_earned', 0) / portfolio.get('investment_amount', 1) - 1) * 100):+.2f}%"
                        ]
                    }
                    pd.DataFrame(portfolio_data).to_excel(
                        writer,
                        sheet_name='포트폴리오현황',
                        index=False
                    )
                # 3. 보유 현황 시트
                if active_trades:
                    holdings_df = pd.DataFrame(active_trades)
                    
                    # 보유 현황 데이터 가공
                    holdings_display = pd.DataFrame({
                        '코인': holdings_df['coin'],
                        '매수시간': holdings_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M'),
                        '매수가': holdings_df['price'].map('{:,.0f}'.format),
                        '현재가': holdings_df['current_price'].map('{:,.0f}'.format),
                        '수익률': holdings_df['profit_rate'].map('{:+.2f}%'.format),
                        '투자금액': holdings_df['investment_amount'].map('{:,.0f}'.format)
                    })
                    
                    # 보유 현황 시트에 데이터 저장
                    holdings_display.to_excel(
                        writer,
                        sheet_name='보유현황',
                        startrow=1,  # 그래프를 위한 공간 확보
                        startcol=0,
                        index=False
                    )

                    # 원형 그래프 생성
                    workbook = writer.book
                    worksheet = writer.sheets['보유현황']
                    
                    # 차트 데이터 준비
                    chart_data = {
                        'coin': holdings_df['coin'].tolist(),
                        'amount': holdings_df['investment_amount'].tolist()
                    }
                    
                    # 차트 데이터를 시트에 쓰기 (숨겨진 영역에)
                    chart_row_offset = len(holdings_df) + 5
                    worksheet.write_column(chart_row_offset, 0, chart_data['coin'])
                    worksheet.write_column(chart_row_offset, 1, chart_data['amount'])
                    
                    # 원형 차트 생성
                    pie_chart = workbook.add_chart({'type': 'pie'})
                    pie_chart.add_series({
                        'name': '투자 비중',
                        'categories': f'=보유현황!$A${chart_row_offset+1}:$A${chart_row_offset+len(chart_data["coin"])}',
                        'values': f'=보유현황!$B${chart_row_offset+1}:$B${chart_row_offset+len(chart_data["amount"])}',
                        'data_labels': {'percentage': True, 'category': True},
                    })
                    
                    # 차트 제목 및 스타일 설정
                    pie_chart.set_title({'name': '코인별 투자 비중'})
                    pie_chart.set_style(10)
                    pie_chart.set_size({'width': 500, 'height': 300})
                    
                    # 차트를 시트에 삽입
                    worksheet.insert_chart('H2', pie_chart)
                    
                    # 열 너비 자동 조정
                    for idx, col in enumerate(holdings_display.columns):
                        max_length = max(
                            holdings_display[col].astype(str).apply(len).max(),
                            len(col)
                        )
                        worksheet.set_column(idx, idx, max_length + 2)

                # 워크북 서식 설정
                for sheet in writer.sheets.values():
                    sheet.set_column('A:Z', 15)  # 기본 열 너비 설정
                    
                # 숨겨진 차트 데이터 영역 숨기기
                if active_trades:
                    worksheet.set_default_row(hide_unused_rows=True)
            
            # 메신저 알림
            stats_message = (
                f"📊 {kst_today.strftime('%Y-%m-%d')} 거래 실적\n"
                f"총 거래: {total_trades}건\n"
                f"수익 거래: {profitable_trades}건\n"
                f"승률: {(profitable_trades/total_trades*100):.1f}%\n"
                f"총 수익금: ₩{portfolio.get('profit_earned', 0):,.0f}"
            ) if trading_history else "오늘의 거래 내역이 없습니다."
            
            self.messenger.send_message(
                message=stats_message,
                messenger_type="slack"
            )
            
            self.messenger.send_message(
                message=stats_message,
                messenger_type="email",
                subject=f"{kst_today.strftime('%Y-%m-%d')} 투자 리포트",
                attachment_path=filename
            )
            
            # system_config에서 초기 투자금 가져오기
            system_config = self.db.get_sync_collection('system_config').find_one({})
            initial_investment = system_config.get('initial_investment', 1000000)
            
            # 누적 수익 계산
            total_profit_earned = portfolio.get('profit_earned', 0)
            
            # 현성 거래에서 총 투자금과 현재 가치 계산
            total_investment = system_config.get('investment_amount', 0)
            total_current_value = 0
            
            for trade in active_trades:
                investment_amount = trade.get('investment_amount', 0)
                current_price = self.upbit.get_current_price(trade['coin'])
                executed_volume = trade.get('executed_volume', 0)
                
                # 현재 가치 계산 (현재가 * 보유수량)
                current_value = current_price * executed_volume
                
                total_investment += investment_amount
                total_current_value += current_value
            
            # 수익 계산
            total_profit_amount = total_profit_earned
            total_profit_rate = (total_profit_earned / initial_investment * 100) if initial_investment > 0 else 0
            
            # 당일 수익률 계산 (0으로 나누기 방지)
            daily_profit_rate = ((total_profit_amount/total_investment)*100) if total_investment > 0 else 0

            # system_config 업데이트
            self.db.get_sync_collection('system_config').update_one(
                {},
                {
                    '$set': {
                        'total_profit_earned': total_profit_earned + total_profit_amount,
                        'last_updated': datetime.now(timezone(timedelta(hours=9)))
                    }
                }
            )
            
            portfolio_summary = (
                f"📈 포트폴리오 요약\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 초기 투자금: ₩{initial_investment:,}\n"
                f"💵 현재 평가금액: ₩{total_current_value:,.0f}\n"
                f"📊 누적 수익률: {total_profit_rate:+.2f}% (₩{total_profit_earned:+,.0f})\n"
                f"📈 당일 수익률: {daily_profit_rate:+.2f}% (₩{total_profit_amount:+,.0f})\n"
                f"🔢 보유 코인: {len(active_trades)}개\n"
            )
            
            message = "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n" + portfolio_summary + "\n" + "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            
            # 포트폴리오 정보 추가
            portfolio = self.db.get_portfolio()
            
            # 포트폴리오 정보 업데이트
            portfolio_update = {
                'current_amount': floor(total_current_value),
                'investment_amount': initial_investment,
                'available_investment': floor(initial_investment - total_investment),
                'last_updated': datetime.now(timezone(timedelta(hours=9))),
                'coin_list': {
                    trade['coin']: {
                        'amount': trade.get('executed_volume', 0),
                        'price': trade.get('price', 0),
                        'current_price': self.upbit.get_current_price(trade['coin']),
                        'investment_amount': trade.get('investment_amount', 0)
                    } for trade in active_trades
                }
            }
            
            self.db.update_portfolio(portfolio_update)
            
            # Slack으로 메시지 전송
            self.messenger.send_message(message=message, messenger_type="slack")
            
            # 리포트 전송 상태 업데이트
            self.db.update_daily_profit_report_status(reported=True)
            
            self.logger.info(f"일일 리포트 생성 및 전송 완료: {kst_today.strftime('%Y-%m-%d')}")
            
        except Exception as e:
            self.logger.error(f"일일 리포트 생성 중 오류 발생: {str(e)}")
            # 리포트 전송 실패 시 상태 업데이트
            self.db.update_daily_profit_report_status(reported=False)
            raise
        finally:
            # 파일 정리
            if os.path.exists(filename):
                os.remove(filename)

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
        if trade_data.get('averaging_down_count', 0) > 0:
            buy_reason = "물타기"
            last_averaging = trade_data.get('last_averaging_down', {})
            additional_info = (
                f" 물타기 횟수: {trade_data['averaging_down_count']}회\n"
                f" 평균 매수가: {trade_data['price']:,}원\n"
                f" 이전 매수가: {last_averaging.get('price', 0):,}원\n"
                f" 추가 매수액: {last_averaging.get('amount', 0):,}원\n"
            )
        else:
            buy_reason = "상승세 감지" if strategy_data.get('uptrend_signal', 0) > 0.5 else "하락세 종료"
            additional_info = ""

        kst_now = datetime.now(timezone(timedelta(hours=9)))

        message = (
            f"------------------------------------------------\n"
            f"Coin: {trade_data['coin']}, 구매 ({buy_reason})\n"
            f" 구매 시간: {kst_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f" 구매 가격: {trade_data['price']:,}\n"
            f" 구매 신호: {trade_data['signal_strength']:.2f}\n"
            f" Coin-rank: {trade_data.get('thread_id', 'N/A')}\n"
            f" 투자 금액: W{trade_data.get('investment_amount', 0):,}\n"
        )

        # 물타기 정보 추가
        if additional_info:
            message += additional_info

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

    def create_sell_message(self, trade_data: Dict, sell_price: float, buy_price: float,
                           sell_signal: float, fee_amount: float = 0, 
                           total_fees: float = 0) -> str:
        """매도 메시지 생성
        
        매도 시점의 전략 데이터를 기반으로 메시지를 생성합니다.

        Args:
            trade_data: 거래 데이터
            sell_price: 판매 가격
            sell_signal: 판매 신호
        Returns:
            매도 메시지
        """
        profit_amount = floor((sell_price - trade_data['price']) * trade_data.get('executed_volume', 0))
        total_investment = trade_data.get('investment_amount', 0) + profit_amount
        kst_now = datetime.now(timezone(timedelta(hours=9)))

        message = (
            f"------------------------------------------------\n"
            f"Coin: {trade_data['coin']}, 판매\n"
            f" 판매 시간: {kst_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f" 구매 가격: {buy_price:,}\n"
            f" 판매 가격: {sell_price:,}\n"
            f" 판매 신호: {sell_signal:.2f}\n"
            f" Coin-rank: {trade_data.get('thread_id', 'N/A')}\n"
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

        message += (
            f"  └ 매도 수수료: ₩{fee_amount:,.0f}\n"
            f"  └ 총 수수료: ₩{total_fees:,.0f}\n"
            f"  └ 순수익: ₩{profit_amount:+,.0f} (수수료 차감 후)\n"
        )

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

    def generate_hourly_report(self):
        """시간별 리포트 생성
        
        매 시간 정각에 실행되며 현재 보유 포지션과 투자 현황을 보고합니다.
        - 현재 보유 코인 목록
        - 각 코인별 매수 시간과 임계값
        - 총 투자금액
        """
        try:
            self.logger.info("시간별 리포트 생성 시작")
            now = datetime.now(timezone(timedelta(hours=9)))
            current_time = now.strftime('%Y-%m-%d %H:00')
            message = ""
            
            # 변수 초기화
            total_investment = 0
            total_current_value = 0

            # 활성 거래 조회
            active_trades = list(self.db.get_sync_collection('trades').find({
                'status': 'active'
            }))
            
            # 포트폴리오 정보 조회
            portfolio = self.db.get_sync_collection('portfolio').find_one({'_id': 'main'})
            if not portfolio:
                self.logger.warning("포트폴리오 정보를 찾을 수 없습니다")
                return
            
            # 각 코인별 상세 정보
            for trade in active_trades:
                # timestamp를 KST로 변환
                trade_time = trade['timestamp']
                if trade_time.tzinfo is None:
                    trade_time = trade_time.replace(tzinfo=timezone(timedelta(hours=9)))
                
                hold_time = now - trade_time
                hours = hold_time.total_seconds() / 3600
                
                # 현재 가격 조회
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
                    f"  └ 매수 임계값: {trade['strategy_data'].get('overall_signal', 'N/A')}\n"
                    f"  └ 투자금액: ₩{investment_amount:,}\n"
                )
                message += coin_info + "\n"
                time.sleep(0.07)
            
            # 전체 포트폴리오 수익률
            total_profit_rate = ((total_current_value - total_investment) / total_investment * 100) if total_investment > 0 else 0
            total_profit_amount = total_current_value - total_investment
            
            # system_config에서 초기 투자금 가져오기
            system_config = self.db.get_sync_collection('system_config').find_one({})
            initial_investment = system_config.get('initial_investment', 1000000)
            
            # 누적 수익 계산
            total_profit_earned = portfolio.get('profit_earned', 0)
            
            # 현재 수익률 계산 (0으로 나누기 방지)
            total_profit_rate = (total_profit_earned / initial_investment * 100) if initial_investment > 0 else 0
            
            # 일일 리포트 후 system_config 업데이트
            self.db.get_sync_collection('system_config').update_one(
                {},
                {
                    '$set': {
                        'total_profit_earned': total_profit_earned,
                        'last_updated': datetime.now(timezone(timedelta(hours=9)))
                    }
                }
            )

            # 당일 수익률 계산 (0으로 나누기 방지)
            daily_profit_rate = ((total_profit_amount/total_investment)*100) if total_investment > 0 else 0
            
            portfolio_summary = (
                f"📈 포트폴리오 요약\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 초기 투자금: ₩{initial_investment:,}\n"
                f"💵 현재 평가금액: ₩{total_current_value:,.0f}\n"
                f"📊 보유 코인 누적 수익률: {total_profit_rate:+.2f}% (₩{total_profit_earned:+,.0f})\n"
                f"📈 당일 수익률: {daily_profit_rate:+.2f}% (₩{total_profit_amount:+,.0f})\n"
                f"🔢 보유 코인: {len(active_trades)}개\n"
            )
            
            message = portfolio_summary + "\n" + message + "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            
            # 포트폴리오 정보 추가
            portfolio = self.db.get_portfolio()
            
            message += (
                f"\n📊 포트폴리오 현황\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 총 투자금액: ₩{portfolio.get('investment_amount', 0):,.0f}\n"
                f"💵 사용 가능 금액: ₩{portfolio.get('available_investment', 0):,.0f}\n"
                f"📈 당일 수익률: {daily_profit_rate:+.2f}% (₩{total_profit_amount:+,.0f})\n"
                f"📊 보유 코인 누적 수익률: {total_profit_rate:+.2f}% (₩{total_profit_amount:+,.0f})\n"
                f"🔢 보유 코인: {len(active_trades)}개\n\n"
            )
            
            # Slack으로 메시지 전송
            self.messenger.send_message(message=message, messenger_type="slack")
            
            self.logger.info(f"시간별 리포트 생성 및 전송 완료: {current_time}")
            
        except Exception as e:
            self.logger.error(f"시간별 리포트 생성 중 오류 발생: {e}")
            raise

    def update_strategy_data(self, coin: str, thread_id: int, price: float, strategy_results: Dict):
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
                
                # 활성 거래 조회 및 업데이트
                active_trades = self.db.trades.find(
                    {
                        'coin': coin, 
                        'status': 'active'
                    }
                )
                current_price = strategy_results.get('price', price)
                
                for active_trade in active_trades:
                    # 수익률 계산 시 0으로 나누기 방지
                    base_price = active_trade.get('price', current_price)
                    if base_price <= 0:
                        self.logger.warning(f"{coin} - 유효하지 않은 매수가: {base_price}")
                        profit_rate = 0
                    else:
                        profit_rate = ((current_price / base_price) - 1) * 100

                    # 현재 가격 업데이트
                    self.db.trades.update_one(
                        {'_id': active_trade['_id']},
                        {
                            '$set': {
                                'current_price': current_price,
                                'thread_id': thread_id,
                                'current_value': current_price * active_trade.get('executed_volume', 0),
                                'signal_strength': strategy_results.get('overall_signal', 0),
                                'profit_rate': profit_rate,
                                'last_updated': datetime.now(timezone(timedelta(hours=9))),
                                'user_call': active_trade.get('user_call', False)
                            }
                        }
                    )
                    
                    self.logger.debug(f"가격 정보 업데이트 완료: {coin} - 현재가: {current_price:,}원")
                    
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

    async def user_call_buy(self, coin: str, price: float, immediate: bool = False) -> Dict:
        """사용자 매수 주문
        
        Args:
            coin: 코인명
            price: 주문 가격
            immediate: 즉시 체결 여부
            
        Returns:
            Dict: 주문 결과
        """
        try:
            # 테스트 모드 확인
            is_test = self.config.get('test_mode', True)
            self.logger.info(f"매수 주문 시작 - 코인: {coin}, 가격: {price:,}, 즉시체결: {immediate}")
            
            # 전략/시장 데이터 조회
            strategy_data = await self.db.get_collection('strategy_data').find_one({'coin': coin})
            if not strategy_data:
                self.logger.warning(f"{coin} - 전략 데이터 없음")
                return {'success': False, 'message': '전략 데이터 없음'}
            
            # 주문 데이터 생성
            order_data = {
                'coin': coin,
                'type': 'buy',
                'price': price,
                'status': 'pending',
                'immediate': immediate,
                'created_at': datetime.now(timezone(timedelta(hours=9))),
                'updated_at': datetime.now(timezone(timedelta(hours=9))),
                'is_test': is_test,
                'strategy_data': strategy_data
            }
            
            # 주문 컬렉션 초기화 확인 및 생성
            await self._ensure_order_collection()
            
            # 주문 추가
            result = await self.db.get_collection('order_list').insert_one(order_data)
            
            if immediate:
                # 즉시 체결인 경우 바로 process_buy_signal 호출
                await self.process_buy_signal(
                    coin=coin,
                    thread_id=0,  # 사용자 주문은 thread_id 0 사용
                    signal_strength=1.0,
                    price=price,
                    strategy_data=strategy_data
                )
                return {'success': True, 'message': '즉시 매수 주문 처리됨'}
            
            return {'success': True, 'message': '매수 주문이 등록되었습니다', 'order_id': str(result.inserted_id)}
            
        except Exception as e:
            self.logger.error(f"매수 주문 처리 중 오류: {str(e)}")
            return {'success': False, 'message': f'주문 처리 실패: {str(e)}'}

    async def user_call_sell(self, coin: str, price: float, immediate: bool = False) -> Dict:
        """사용자 매도 주문
        
        Args:
            coin: 코인명
            price: 주문 가격
            immediate: 즉시 체결 여부
            
        Returns:
            Dict: 주문 결과
        """
        try:
            # 테스트 모드 확인
            is_test = self.config.get('test_mode', True)
            self.logger.info(f"매도 주문 시작 - 코인: {coin}, 가격: {price:,}, 즉시체결: {immediate}")
            
            # 활성 거래 확인
            active_trade = await self.db.get_collection('trades').find_one({
                'coin': coin,
                'status': 'active'
            })
            
            if not active_trade:
                return {'success': False, 'message': '해당 코인의 활성 거래가 없습니다'}
            
            # 주문 데이터 생성
            order_data = {
                'coin': coin,
                'type': 'sell',
                'price': price,
                'status': 'pending',
                'immediate': immediate,
                'created_at': datetime.now(timezone(timedelta(hours=9))),
                'updated_at': datetime.now(timezone(timedelta(hours=9))),
                'is_test': is_test,
                'trade_data': active_trade
            }
            
            # 주문 컬렉션 초기화 확인 및 생성
            await self._ensure_order_collection()
            
            # 주문 추가
            result = await self.db.get_collection('order_list').insert_one(order_data)
            
            if immediate:
                # 즉시 체결인 경우 바로 process_sell_signal 호출
                await self.process_sell_signal(
                    coin=coin,
                    thread_id=active_trade['thread_id'],
                    signal_strength=1.0,
                    price=price,
                    strategy_data={'forced_sell': True}
                )
                return {'success': True, 'message': '즉시 매도 주문 처리됨'}
            
            return {'success': True, 'message': '매도 주문이 등록되었습니다', 'order_id': str(result.inserted_id)}
            
        except Exception as e:
            self.logger.error(f"매도 주문 처리 중 오류: {str(e)}")
            return {'success': False, 'message': f'주문 처리 실패: {str(e)}'}

    async def _ensure_order_collection(self):
        """주문 컬렉션 초기화 확인"""
        try:
            collections = await self.db.get_collection('order_list').list_collection_names()
            if 'order_list' not in collections:
                await self.db.get_collection('order_list').create_index([
                    ('coin', 1),
                    ('status', 1),
                    ('created_at', -1)
                ])
                self.logger.info("order_list 컬렉션 생성 완료")
        except Exception as e:
            self.logger.error(f"order_list 컬렉션 초기화 중 오류: {str(e)}")
            raise