import time
from typing import Dict, List, Any
import logging
from database.mongodb_manager import MongoDBManager
from messenger.Messenger import Messenger
from datetime import datetime, timezone, timedelta
import pandas as pd
import yaml
from trading.long_term_trading_manager import LongTermTradingManager
import os
from math import floor
from utils.time_utils import TimeUtils
from control_center.exchange_factory import ExchangeFactory
from monitoring.memory_monitor import MemoryProfiler, memory_profiler

class TradingManager:
    """
    거래 관리자
    
    거래 신호 처리 및 거래 데이터 관리를 담당합니다.
    """
    def __init__(self, exchange_name: str):
        self.db = MongoDBManager(exchange_name=exchange_name)
        self.config = self._load_config()
        self.messenger = Messenger(self.config)
        self.logger = logging.getLogger('investment-center')
        self.exchange_name = exchange_name  
        self.exchange = self._initialize_exchange(exchange_name)
        self.long_term_trading_manager = LongTermTradingManager(self.db, self.exchange_name, self.config)
        self.test_mode = self.config.get('mode') == 'test' or self.db.get_portfolio('test_mode')
        self.memory_profiler = MemoryProfiler()

    def _load_config(self) -> Dict:
        """설정 파일 로드"""
        try:
            with open("resource/application.yml", 'r', encoding='utf-8') as file:
                return yaml.safe_load(file)
        except Exception as e:
            self.logger.error(f"설정 파일 로드 실패: {str(e)}")
            return {}
    
    def _initialize_exchange(self, exchange_name: str) -> Any:
        """거래소 초기화"""
        try:
            exchange = ExchangeFactory.create_exchange(exchange_name, self.config)
            self.logger.info(f"{exchange_name} 거래소 초기화 성공")
            return exchange
        except Exception as e:
            self.logger.error(f"거래소 초기화 실패: {str(e)}")
            raise

    
    def process_buy_signal(self, market: str, exchange: str, thread_id: int, signal_strength: float, 
                               price: float, strategy_data: Dict, buy_message: str = None):
        """매수 신호 처리"""
        try:
            with self.db.transaction():  # 트랜잭션 처리 추가
                # KST 시간 가져오기
                kst_now = TimeUtils.get_current_kst()
                self.logger.debug(f"현재 KST 시간: {TimeUtils.format_kst(kst_now)}")
                
                # 투자 가능 금액 확인
                if not self.check_investment_limit():
                    self.logger.warning(f"전체 투자 한도 초과: thread_id={thread_id}")
                    return False

                # 테스트 모드 확인
                is_test = self.test_mode
                
                # 수수료 계산
                fee_rate = self.config['api_keys']['upbit'].get('fee', 0.05) / 100  # 0.05% -> 0.0005
                investment_amount = strategy_data.get('investment_amount', 0)
                fee_amount = investment_amount * fee_rate
                actual_investment = investment_amount - fee_amount

                # 장기 투자 여부 확인 및 기존 거래 정보 조회
                long_term_trade = self.db.long_term_trades.find_one({
                        'market': market,
                        'exchange': exchange,
                        'status': 'active'
                    })
                existing_trade = None
                if long_term_trade:
                    existing_trade = self.db.trades.find_one({
                        'market': market,
                        'exchange': exchange,
                        'status': 'converted'
                    })
                    self.logger.info(f"물타기 신호 감지: {market} - 현재 수익률: {existing_trade.get('profit_rate', 0):.2f}%")

                order_result = None
                if not is_test:
                    # 실제 매수 주문 실행
                    order_result = self.investment_center.exchange.place_order(
                        market=market,
                        side='bid',
                        price=price,
                        volume=actual_investment / price
                    )

                    if not order_result:
                        self.logger.error(f"매수 주문 실패: {market}")
                        return False
                else:
                    # 테스트 모드 로그
                    self.logger.info(f"[TEST MODE] 가상 매수 신호 처리: {market} @ {price:,}원 (수수료: {fee_amount:,.0f}원)")
                    order_result = {
                        'uuid': f'test_buy_{kst_now.timestamp()}',
                        'executed_volume': actual_investment / price,  # 수수료를 제외한 수량
                        'price': price
                    }

                if existing_trade and long_term_trade:
                    # 기존 거래 정보 업데이트 (장기 투자)
                    total_investment = long_term_trade['total_investment'] + investment_amount
                    total_volume = long_term_trade['executed_volume'] + order_result['executed_volume']
                    average_price = (long_term_trade['average_price'] * long_term_trade['executed_volume'] + 
                                   price * order_result['executed_volume']) / total_volume

                    # 새로운 포지션 정보
                    new_position = {
                        'price': price,
                        'amount': investment_amount,
                        'executed_volume': order_result['executed_volume'],
                        'timestamp': kst_now
                    }

                    # positions 배열에 새로운 포지션 추가
                    self.db.long_term_trades.update_one(
                        {'_id': long_term_trade['_id']},  # long_term_trade의 _id 사용
                        {
                            '$set': {
                                'total_investment': total_investment,
                                'executed_volume': total_volume,
                                'average_price': round(average_price, 9),
                                'last_updated': kst_now,
                            },
                            '$push': {
                                'positions': new_position  # positions 배열에 새 포지션 추가
                            }
                        }
                    )

                    # trades 컬렉션 업데이트
                    update_data2 = {
                        'investment_amount': total_investment,
                        'actual_investment': existing_trade['actual_investment'] + actual_investment,
                        'executed_volume': total_volume,
                        'price': round(average_price, 9),
                        'is_long_term_trade': True,
                    }

                    self.db.trades.update_one(
                        {'_id': existing_trade['_id']},
                        {'$set': update_data2}
                    )
                    
                    # 업데이트된 거래 데이터 조회
                    trade_data = {
                        **existing_trade,
                        **update_data2,
                        'positions': long_term_trade['positions'] + [new_position]
                    }
                else:
                    # 새로운 거래 데이터 생성
                    trade_data = {
                        'market': market,
                        'exchange': exchange,
                        'type': 'buy',
                        'price': price,
                        'buy_signal': signal_strength,
                        'sell_signal': 0,
                        'signal_strength': signal_strength,
                        'current_price': price,
                        'profit_rate': 0,
                        'buy_reason': buy_message,
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
                        'timestamp': kst_now,
                        'averaging_down_count': 0,
                        'user_call': False,
                        'is_tradeable': False,
                        'is_long_term_trade': False
                    }
                    
                    # 새 거래 데이터 저장
                    self.db.insert_trade(trade_data)

                # 메신저로 매수 알림
                message = f"{'[TEST MODE] ' if is_test else ''}" + self.create_buy_message(
                    trade_data=trade_data,
                    buy_message=buy_message
                )
                self.messenger.send_message(message=message, messenger_type="slack")
                
                # 포트폴리오 업데이트
                if order_result:
                    portfolio = self.db.get_portfolio(exchange)
                    
                    # market_list가 없는 경우 초기화
                    if 'market_list' not in portfolio:
                        portfolio['market_list'] = []
                        portfolio['exchange'] = exchange
                    
                    # 해당 마켓 정보 업데이트
                    portfolio['market_list'].append({
                        'market': market,
                        'amount': trade_data['executed_volume'],
                        'price': trade_data['price'],
                        'timestamp': kst_now
                    })
                    
                    # 현재 금액 업데이트
                    current_amount = portfolio.get('current_amount', 0)
                    portfolio['current_amount'] = floor(current_amount - investment_amount)
                    
                    self.db.update_portfolio(portfolio)
                
                return True

        except Exception as e:
            self.logger.error(f"매수 처리 중 오류: {str(e)}")
            self.messenger.send_message(f"매수 처리 실패: {market}", "slack")
            return False

    
    def process_sell_signal(self, market: str, exchange: str, thread_id: int, signal_strength: float, 
                            price: float, strategy_data: Dict, sell_message: str = None):
        """매도 신호 처리
        
        개선사항:
        - current_strategy_data 추가하여 매도 시점의 전략 데이터 저장
        - 수익률 계산 및 기록
        """
        try:
            # 활성 거래 조회
            active_trade = self.db.trades.find_one({
                "market": market,
                "exchange": exchange
            })
            
            if not active_trade:
                return False

            # KST 시간으로 통일
            kst_now = TimeUtils.get_current_kst()
            
            # 수익률 계산
            profit_rate = ((price - active_trade['price']) / active_trade['price']) * 100

            # 수수료 계산
            fee_rate = self.config['api_keys']['upbit'].get('fee', 0.05) / 100
            sell_amount = active_trade.get('executed_volume', 0) * price
            fee_amount = sell_amount * fee_rate
            actual_sell_amount = sell_amount - fee_amount  # 수수료를 제외한 실제 판매금액

            # 수익률 계산 (수수료 포함)
            total_fees = active_trade.get('fee_amount', 0) + fee_amount  # 매수/매도 수수료 합계
            profit_amount = actual_sell_amount - active_trade.get('investment_amount', 0)
            profit_rate = (profit_amount / active_trade.get('investment_amount', 0)) * 100

            order_result = None
            # 테스트 모드 확인 (self.test_mode 사용)
            if not self.test_mode:
                # 실제 매도 주문 실행
                order_result = self.exchange.place_order(
                    market=market,
                    side='ask',
                    price=price,
                    volume=active_trade.get('executed_volume', 0)
                )

                if not order_result:
                    self.logger.error(f"매도 주문 실패: {market}")
                    return False
            else:
                # 테스트 모드 로그
                self.logger.info(f"[TEST MODE] 가상 매도 신호 처리: {market} @ {price:,}원")
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
                'test_mode': self.test_mode,
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
                'market': market,
                'thread_id': thread_id,
                'profit_rate': round(profit_rate, 2),
                'profit_amount': floor(profit_amount),
                'buy_reason': active_trade.get('buy_reason', 'N/A'),
                'sell_reason': sell_message,
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
                'exchange': exchange,
                'buy_signal': active_trade.get('buy_signal', 0),
                'sell_signal': signal_strength,
                'strategy_data': {
                    'buy': active_trade.get('strategy_data', {}),
                    'sell': strategy_data
                },
                'test_mode': self.test_mode
            }
            
            # trading_history에 거래 내역 저장
            self.db.trading_history.insert_one(trade_history)
            
            # trades 컬렉션에서 완료된 거래 삭제
            self.db.trades.delete_one({'_id': active_trade['_id']})
            self.db.long_term_trades.delete_one({'market': market, 'exchange': exchange})
            self.logger.info(f"거래 내역 기록 완료 및 활성 거래 삭제: {market}")

            if order_result:
                # 포트폴리오 업데이트
                portfolio = self.db.get_portfolio(exchange)
                
                # market_list에서 판매된 마켓 제거
                if 'market_list' in portfolio:
                    portfolio['market_list'] = [
                        item for item in portfolio['market_list'] 
                        if item.get('market') != market
                    ]
                
                # 가용 투자금액과 현재 금액 업데이트
                current_amount = portfolio['current_amount']
                portfolio['current_amount'] = floor(current_amount + floor(actual_sell_amount))
                
                # 누적 수익 업데이트
                portfolio['profit_earned'] = floor(
                    portfolio.get('profit_earned', 0) + profit_amount
                )
                
                self.db.update_portfolio(portfolio)

            # 메신저로 매도 알림
            message = f"{'[TEST MODE] ' if self.test_mode else ''}" + self.create_sell_message(
                trade_data=active_trade, 
                sell_price=price,
                buy_price=active_trade['price'],
                sell_signal=signal_strength,
                fee_amount=fee_amount,
                total_fees=total_fees,
                sell_message=sell_message
            )
            self.messenger.send_message(message=message, messenger_type="slack")
            
            return True

        except Exception as e:
            self.logger.error(f"Error in process_sell_signal: {e}")
            return False

    
    def generate_daily_report(self, exchange: str):
        """일일 리포트 생성
        
        매일 20시에 실행되며 하루 동안의 거래 실적과 현재 포지션을 보고합니다.
        - 당일 거래 요약
        - 수익/손실 현황
        - 포트폴리오 상태
        - 장기 투자 현황
        """
        try:
            self.logger.info("일일 리포트 생성 시작")
            
            # KST 시간으로 오늘 날짜 설정
            kst_today = TimeUtils.ensure_aware(
                TimeUtils.get_current_kst().replace(hour=0, minute=0, second=0, microsecond=0)
            )
            kst_tomorrow = kst_today + timedelta(days=1)

            portfolio = self.db.get_portfolio(exchange)
        
            # 거래 내역 조회 시 timezone 정보 포함
            trading_history = list(self.db.trading_history.find({
                'sell_timestamp': {
                    '$gte': TimeUtils.to_mongo_date(kst_today),
                    '$lt': TimeUtils.to_mongo_date(kst_tomorrow)
                },
                'exchange': exchange
            }))
            
            filename = f"투자현황-{kst_today.strftime('%Y%m%d')}.xlsx"
            
            # 현재 활성 거래 조회
            active_trades = list(self.db.trades.find({"status": {"$in": ["active", "converted"]}}))
            
            with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
                # 1. 거래 내역 시트
                if trading_history:
                    history_df = pd.DataFrame(trading_history)
                    # datetime 객체를 KST로 변환
                    history_df['거래일자'] = pd.to_datetime(history_df['sell_timestamp']).apply(
                        lambda x: TimeUtils.ensure_aware(
                            TimeUtils.from_mongo_date(x)
                        ).strftime('%Y-%m-%d %H:%M')
                    )
                    history_df['매수일자'] = pd.to_datetime(history_df['buy_timestamp']).apply(
                        lambda x: TimeUtils.ensure_aware(
                            TimeUtils.from_mongo_date(x)
                        ).strftime('%Y-%m-%d %H:%M')
                    )
                    history_df['거래종목'] = history_df['market']
                    history_df['매수가'] = history_df['buy_price'].map('{:,.0f}'.format)
                    history_df['매도가'] = history_df['sell_price'].map('{:,.0f}'.format)
                    history_df['수익률'] = history_df['profit_rate'].map('{:+.2f}%'.format)
                    history_df['투자금액'] = history_df['investment_amount'].map('{:,.0f}'.format)
                    history_df['수익금액'] = history_df['profit_amount'].map('{:+,.0f}'.format)
                    
                    # 필요한 컬럼만 선택하여 저장
                    display_columns = [
                        '거래종목', '거래일자', '매수일자', '매수가', '매도가', '수익률', 
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
                            f"{(portfolio.get('profit_earned', 0) / portfolio.get('investment_amount', 1) * 100):+.2f}%"
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
                    
                    # 보유 시간 계산 시 timezone 고려
                    holdings_df['보유기간'] = holdings_df['timestamp'].apply(
                        lambda x: (TimeUtils.get_current_kst() - TimeUtils.ensure_aware(
                            TimeUtils.from_mongo_date(x))).total_seconds() / 3600
                    )
                    
                    holdings_display = pd.DataFrame({
                        '거래종목': holdings_df['market'],
                        'RANK': holdings_df['thread_id'],
                        '매수시간': holdings_df['timestamp'].apply(
                            lambda x: TimeUtils.ensure_aware(
                                TimeUtils.from_mongo_date(x)
                            ).strftime('%Y-%m-%d %H:%M')
                        ),
                        '매수가': holdings_df['price'].map('{:,.0f}'.format),
                        '현재가': holdings_df['current_price'].map('{:,.0f}'.format),
                        '수익률': holdings_df['profit_rate'].map('{:+.2f}%'.format),
                        '투자금액': holdings_df['investment_amount'],
                        '보유시간': holdings_df['보유기간'].map('{:.1f}시간'.format)
                    })
                    
                    # 보유 현황 시트에 데이터 저장
                    holdings_display.to_excel(
                        writer,
                        sheet_name='보유현황',
                        startrow=1,
                        startcol=0,
                        index=False
                    )

                    # 숫자 형식 설정
                    workbook = writer.book
                    worksheet = writer.sheets['보유현황']
                    number_format = workbook.add_format({'num_format': '#,##0'})
                    worksheet.set_column('G:G', 15, number_format)  # 투자금액 열 서식 설정
                    
                    # 차트 색상 정의 (더 많은 색상 추가)
                    chart_colors = [
                        '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEEAD',  # 밝은 계열
                        '#D4A5A5', '#9B59B6', '#3498DB', '#E67E22', '#2ECC71',  # 중간 계열
                        '#FF8C42', '#7FB069', '#D65DB1', '#6C5B7B', '#C06C84',  # 진한 계열
                        '#F8B195', '#355C7D', '#99B898', '#E84A5F', '#2A363B',  # 파스텔 계열
                        '#084C61', '#DB504A', '#56A3A6', '#FF4B3E', '#4A90E2'   # 추가 색상
                    ]
                    
                    # 원형 그래프 생성
                    chart_format = {'type': 'pie', 'subtype': 'pie'}
                    pie_chart = workbook.add_chart(chart_format)
                    
                    pie_chart.add_series({
                        'name': '투자 비중',
                        'categories': f'=보유현황!$A$3:$A${len(holdings_display) + 2}',
                        'values': f'=보유현황!$G$3:$G${len(holdings_display) + 2}',
                        'data_labels': {
                            'percentage': True,
                            'category': True,
                            'position': 'best_fit',  # 자동으로 최적의 위치 선정
                            'leader_lines': True,
                            'font': {'size': 9},
                            'separator': '\n',  # 줄바꿈으로 레이블 구분
                            'format': {
                                'border': {'color': 'white', 'width': 1},
                                'fill': {'color': 'white'},
                                'font': {'color': 'black', 'bold': True}
                            }
                        },
                        'points': [
                            {
                                'fill': {'color': chart_colors[i % len(chart_colors)]},
                                'border': {'color': 'white', 'width': 1}
                            }
                            for i in range(len(holdings_display))
                        ]
                    })
                    
                    # 차트 크기와 위치 조정
                    pie_chart.set_title({
                        'name': '마켓별 투자 비중',
                        'name_font': {'size': 12, 'bold': True},
                        'overlay': False
                    })
                    
                    pie_chart.set_size({'width': 600, 'height': 400})  # 크기 증가
                    pie_chart.set_legend({
                        'position': 'right',  # 범례 위치 변경
                        'font': {'size': 9},
                        'layout': {'x': 1.1, 'y': 0.25}  # 범례 위치 미세 조정
                    })
                    
                    # 차트 삽입 위치 조정
                    worksheet.insert_chart('I2', pie_chart, {'x_offset': 25, 'y_offset': 10})

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
            total_max_investment = system_config.get('total_max_investment', 1000000)
            
            # 누적 수익 계산
            total_profit_earned = portfolio.get('profit_earned', 0)
            
            # 현성 거래에서 총 투자금과 현재 가치 계산
            total_investment = system_config.get('investment_amount', 0)
            total_current_value = 0
            
            for trade in active_trades:
                investment_amount = trade.get('investment_amount', 0)
                current_price = self.exchange.get_current_price(trade['market'])
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
                        'total_max_investment': total_max_investment + total_profit_amount,
                        'last_updated': TimeUtils.get_current_kst()
                    }
                }
            )
            
            portfolio_summary = (
                f"📈 포트폴리오 요약\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 초기 투자금: ₩{initial_investment:,}\n"
                f"💰 현재 투자금: ₩{total_max_investment:,}\n"
                f"💵 현재 평가금액: ₩{total_current_value:,.0f}\n"
                f"📊 누적 수익률: {total_profit_rate:+.2f}% (₩{total_profit_earned:+,.0f})\n"
                f"📈 당일 수익률: {daily_profit_rate:+.2f}% (₩{total_profit_amount:+,.0f})\n"
                f"🔢 보유 마켓: {len(active_trades)}개\n"
            )
            
            message = "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n" + portfolio_summary + "\n" + "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            
            # 포트폴리오 정보 추가
            portfolio = self.db.get_portfolio(exchange_name=exchange)
            
            # 포트폴리오 정보 업데이트
            portfolio_update = {
                'exchange': exchange,
                'current_amount': floor(total_current_value),
                'investment_amount': total_max_investment + total_profit_amount,
                'profit_earned': 0,
                'last_updated': TimeUtils.get_current_kst(),
                'market_list': [
                    {
                        'market': trade['market'],
                        'amount': trade.get('executed_volume', 0),
                        'price': trade.get('price', 0),
                        'current_price': self.exchange.get_current_price(trade['market']),
                        'investment_amount': trade.get('investment_amount', 0),
                        'timestamp': TimeUtils.get_current_kst()
                    } for trade in active_trades
                ]
            }
            
            # 포트폴리오 업데이트   
            self.db.update_portfolio(portfolio_update)
            
            # 일일 수익 업데이트
            daily_profit_update = {
                'date': kst_today,
                'exchange': exchange,
                'profit_earned': total_profit_earned,
                'profit_rate': total_profit_rate,
                'total_investment': total_investment,
                'total_current_value': total_current_value,
                'reported': True,
                'trading_summary': {
                    'total_trades': total_trades,
                    'profitable_trades': profitable_trades,
                    'win_rate': (profitable_trades/total_trades*100) if total_trades > 0 else 0,
                    'daily_profit_amount': total_profit_amount,
                    'daily_profit_rate': daily_profit_rate
                },
                'portfolio_status': {
                    'initial_investment': initial_investment,
                    'current_investment': total_max_investment,
                    'available_amount': portfolio.get('available_investment', 0),
                    'total_holdings': len(active_trades),
                    'market_list': [
                        {
                            'market': trade['market'],
                            'amount': trade.get('executed_volume', 0),
                            'buy_price': trade.get('price', 0),
                            'current_price': self.exchange.get_current_price(trade['market']),
                            'investment_amount': trade.get('investment_amount', 0),
                            'profit_rate': trade.get('profit_rate', 0),
                            'holding_time': (TimeUtils.get_current_kst() - TimeUtils.ensure_aware(
                                TimeUtils.from_mongo_date(trade['timestamp'])
                            )).total_seconds() / 3600
                        } for trade in active_trades
                    ]
                },
                'long_term_status': {
                    'active_count': len(long_term_trades),
                    'total_investment': long_term_summary['total_investment'],
                    'total_current_value': long_term_summary['total_current_value'],
                    'avg_profit_rate': long_term_summary['avg_profit_rate'],
                    'holdings': [
                        {
                            'market': detail['market'],
                            'total_investment': detail['total_investment'],
                            'current_value': detail['current_value'],
                            'profit_rate': detail['profit_rate'],
                            'position_count': detail['position_count'],
                            'days_active': detail['days_active']
                        } for detail in sorted_details
                    ]
                },
                'trading_history': [
                    {
                        'market': trade['market'],
                        'buy_price': trade['buy_price'],
                        'sell_price': trade['sell_price'],
                        'profit_rate': trade['profit_rate'],
                        'profit_amount': trade['profit_amount'],
                        'investment_amount': trade['investment_amount'],
                        'buy_timestamp': trade['buy_timestamp'],
                        'sell_timestamp': trade['sell_timestamp'],
                        'test_mode': trade.get('test_mode', False)
                    } for trade in trading_history
                ],
                'timestamp': TimeUtils.get_current_kst()
            }
            
            # MongoDB에 저장
            self.db.daily_profit.insert_one(daily_profit_update)
            
            # 오후 8시 이전 거래 내역 삭제
            kst_cutoff = kst_today.replace(hour=20, minute=0, second=0, microsecond=0)
            self.db.trading_history.delete_many({
                'sell_timestamp': {'$lt': kst_cutoff},
                'exchange': exchange
            })
            self.logger.info(f"오후 8시 이전 거래 내역 삭제 완료 (기준시간: {kst_cutoff.strftime('%Y-%m-%d %H:%M:%S')})")
            
            # Slack으로 메시지 전송
            self.messenger.send_message(message=message, messenger_type="slack")
            
            # 리포트 전송 상태 업데이트
            self.db.update_daily_profit_report_status(exchange=exchange, reported=True)
            
            self.logger.info(f"일일 리포트 생성 및 전송 완료: {kst_today.strftime('%Y-%m-%d')}")
            
            # 장기 투자 정보 추가
            long_term_trades = list(self.db.long_term_trades.find({
                'exchange': exchange,
                'status': 'active'
            }))
            
            # 장기 투자 상세 정보
            long_term_details = []
            for trade in long_term_trades:
                current_price = self.exchange.get_current_price(trade['market'])
                total_volume = sum(pos['executed_volume'] for pos in trade.get('positions', []))
                current_value = total_volume * current_price
                profit_rate = ((current_value - trade['total_investment']) / trade['total_investment']) * 100
                
                long_term_details.append({
                    'market': trade['market'],
                    'total_investment': trade['total_investment'],
                    'current_value': current_value,
                    'profit_rate': profit_rate,
                    'position_count': len(trade.get('positions', [])),
                    'days_active': (TimeUtils.get_current_kst() - trade['created_at']).days
                })
            
            # 장기 투자 요약 정보
            long_term_summary = {
                'active_count': len(long_term_trades),
                'total_investment': sum(trade.get('total_investment', 0) for trade in long_term_trades),
                'total_current_value': sum(detail['current_value'] for detail in long_term_details),
                'avg_profit_rate': sum(detail['profit_rate'] for detail in long_term_details) / len(long_term_details) if long_term_details else 0
            }
            
            # 메시지에 장기 투자 정보 추가
            message += (
                f"\n\n📊 장기 투자 현황\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 활성 투자: {long_term_summary['active_count']}건\n"
                f"💵 총 투자금: ₩{long_term_summary['total_investment']:,}\n"
                f"📈 평가금액: ₩{long_term_summary['total_current_value']:,}\n"
                f"📊 평균 수익률: {long_term_summary['avg_profit_rate']:+.2f}%\n\n"
                f"📋 상세 현황:\n"
            )
            
            # 수익률 순으로 정렬하여 상세 정보 추가
            sorted_details = sorted(long_term_details, key=lambda x: x['profit_rate'], reverse=True)
            for detail in sorted_details:
                message += (
                    f"• {detail['market']}\n"
                    f"  └ 투자금: ₩{detail['total_investment']:,}\n"
                    f"  └ 평가금: ₩{detail['current_value']:,}\n"
                    f"  └ 수익률: {detail['profit_rate']:+.2f}%\n"
                    f"  └ 포지션: {detail['position_count']}개\n"
                    f"  └ 경과일: {detail['days_active']}일\n\n"
                )
            
            return filename

        except Exception as e:
            self.logger.error(f"일일 리포트 생성 중 오류: {str(e)}")
            # 리포트 전송 실패 시 상태 업데이트
            self.db.update_daily_profit_report_status(exchange=exchange, reported=False)
            raise
        finally:
            # 파일 정리
            if filename and os.path.exists(filename):
                os.remove(filename)

    
    def create_long_term_message(self, trade_data: Dict, conversion_price: float, reason: str) -> str:
        """장기 투자 전환 메시지 생성
        
        장기 투자 전환 시점의 전략 데이터를 기반으로 메시지를 생성합니다.
        """
        strategy_data = trade_data['strategy_data']
        kst_now = TimeUtils.get_current_kst()        
        is_test = self.test_mode

        message = f"{'[TEST MODE] ' if is_test else ''}" + (
            f"------------------------------------------------\n"
            f"거래종목: {trade_data['market']}, 장기 투자 전환\n"
            f" 전환 시간: {TimeUtils.format_kst(kst_now)}\n"
            f" 전환 가격: {conversion_price:,}\n"
            f" 전환 사유: {reason}\n"
        ) + "\n------------------------------------------------"

        self.messenger.send_message(message=message, messenger_type="slack")

    
    def create_buy_message(self, trade_data: Dict, buy_message: str = None) -> str:
        """매수 메시지 생성
        
        매수 시점의 전략 데이터를 기반으로 메시지를 생성합니다.

        Args:
            trade_data: 거래 데이터
        Returns:
            매수 메시지
        """
        strategy_data = trade_data['strategy_data']
        # 구매 경로 확인
        additional_info = None
        if trade_data.get('is_long_term_trade', False):
            long_term_trade = self.db.long_term_trades.find_one({
                'market': trade_data['market'],
                'status': 'active'
            })
            additional_info = (
                f" 장기 투자 횟수: {len(long_term_trade.get('positions', []))}회\n"
                f" 평균 매수가: {long_term_trade.get('average_price', 0):,}원\n"
                f" 이전 매수가: {long_term_trade.get('positions', [])[-1].get('price', 0):,}원\n"
                f" 추가 매수액: {long_term_trade.get('positions', [])[-1].get('investment_amount', 0):,}원\n"
            )

        kst_now = TimeUtils.get_current_kst()

        message = (
            f"------------------------------------------------\n"
            f"거래종목: {trade_data['market']}, 구매\n"
            f" 구매 시간: {TimeUtils.format_kst(kst_now)}\n"
            f" 구매 가격: {trade_data['price']:,}\n"
            f" 구매 신호: {trade_data['signal_strength']:.2f}\n"
            f" Trade-rank: {trade_data.get('thread_id', 'N/A')}\n"
            f" 투자 금액: W{trade_data.get('investment_amount', 0):,}\n"
            f" 거래 사유: {buy_message}\n"
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
            if key not in ['rsi', 'stochastic_k', 'stochastic_d', 'market_rank'] and '_signal' in key:
                strategy_name = key.replace('_signal', '').upper()
                message += f" {strategy_name}: [{value:.1f}]\n"

        message += "\n------------------------------------------------"
        return message

    
    def create_sell_message(self, trade_data: Dict, sell_price: float, buy_price: float,
                           sell_signal: float, fee_amount: float = 0, 
                           total_fees: float = 0, sell_message: str = None) -> str:
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
        kst_now = TimeUtils.get_current_kst()

        message = (
            f"------------------------------------------------\n"
            f"거래종목: {trade_data['market']}, 판매\n"
            f" 판매 시간: {TimeUtils.format_kst(kst_now)}\n"
            f" 구매 가격: {buy_price:,}\n"
            f" 판매 가격: {sell_price:,}\n"
            f" 판매 신호: {sell_signal:.2f}\n"
            f" Trade-rank: {trade_data.get('thread_id', 'N/A')}\n"
            f" 총 투자 금액: W{total_investment:,}\n"
            f" 거래 사유: {sell_message}\n"
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
            if key not in ['rsi', 'stochastic_k', 'stochastic_d', 'market_rank'] and '_signal' in key:
                strategy_name = key.replace('_signal', '').upper()
                message += f" {strategy_name}: [{value:.1f}]\n"

        # 수익률 정보 추가
        profit_rate = ((sell_price - trade_data['price']) / trade_data['price']) * 100
        message += f" 수익률: {profit_rate:.2f}%\n"

        message += (
            f"  └ 매도 수수료: ₩{fee_amount:,.0f}\n"
            f"  └ 총 수수료: ₩{total_fees:,.0f}\n"
            f"  └ 순수익: ₩{(profit_amount - fee_amount):+,.0f} (수수료 차감 후)\n"
        )

        message += "\n------------------------------------------------"
        return message

    
    def generate_hourly_report(self, exchange: str):
        """시간별 리포트 생성
        
        매 시간 정각에 실행되며 현재 보유 포지션과 투자 현황을 보고합니다.
        - 현재 보유 마켓 목록
        - 각 마켓별 매수 시간과 임계값
        - 총 투자금액
        - 장기 투자 현황
        """
        try:
            self.logger.info("시간별 리포트 생성 시작")
            kst_now = TimeUtils.get_current_kst()
            current_time = kst_now.strftime('%Y-%m-%d %H:00')
            message = ""
            
            # 변수 초기화
            total_investment = 0
            total_current_value = 0

            # 활성 거래 조회
            active_trades = list(self.db.get_sync_collection('trades').find({
                'status': 'active'
            }))
            
            # 포트폴리오 정보 조회
            portfolio = self.db.get_sync_collection('portfolio').find_one({'exchange': exchange})
            if not portfolio:
                self.logger.warning("포트폴리오 정보를 찾을 수 없습니다")
                return
            
            # 각 마켓별 상세 정보
            for trade in active_trades:
                # timestamp를 KST로 변환하고 timezone 정보 추가
                trade_time = TimeUtils.ensure_aware(
                    TimeUtils.from_mongo_date(trade['timestamp'])
                )
                if trade_time.tzinfo is None:
                    trade_time = trade_time.replace(tzinfo=timezone(timedelta(hours=9)))  # KST
                
                # 현재 시간도 KST로 통일
                if kst_now.tzinfo is None:
                    kst_now = kst_now.replace(tzinfo=timezone(timedelta(hours=9)))
                
                hold_time = kst_now - trade_time
                hours = hold_time.total_seconds() / 3600
                
                # 현재 가격 조회
                current_price = self.exchange.get_current_price(trade['market'])
                investment_amount = trade.get('investment_amount', 0)
                
                # 수익률 계산
                profit_rate = ((current_price - trade['price']) / trade['price']) * 100
                profit_amount = investment_amount * (profit_rate / 100)
                
                # 총계 계산
                total_investment += investment_amount
                total_current_value += (investment_amount + profit_amount)
                
                market_info = (
                    f"• {trade['market']}\n"
                    f"  └ RANK: {trade['thread_id']:,}\n"
                    f"  └ 매수가: ₩{trade['price']:,}\n"
                    f"  └ 매수원인: {trade['buy_reason']}\n"
                    f"  └ 현재가: ₩{current_price:,}\n"
                    f"  └ 수익률: {profit_rate:+.2f}% (₩{profit_amount:+,.0f})\n"
                    f"  └ 매수시간: {trade['timestamp'].strftime('%Y-%m-%d %H:%M')}"
                    f" ({hours:.1f}시간 전)\n"
                    f"  └ 매수 임계값: {trade['strategy_data'].get('overall_signal', 'N/A')}\n"
                    f"  └ 투자금액: ₩{investment_amount:,}\n"
                )
                message += market_info + "\n"
                time.sleep(0.2)
            
            # 전체 포트폴리오 수익률
            total_profit_rate = ((total_current_value - total_investment) / total_investment * 100) if total_investment > 0 else 0
            total_profit_amount = total_current_value - total_investment
            
            # system_config에서 초기 투자금 가져오기
            system_config = self.db.get_sync_collection('system_config').find_one({})
            initial_investment = system_config.get('initial_investment', 1000000)
            total_max_investment = system_config.get('total_max_investment', 1000000)
            
            # 누적 수익 계산
            total_profit_earned = portfolio.get('profit_earned', 0)
            
            # 현재 수익률 계산 (0으로 나누기 방지)
            total_profit_rate = (total_profit_earned / initial_investment * 100) if initial_investment > 0 else 0

            # 당일 수익률 계산 (0으로 나누기 방지)
            daily_profit_rate = ((total_profit_amount/total_investment)*100) if total_investment > 0 else 0
            
            portfolio_summary = (
                f"📈 포트폴리오 요약\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 초기 투자금: ₩{initial_investment:,}\n"
                f"💰 현재 투자금: ₩{total_max_investment:,}\n"
                f"💵 현재 평가금액: ₩{total_current_value:,.0f}\n"
                f"📊 누적 수익률: {total_profit_rate:+.2f}% (₩{total_profit_earned:+,.0f})\n"
                f"📈 당일 수익률: {daily_profit_rate:+.2f}% (₩{total_profit_amount:+,.0f})\n"
                f"🔢 보유 마켓: {len(active_trades)}개\n"
            )
            
            message = portfolio_summary + "\n" + message + "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            
            # 포트폴리오 정보 추가
            portfolio = self.db.get_portfolio(exchange)
            
            message += (
                f"\n📊 포트폴리오 현황\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 총 투자금액: ₩{portfolio.get('investment_amount', 0):,.0f}\n"
                f"💵 사용 가능 금액: ₩{portfolio.get('available_investment', 0):,.0f}\n"
                f"📈 당일 수익률: {daily_profit_rate:+.2f}% (₩{total_profit_amount:+,.0f})\n"
                f"📊 보유 마켓 누적 수익률: {total_profit_rate:+.2f}% (₩{total_profit_earned:+,.0f})\n"
                f"🔢 보유 마켓: {len(active_trades)}개\n\n"
            )
            
            # 장기 투자 정보 추가
            long_term_trades = list(self.db.long_term_trades.find({
                'exchange': exchange,
                'status': 'active'
            }))
            
            # 장기 투자 상세 정보
            long_term_details = []
            for trade in long_term_trades:
                # created_at에 timezone 정보 추가
                if trade['created_at'].tzinfo is None:
                    trade['created_at'] = trade['created_at'].replace(tzinfo=timezone(timedelta(hours=9)))
                
                current_price = self.exchange.get_current_price(trade['market'])
                total_volume = sum(pos['executed_volume'] for pos in trade.get('positions', []))
                current_value = total_volume * current_price
                profit_rate = ((current_value - trade['total_investment']) / trade['total_investment']) * 100
                
                long_term_details.append({
                    'market': trade['market'],
                    'total_investment': trade['total_investment'],
                    'current_value': current_value,
                    'profit_rate': profit_rate,
                    'position_count': len(trade.get('positions', [])),
                    'days_active': (kst_now - trade['created_at']).days
                })
            
            # 장기 투자 요약 정보
            long_term_summary = {
                'active_count': len(long_term_trades),
                'total_investment': sum(trade.get('total_investment', 0) for trade in long_term_trades),
                'total_current_value': sum(detail['current_value'] for detail in long_term_details),
                'avg_profit_rate': sum(detail['profit_rate'] for detail in long_term_details) / len(long_term_details) if long_term_details else 0
            }
            
            # 메시지에 장기 투자 정보 추가
            message += (
                f"\n📊 장기 투자 현황\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 활성 투자: {long_term_summary['active_count']}건\n"
                f"💵 총 투자금: ₩{long_term_summary['total_investment']:,}\n"
                f"📈 평가금액: ₩{floor(long_term_summary['total_current_value']):,}\n"
                f"📊 평균 수익률: {long_term_summary['avg_profit_rate']:+.2f}%\n\n"
                f"📋 상세 현황:\n"
            )
            
            # 수익률 순으로 정렬하여 상세 정보 추가
            sorted_details = sorted(long_term_details, key=lambda x: x['profit_rate'], reverse=True)
            for detail in sorted_details:
                message += (
                    f"• {detail['market']}\n"
                    f"  └ 투자금: ₩{detail['total_investment']:,}\n"
                    f"  └ 평가금: ₩{floor(detail['current_value']):,}\n"
                    f"  └ 수익률: {detail['profit_rate']:+.2f}%\n"
                    f"  └ 포지션: {detail['position_count']}개\n"
                    f"  └ 경과일: {detail['days_active']}일\n\n"
                )
            
            # Slack으로 메시지 전송
            self.messenger.send_message(message=message, messenger_type="slack")
            
            self.logger.info(f"시간별 리포트 생성 및 전송 완료: {current_time}")
            
        except Exception as e:
            self.logger.error(f"시간별 리포트 생성 중 오류 발생: {str(e)}")
            raise

    
    def update_strategy_data(self, market: str, candles: List[Dict], exchange: str, thread_id: int, price: float, strategy_results: Dict):
        """전략 분석 결과 업데이트
        
        Args:
            market: 마켓 정보
            price: 현재 가격
            strategy_results: 전략 분석 결과
        """
        try:
            # 입력값 로깅
            self.logger.debug(f"{market} - 입력된 전략 결과: {strategy_results}")
            
            # 전략 결과 검증
            if not isinstance(strategy_results, dict):
                self.logger.error(f"{market} - 유효하지 않은 전략 결과 형식: {type(strategy_results)}")
                return
            
            if 'strategy_data' not in strategy_results:
                self.logger.error(f"{market} - strategy_data 없음: {strategy_results}")
                return
            
            # 전략 데이터 구성
            strategy_data = {
                'exchange': exchange, # 거래소 이름
                'market': market, # 마켓 이름
                'current_price': price, # 현재 가격
                'timestamp': TimeUtils.get_current_kst(),  # KST 시간
                'price':  price, # 매수 가격
                'candles': candles, # 캔들 데이터
                'action': strategy_results.get('action', 'hold'), # 매수/매도 여부
                'signal_strength': strategy_results.get('overall_signal', 0), # 전략 신호
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
                    {'market': market}, # 마켓 이름으로 조회
                    {'$set': strategy_data}, # 전략 데이터 업데이트
                    upsert=True # 데이터가 없으면 생성
                )
                
                if result.modified_count > 0 or result.upserted_id:
                    self.logger.debug(f"{market} 전략 데이터 저장/업데이트 성공")
                else:
                    self.logger.warning(f"{market} 전략 데이터 변경 없음")
                
                # 활성 거래 조회 및 업데이트
                active_trades = list(self.db.trades.find({
                    'market': market,     
                    'status': {'$in': ['active', 'converted']}
                }))
                current_price = price
                
                for active_trade in active_trades:
                    # 수익률 계산 시 0으로 나누기 방지
                    base_price = active_trade.get('price', current_price)
                    if base_price <= 0:
                        self.logger.warning(f"{market} - 유효하지 않은 매수가: {base_price}")
                        profit_rate = 0
                    else:
                        profit_rate = ((current_price / base_price) - 1) * 100

                    # 현재 가격 업데이트
                    self.db.trades.update_one(
                        {'_id': active_trade['_id']},
                        {
                            '$set': {
                                'exchange': exchange,
                                'market': market,
                                'current_price': current_price,
                                'thread_id': thread_id,
                                'current_value': current_price * active_trade.get('executed_volume', 0),
                                'signal_strength': strategy_results.get('overall_signal', 0),
                                'profit_rate': profit_rate,
                                'last_updated': TimeUtils.get_current_kst(),
                                'user_call': active_trade.get('user_call', False)
                            }
                        }
                    )
                    
                    self.logger.debug(f"가격 정보 업데이트 완료: {market} - 현재가: {current_price:,}원")

                    # 장기 투자 거래 조회 및 업데이트
                    long_term_trade = self.db.long_term_trades.find_one({
                        'market': market
                    })
                    
                    if long_term_trade:
                        self.db.long_term_trades.update_one(
                            {'_id': long_term_trade['_id']},
                            {'$set': {
                                'status': 'active',
                                'price': current_price,
                                'profit_rate': profit_rate,
                                'last_updated': TimeUtils.get_current_kst()
                            }}
                        )
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
            # 커서를 리스트로 변환하여 반환
            active_trades = list(self.db.trades.find({"status": "active"}))
            return active_trades
        except Exception as e:
            self.logger.error(f"활성 거래 조회 중 오류: {str(e)}")
            return []

    
    def check_investment_limit(self) -> bool:
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
            portfolio = self.db.portfolio.find_one({'exchange': self.exchange_name})
            system_config = self.db.system_config.find_one({'exchange': self.exchange_name})
            total_max_investment = portfolio.get('available_investment', 800000)
            reserve_amount = portfolio.get('reserve_amount', 200000)
            min_trade_amount = system_config.get('min_trade_amount', 5000)
            
            # 현재 스레드의 활성 거래들 조회
            thread_trades = self.db.trades.find({
                'status': {'$in': ['active', 'converted']}
            })
            
            # 스레드별 투자 총액 계산
            thread_investment = sum(trade.get('investment_amount', 0) for trade in thread_trades)
            
            # 스레드별 한도 체크
            if thread_investment >= total_max_investment:
                self.logger.warning(f"전체 투자 한도 초과: {thread_investment:,}원/{total_max_investment:,}원")
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

    
    async def user_call_buy(self, market: str, exchange: str, price: float, immediate: bool = False) -> Dict:
        """사용자 매수 주문
        
        Args:
            market: 마켓명
            price: 주문 가격
            immediate: 즉시 체결 여부
            
        Returns:
            Dict: 주문 결과
        """
        try:
            # 테스트 모드 확인
            is_test = self.test_mode
            self.logger.info(f"매수 주문 시작 - 마켓: {market}, 가격: {price:,}, 즉시체결: {immediate}")
            
            # 전략/시장 데이터 조회
            strategy_data = await self.db.get_collection('strategy_data').find_one({'market': market, 'exchange': exchange})
            if not strategy_data:
                self.logger.warning(f"{market} - 전략 데이터 없음")
                return {'success': False, 'message': '전략 데이터 없음'}
            
            # 주문 데이터 생성
            order_data = {
                'market': market,
                'exchange': exchange,
                'type': 'buy',
                'price': price,
                'status': 'pending',
                'immediate': immediate,
                'created_at': TimeUtils.get_current_kst(),
                'updated_at': TimeUtils.get_current_kst(),
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
                    market=market,
                    exchange=exchange,
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

    
    async def user_call_sell(self, market: str, exchange: str, price: float, immediate: bool = False) -> Dict:
        """사용자 매도 주문
        
        Args:
            market: 마켓명
            price: 주문 가격
            immediate: 즉시 체결 여부
            
        Returns:
            Dict: 주문 결과
        """
        try:
            # 테스트 모드 확인
            is_test = self.test_mode
            self.logger.info(f"매도 주문 시작 - 마켓: {market}, 가격: {price:,}, 즉시체결: {immediate}")
            
            # 활성 거래 확인
            active_trade = await self.db.get_collection('trades').find_one({
                'market': market,
                'exchange': exchange,
                'status': 'active'
            })
            
            if not active_trade:
                return {'success': False, 'message': '해당 마켓의 활성 거래가 없습니다'}
            
            # 주문 데이터 생성
            order_data = {
                'market': market,
                'exchange': exchange,
                'type': 'sell',
                'price': price,
                'status': 'pending',
                'immediate': immediate,
                'created_at': TimeUtils.get_current_kst(),
                'updated_at': TimeUtils.get_current_kst(),
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
                    market=market,
                    exchange=exchange,
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
                    ('market', 1),
                    ('exchange', 1),
                    ('status', 1),
                    ('created_at', -1)
                ])
                self.logger.info("order_list 컬렉션 생성 완료")
        except Exception as e:
            self.logger.error(f"order_list 컬렉션 초기화 중 오류: {str(e)}")
            raise

    
    def initialize_lowest_price(self, exchange: str):
        """최저가 초기화
        
        strategy_data 컬렉션의 모든 마켓에 대해 
        lowest_price와 lowest_signal을 초기화합니다.
        """
        try:
            self.logger.info("최저가 초기화 시작")
            
            # 모든 strategy_data 문서 업데이트
            result = self.db.strategy_data.update_many(
                {'exchange': exchange},  # 모든 문서 선택
                {
                    '$set': {
                        'lowest_price': None,
                        'lowest_signal': 0,
                        'last_updated': TimeUtils.get_current_kst()
                    }
                }
            )
            
            self.logger.info(f"최저가 초기화 완료: {result.modified_count}개 문서 업데이트")
            
        except Exception as e:
            self.logger.error(f"최저가 초기화 중 오류 발생: {str(e)}")
            raise

    def auto_recovery(self):
        """자동 복구 메커니즘"""
        try:
            # 미완료 주문 확인 및 처리
            pending_orders = self.db.get_pending_orders()
            for order in pending_orders:
                status = self.exchange.get_order_status(order['uuid'])
                if status == 'completed':
                    self.db.update_order_status(order['uuid'], status)
                elif status == 'canceled':
                    self.db.cleanup_failed_order(order['uuid'])
                    
            # 거래 상태 정합성 검증
            self.validate_trade_status()
            
        except Exception as e:
            self.logger.error(f"자동 복구 실패: {str(e)}")

    def validate_trade_status(self):
        """거래 상태 정합성 검증"""
        try:
            # 활성 거래 조회
            active_trades = self.db.trades.find({'status': 'active'})
            
            for trade in active_trades:
                # 실제 주문 상태 확인
                order_status = self.exchange.get_order_status(trade.get('order_uuid'))
                
                # 주문이 이미 체결되었는데 거래 상태가 active인 경우
                if order_status == 'completed' and trade['status'] == 'active':
                    self.logger.warning(f"거래 상태 불일치 감지: {trade['market']}")
                    # 거래 상태 업데이트
                    self.db.trades.update_one(
                        {'_id': trade['_id']},
                        {'$set': {'status': 'completed'}}
                    )
                    
        except Exception as e:
            self.logger.error(f"거래 상태 검증 실패: {str(e)}")

    def process_exchange_order(self, exchange: str, order_type: str, market: str, volume: float, price: float) -> Dict:
        """거래소 주문 처리"""
        try:
            # 거래소 설정 조회
            exchange_settings = self.db.get_exchange_settings(exchange)
            if not exchange_settings.get('is_active'):
                raise Exception(f"{exchange} 거래소가 비활성화 상태입니다.")

            # 주문 데이터 생성
            order_data = {
                'exchange': exchange,
                'market': market,
                'type': order_type,
                'volume': volume,
                'price': price,
                'status': 'pending',
                'created_at': TimeUtils.get_current_kst()
            }

            # 테스트 모드 확인
            if exchange_settings.get('test_mode'):
                order_data['test_mode'] = True
                order_data['status'] = 'completed'
                self.logger.info(f"[TEST MODE] 주문 처리: {order_data}")
                return order_data

            # 실제 거래소 API 호출
            exchange_instance = self._get_exchange_instance(exchange)
            order_result = exchange_instance.place_order(
                market=market,
                side=order_type,
                volume=volume,
                price=price
            )

            # 주문 결과 저장
            order_data.update(order_result)
            self.db.orders.insert_one(order_data)

            return order_data

        except Exception as e:
            self.logger.error(f"주문 처리 실패: {str(e)}")
            return {'error': str(e)}

    def _get_exchange_instance(self, exchange_name: str) -> Any:
        """거래소 인스턴스 반환"""
        if not hasattr(self, f'_{exchange_name}_instance'):
            settings = self.db.get_exchange_settings(exchange_name)
            instance = ExchangeFactory.create_exchange(
                exchange_name,
                settings.get('api_key', ''),
                settings.get('secret_key', ''),
                settings.get('test_mode', True)
            )
            setattr(self, f'_{exchange_name}_instance', instance)
        return getattr(self, f'_{exchange_name}_instance')