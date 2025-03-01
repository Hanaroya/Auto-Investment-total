import { useEffect, useCallback } from 'react';
import { socketService } from '@/services/socket';
import { MarketData, TradeData, PortfolioUpdate } from '@/types/socket';

interface UseSocketOptions<T> {
  type: string;
  onMessage: (data: T) => void;
}

export function useSocket<T>({ type, onMessage }: UseSocketOptions<T>) {
  useEffect(() => {
    // 웹소켓 연결이 없으면 연결
    if (!socketService.isConnected()) {
      socketService.connect();
    }

    // 메시지 구독
    const unsubscribe = socketService.subscribe<T>(type, onMessage);

    // 컴포넌트 언마운트 시 구독 해제
    return () => {
      unsubscribe();
    };
  }, [type, onMessage]);

  // 메시지 전송 함수
  const sendMessage = useCallback((event: string, data: any) => {
    socketService.emit(event, data);
  }, []);

  return { sendMessage };
}

// 특정 데이터 타입을 위한 커스텀 훅들
export function useMarketData(onMessage: (data: MarketData) => void) {
  return useSocket<MarketData>({
    type: 'market_update',
    onMessage,
  });
}

export function useTradeData(onMessage: (data: TradeData) => void) {
  return useSocket<TradeData>({
    type: 'trade_update',
    onMessage,
  });
}

export function usePortfolioData(onMessage: (data: PortfolioUpdate) => void) {
  return useSocket<PortfolioUpdate>({
    type: 'portfolio_update',
    onMessage,
  });
} 