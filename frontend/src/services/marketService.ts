import axios from 'axios';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export interface MarketData {
  market: string;
  timestamp: string;
  price: number;
  volume: number;
  change: number;
  high: number;
  low: number;
}

export interface CandleData {
  market: string;
  timestamp: string;
  interval: string;
  open_price: number;
  high_price: number;
  low_price: number;
  close_price: number;
  volume: number;
}

export interface DailyProfit {
  date: string;
  total_profit: number;
  trade_count: number;
  win_rate: number;
  markets: Record<string, number>;
  details?: Record<string, any>;
}

export interface TradeSignal {
  market: string;
  exchange: string;
  thread_id: string;
  signal_strength: number;
  price: number;
  strategy_data: Record<string, any>;
}

class MarketService {
  async getMarketData(market: string): Promise<MarketData> {
    try {
      const response = await axios.get(`${API_BASE_URL}/api/market/data/${market}`);
      return response.data;
    } catch (error) {
      console.error('마켓 데이터 조회 실패:', error);
      throw error;
    }
  }

  async getCandleData(market: string, interval: string = '240', limit: number = 100): Promise<CandleData[]> {
    try {
      const response = await axios.get(`${API_BASE_URL}/api/market/candles/${market}`, {
        params: { interval, limit }
      });
      return response.data;
    } catch (error) {
      console.error('캔들 데이터 조회 실패:', error);
      throw error;
    }
  }

  async getDailyProfit(days: number = 7): Promise<DailyProfit[]> {
    try {
      const response = await axios.get(`${API_BASE_URL}/api/market/daily-profit`, {
        params: { days }
      });
      return response.data;
    } catch (error) {
      console.error('일일 수익 데이터 조회 실패:', error);
      throw error;
    }
  }

  async processBuySignal(signal: TradeSignal): Promise<boolean> {
    try {
      const response = await axios.post(`${API_BASE_URL}/api/market/trade/buy`, signal);
      return response.data.success;
    } catch (error) {
      console.error('매수 신호 처리 실패:', error);
      throw error;
    }
  }

  async processSellSignal(signal: TradeSignal): Promise<boolean> {
    try {
      const response = await axios.post(`${API_BASE_URL}/api/market/trade/sell`, signal);
      return response.data.success;
    } catch (error) {
      console.error('매도 신호 처리 실패:', error);
      throw error;
    }
  }
}

export const marketService = new MarketService(); 