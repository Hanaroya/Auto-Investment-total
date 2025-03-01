import { create } from 'zustand'
import { AppStore } from '@/types/store'
import { MarketData, TradeData, PortfolioUpdate } from '@/types/socket'

const initialState = {
  markets: [],
  activeTrades: [],
  portfolio: {
    total_value: 0,
    daily_profit: 0,
    total_profit: 0,
    active_trades_count: 0,
    win_rate: 0,
  },
  settings: {
    autoTrade: false,
    riskLevel: 'medium',
    maxConcurrentTrades: 5,
    notificationsEnabled: true,
    darkMode: false,
  },
  ui: {
    selectedMarket: null,
    timeframe: '15m',
    chartType: 'candles',
    sidebarOpen: true,
  },
  isLoading: false,
  error: null,
}

export const useStore = create<AppStore>((set) => ({
  ...initialState,

  // 마켓 관련 액션
  updateMarket: (market: MarketData) =>
    set((state) => ({
      markets: state.markets.map((m) =>
        m.market === market.market ? market : m
      ),
    })),

  updateMarkets: (markets: MarketData[]) =>
    set(() => ({
      markets,
    })),

  // 거래 관련 액션
  updateTrade: (trade: TradeData) =>
    set((state) => ({
      activeTrades: state.activeTrades.map((t) =>
        t.id === trade.id ? trade : t
      ),
    })),

  addTrade: (trade: TradeData) =>
    set((state) => ({
      activeTrades: [...state.activeTrades, trade],
    })),

  removeTrade: (tradeId: string) =>
    set((state) => ({
      activeTrades: state.activeTrades.filter((t) => t.id !== tradeId),
    })),

  // 포트폴리오 관련 액션
  updatePortfolio: (portfolio: PortfolioUpdate) =>
    set(() => ({
      portfolio,
    })),

  // 설정 관련 액션
  updateSettings: (settings) =>
    set((state) => ({
      settings: {
        ...state.settings,
        ...settings,
      },
    })),

  // UI 관련 액션
  updateUI: (ui) =>
    set((state) => ({
      ui: {
        ...state.ui,
        ...ui,
      },
    })),

  setSelectedMarket: (market) =>
    set((state) => ({
      ui: {
        ...state.ui,
        selectedMarket: market,
      },
    })),

  toggleSidebar: () =>
    set((state) => ({
      ui: {
        ...state.ui,
        sidebarOpen: !state.ui.sidebarOpen,
      },
    })),

  // 상태 관련 액션
  setLoading: (isLoading: boolean) =>
    set(() => ({
      isLoading,
    })),

  setError: (error: string | null) =>
    set(() => ({
      error,
    })),

  resetError: () =>
    set(() => ({
      error: null,
    })),
})) 