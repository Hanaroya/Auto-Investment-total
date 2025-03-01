import { MarketData, TradeData, PortfolioUpdate } from './socket'

export interface Settings {
  autoTrade: boolean
  riskLevel: 'low' | 'medium' | 'high'
  maxConcurrentTrades: number
  notificationsEnabled: boolean
  darkMode: boolean
}

export interface UIState {
  selectedMarket: string | null
  timeframe: '1m' | '5m' | '15m' | '1h' | '4h' | '1d'
  chartType: 'candles' | 'line'
  sidebarOpen: boolean
}

export interface AppState {
  markets: MarketData[]
  activeTrades: TradeData[]
  portfolio: PortfolioUpdate
  settings: Settings
  ui: UIState
  isLoading: boolean
  error: string | null
}

export interface AppStore extends AppState {
  // 마켓 관련 액션
  updateMarket: (market: MarketData) => void
  updateMarkets: (markets: MarketData[]) => void
  
  // 거래 관련 액션
  updateTrade: (trade: TradeData) => void
  addTrade: (trade: TradeData) => void
  removeTrade: (tradeId: string) => void
  
  // 포트폴리오 관련 액션
  updatePortfolio: (portfolio: PortfolioUpdate) => void
  
  // 설정 관련 액션
  updateSettings: (settings: Partial<Settings>) => void
  
  // UI 관련 액션
  updateUI: (ui: Partial<UIState>) => void
  setSelectedMarket: (market: string | null) => void
  toggleSidebar: () => void
  
  // 상태 관련 액션
  setLoading: (isLoading: boolean) => void
  setError: (error: string | null) => void
  resetError: () => void
} 