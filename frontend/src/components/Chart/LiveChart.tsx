import React, { useEffect, useRef } from 'react';
import { createChart, IChartApi, ISeriesApi } from 'lightweight-charts';

interface LiveChartProps {
  market: string;
  interval: string;
}

const LiveChart: React.FC<LiveChartProps> = ({ market, interval }) => {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candlestickSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    // 차트 초기화
    if (chartContainerRef.current) {
      chartRef.current = createChart(chartContainerRef.current, {
        width: 800,
        height: 400,
        layout: {
          backgroundColor: '#ffffff',
          textColor: '#333',
        },
        grid: {
          vertLines: { color: '#f0f0f0' },
          horzLines: { color: '#f0f0f0' },
        },
        timeScale: {
          timeVisible: true,
          secondsVisible: false,
        },
      });

      // 캔들스틱 시리즈 추가
      candlestickSeriesRef.current = chartRef.current.addCandlestickSeries({
        upColor: '#26a69a',
        downColor: '#ef5350',
        borderVisible: false,
        wickUpColor: '#26a69a',
        wickDownColor: '#ef5350',
      });

      // 기술적 지표 추가
      const rsiSeries = chartRef.current.addLineSeries({
        color: 'purple',
        lineWidth: 1,
        priceFormat: { type: 'custom', minMove: 0.01, formatter: (price: any) => `${price.toFixed(2)}` },
        overlay: true,
        scaleMargins: { top: 0.8, bottom: 0 },
      });

      // WebSocket 연결
      wsRef.current = new WebSocket(`ws://your-api/ws/chart/${market}`);
      wsRef.current.onmessage = (event) => {
        const data = JSON.parse(event.data);
        
        // 캔들스틱 데이터 업데이트
        candlestickSeriesRef.current?.update({
          time: data.timestamp,
          open: data.open,
          high: data.high,
          low: data.low,
          close: data.close,
        });

        // RSI 데이터 업데이트
        rsiSeries.update({
          time: data.timestamp,
          value: data.rsi,
        });
      };
    }

    // 컴포넌트 언마운트 시 정리
    return () => {
      if (wsRef.current) {
        wsRef.current.close();
      }
      if (chartRef.current) {
        chartRef.current.remove();
      }
    };
  }, [market, interval]);

  return (
    <div className="chart-container">
      <div ref={chartContainerRef} />
      <div className="chart-controls">
        <select onChange={(e) => setInterval(e.target.value)}>
          <option value="1m">1분</option>
          <option value="5m">5분</option>
          <option value="15m">15분</option>
          <option value="1h">1시간</option>
          <option value="4h">4시간</option>
          <option value="1d">1일</option>
        </select>
        <button onClick={toggleIndicators}>
          지표 보기/숨기기
        </button>
      </div>
    </div>
  );
}; 