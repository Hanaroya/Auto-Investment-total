'use client'

import { useEffect, useRef } from 'react'
import { Box, useColorModeValue } from '@chakra-ui/react'
import { createChart, ColorType } from 'lightweight-charts'

export default function TradingChart() {
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<any>(null)

  useEffect(() => {
    if (chartContainerRef.current) {
      const chart = createChart(chartContainerRef.current, {
        layout: {
          background: { type: ColorType.Solid, color: 'transparent' },
          textColor: useColorModeValue('#191919', '#FFFFFF'),
        },
        grid: {
          vertLines: { color: useColorModeValue('#E1E1E1', '#363A45') },
          horzLines: { color: useColorModeValue('#E1E1E1', '#363A45') },
        },
        width: chartContainerRef.current.clientWidth,
        height: 400,
      })

      const candlestickSeries = chart.addCandlestickSeries({
        upColor: '#26a69a',
        downColor: '#ef5350',
        borderVisible: false,
        wickUpColor: '#26a69a',
        wickDownColor: '#ef5350',
      })

      // 샘플 데이터
      const data = [
        { time: '2023-12-22', open: 50000, high: 51000, low: 49000, close: 50500 },
        { time: '2023-12-23', open: 50500, high: 52000, low: 50000, close: 51500 },
        { time: '2023-12-24', open: 51500, high: 53000, low: 51000, close: 52500 },
        // ... 더 많은 데이터 포인트 추가 가능
      ]

      candlestickSeries.setData(data)
      chartRef.current = chart

      const handleResize = () => {
        if (chartContainerRef.current) {
          chart.applyOptions({
            width: chartContainerRef.current.clientWidth,
          })
        }
      }

      window.addEventListener('resize', handleResize)

      return () => {
        window.removeEventListener('resize', handleResize)
        chart.remove()
      }
    }
  }, [])

  return (
    <Box
      ref={chartContainerRef}
      bg={useColorModeValue('white', 'gray.800')}
      p={6}
      rounded="lg"
      shadow="base"
      h="500px"
    />
  )
} 