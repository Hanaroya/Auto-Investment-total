'use client'

import { useEffect, useRef } from 'react'
import {
  Box,
  Flex,
  ButtonGroup,
  Button,
  useColorModeValue,
} from '@chakra-ui/react'
import { createChart, ColorType } from 'lightweight-charts'
import { useStore } from '@/store'

export default function DetailedChart() {
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<any>(null)
  const { selectedMarket, timeframe, chartType, updateUI } = useStore((state) => ({
    selectedMarket: state.ui.selectedMarket,
    timeframe: state.ui.timeframe,
    chartType: state.ui.chartType,
    updateUI: state.updateUI,
  }))

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
        height: 600,
      })

      const series = chartType === 'candles' 
        ? chart.addCandlestickSeries({
            upColor: '#26a69a',
            downColor: '#ef5350',
            borderVisible: false,
            wickUpColor: '#26a69a',
            wickDownColor: '#ef5350',
          })
        : chart.addLineSeries({
            color: '#2962FF',
            lineWidth: 2,
          })

      // 샘플 데이터 (실제로는 WebSocket에서 받아와야 함)
      const data = [
        { time: '2023-12-22', open: 50000, high: 51000, low: 49000, close: 50500 },
        { time: '2023-12-23', open: 50500, high: 52000, low: 50000, close: 51500 },
        { time: '2023-12-24', open: 51500, high: 53000, low: 51000, close: 52500 },
      ]

      series.setData(data)
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
  }, [chartType, selectedMarket, timeframe])

  const timeframes = ['1m', '5m', '15m', '1h', '4h', '1d']
  const chartTypes = ['candles', 'line']

  return (
    <Box
      bg={useColorModeValue('white', 'gray.800')}
      p={6}
      rounded="lg"
      shadow="base"
    >
      <Flex justify="space-between" mb={4}>
        <ButtonGroup size="sm" isAttached variant="outline">
          {timeframes.map((tf) => (
            <Button
              key={tf}
              onClick={() => updateUI({ timeframe: tf as any })}
              colorScheme={timeframe === tf ? 'blue' : undefined}
            >
              {tf}
            </Button>
          ))}
        </ButtonGroup>

        <ButtonGroup size="sm" isAttached variant="outline">
          {chartTypes.map((type) => (
            <Button
              key={type}
              onClick={() => updateUI({ chartType: type as any })}
              colorScheme={chartType === type ? 'blue' : undefined}
            >
              {type === 'candles' ? '캔들' : '라인'}
            </Button>
          ))}
        </ButtonGroup>
      </Flex>

      <Box ref={chartContainerRef} h="600px" />
    </Box>
  )
} 