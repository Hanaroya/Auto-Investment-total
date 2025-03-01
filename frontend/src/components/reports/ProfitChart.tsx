'use client'

import { useEffect, useRef } from 'react'
import {
  Box,
  Text,
  useColorModeValue,
  ButtonGroup,
  Button,
} from '@chakra-ui/react'
import { createChart, ColorType } from 'lightweight-charts'

interface ProfitChartProps {
  period: 'daily' | 'weekly' | 'monthly'
}

export default function ProfitChart({ period }: ProfitChartProps) {
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

      const areaSeries = chart.addAreaSeries({
        lineColor: '#2962FF',
        topColor: 'rgba(41, 98, 255, 0.3)',
        bottomColor: 'rgba(41, 98, 255, 0.0)',
      })

      // 샘플 데이터 (실제로는 API에서 받아와야 함)
      const data = [
        { time: '2023-12-22', value: 0 },
        { time: '2023-12-23', value: 2.5 },
        { time: '2023-12-24', value: 1.8 },
        { time: '2023-12-25', value: 3.2 },
        { time: '2023-12-26', value: 4.5 },
        { time: '2023-12-27', value: 3.8 },
        { time: '2023-12-28', value: 5.2 },
      ]

      areaSeries.setData(data)
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
  }, [period])

  const displayTypes = ['누적 수익률', '일일 수익률', '거래량']

  return (
    <Box
      bg={useColorModeValue('white', 'gray.800')}
      p={6}
      rounded="lg"
      shadow="base"
    >
      <Text fontSize="xl" fontWeight="bold" mb={4}>
        수익 분석
      </Text>
      
      <ButtonGroup size="sm" isAttached variant="outline" mb={4}>
        {displayTypes.map((type) => (
          <Button key={type}>
            {type}
          </Button>
        ))}
      </ButtonGroup>

      <Box ref={chartContainerRef} h="400px" />
    </Box>
  )
} 