'use client'

import {
  Box,
  Text,
  VStack,
  HStack,
  Progress,
  useColorModeValue,
} from '@chakra-ui/react'

interface MarketAnalysisProps {
  period: 'daily' | 'weekly' | 'monthly'
}

interface MarketMetric {
  market: string
  profitRate: number
  tradeCount: number
  winRate: number
}

export default function MarketAnalysis({ period }: MarketAnalysisProps) {
  // 샘플 데이터 (실제로는 API에서 받아와야 함)
  const markets: MarketMetric[] = [
    {
      market: 'KRW-BTC',
      profitRate: 5.2,
      tradeCount: 12,
      winRate: 75.0,
    },
    {
      market: 'KRW-ETH',
      profitRate: 3.8,
      tradeCount: 8,
      winRate: 62.5,
    },
    {
      market: 'KRW-XRP',
      profitRate: -1.2,
      tradeCount: 15,
      winRate: 46.7,
    },
    {
      market: 'KRW-SOL',
      profitRate: 7.5,
      tradeCount: 6,
      winRate: 83.3,
    },
  ]

  return (
    <Box
      bg={useColorModeValue('white', 'gray.800')}
      p={6}
      rounded="lg"
      shadow="base"
    >
      <Text fontSize="xl" fontWeight="bold" mb={4}>
        마켓별 분석
      </Text>
      
      <VStack spacing={6} align="stretch">
        {markets.map((market) => (
          <Box key={market.market}>
            <HStack justify="space-between" mb={2}>
              <Text fontWeight="bold">{market.market}</Text>
              <Text
                color={market.profitRate >= 0 ? 'green.500' : 'red.500'}
                fontWeight="semibold"
              >
                {market.profitRate >= 0 ? '+' : ''}{market.profitRate}%
              </Text>
            </HStack>
            
            <Text fontSize="sm" color="gray.500" mb={1}>
              거래 횟수: {market.tradeCount}회
            </Text>
            
            <HStack spacing={4} mb={2}>
              <Text fontSize="sm">승률</Text>
              <Progress
                value={market.winRate}
                colorScheme={market.winRate >= 50 ? 'green' : 'red'}
                size="sm"
                width="100%"
                rounded="full"
              />
              <Text fontSize="sm" whiteSpace="nowrap">
                {market.winRate}%
              </Text>
            </HStack>
          </Box>
        ))}
      </VStack>
    </Box>
  )
} 