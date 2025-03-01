'use client'

import {
  Box,
  Stat,
  StatLabel,
  StatNumber,
  StatHelpText,
  StatArrow,
  SimpleGrid,
  useColorModeValue,
} from '@chakra-ui/react'
import { usePortfolioData } from '@/hooks/useSocket'
import { useStore } from '@/store'

export default function PortfolioSummary() {
  const { portfolio, updatePortfolio } = useStore((state) => ({
    portfolio: state.portfolio,
    updatePortfolio: state.updatePortfolio,
  }))

  usePortfolioData((data) => {
    updatePortfolio(data)
  })

  return (
    <Box
      bg={useColorModeValue('white', 'gray.800')}
      p={6}
      rounded="lg"
      shadow="base"
    >
      <SimpleGrid columns={{ base: 1, md: 4 }} spacing={6}>
        <Stat>
          <StatLabel>총 자산</StatLabel>
          <StatNumber>₩{portfolio.total_value.toLocaleString()}</StatNumber>
          <StatHelpText>
            <StatArrow type={portfolio.total_profit >= 0 ? 'increase' : 'decrease'} />
            {portfolio.total_profit.toFixed(2)}%
          </StatHelpText>
        </Stat>

        <Stat>
          <StatLabel>일일 수익</StatLabel>
          <StatNumber>₩{portfolio.daily_profit.toLocaleString()}</StatNumber>
          <StatHelpText>
            <StatArrow type={portfolio.daily_profit >= 0 ? 'increase' : 'decrease'} />
            {(portfolio.daily_profit / portfolio.total_value * 100).toFixed(2)}%
          </StatHelpText>
        </Stat>

        <Stat>
          <StatLabel>활성 거래</StatLabel>
          <StatNumber>{portfolio.active_trades_count}</StatNumber>
          <StatHelpText>
            실시간 업데이트
          </StatHelpText>
        </Stat>

        <Stat>
          <StatLabel>승률</StatLabel>
          <StatNumber>{portfolio.win_rate.toFixed(1)}%</StatNumber>
          <StatHelpText>
            최근 100건 기준
          </StatHelpText>
        </Stat>
      </SimpleGrid>
    </Box>
  )
} 