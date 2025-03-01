'use client'

import {
  Box,
  SimpleGrid,
  Stat,
  StatLabel,
  StatNumber,
  StatHelpText,
  StatArrow,
  Text,
  useColorModeValue,
} from '@chakra-ui/react'

interface PerformanceMetricsProps {
  period: 'daily' | 'weekly' | 'monthly'
}

export default function PerformanceMetrics({ period }: PerformanceMetricsProps) {
  // 샘플 데이터 (실제로는 API에서 받아와야 함)
  const metrics = [
    {
      label: '총 수익',
      value: '₩1,234,567',
      change: 23.45,
      changeType: 'increase' as const,
    },
    {
      label: '승률',
      value: '68.5%',
      subtext: '총 35건 중 24건',
    },
    {
      label: '평균 수익률',
      value: '2.8%',
      change: 0.5,
      changeType: 'increase' as const,
    },
    {
      label: '최대 손실폭',
      value: '-4.2%',
      subtext: 'KRW-BTC',
    },
    {
      label: '최대 이익폭',
      value: '8.7%',
      subtext: 'KRW-ETH',
    },
    {
      label: '거래 횟수',
      value: '35건',
      change: -2,
      changeType: 'decrease' as const,
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
        성과 지표
      </Text>
      
      <SimpleGrid columns={{ base: 2, md: 3 }} spacing={4}>
        {metrics.map((metric, index) => (
          <Stat key={index}>
            <StatLabel>{metric.label}</StatLabel>
            <StatNumber>{metric.value}</StatNumber>
            {metric.change !== undefined && (
              <StatHelpText>
                <StatArrow type={metric.changeType} />
                {metric.change}
                {typeof metric.change === 'number' && metric.change % 1 === 0 ? '건' : '%'}
              </StatHelpText>
            )}
            {metric.subtext && (
              <StatHelpText>
                {metric.subtext}
              </StatHelpText>
            )}
          </Stat>
        ))}
      </SimpleGrid>
    </Box>
  )
} 