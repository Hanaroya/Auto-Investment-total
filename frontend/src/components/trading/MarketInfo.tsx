'use client'

import {
  Box,
  Grid,
  GridItem,
  Text,
  Stat,
  StatLabel,
  StatNumber,
  StatHelpText,
  StatArrow,
  useColorModeValue,
} from '@chakra-ui/react'
import { useStore } from '@/store'

export default function MarketInfo() {
  const { selectedMarket, markets } = useStore((state) => ({
    selectedMarket: state.ui.selectedMarket,
    markets: state.markets,
  }))

  const marketData = markets.find(m => m.market === selectedMarket)

  if (!marketData) {
    return (
      <Box
        bg={useColorModeValue('white', 'gray.800')}
        p={6}
        rounded="lg"
        shadow="base"
      >
        <Text>마켓을 선택해주세요.</Text>
      </Box>
    )
  }

  const stats = [
    {
      label: '24시간 거래량',
      value: `₩${marketData.acc_trade_volume_24h.toLocaleString()}`,
      subtext: '최근 24시간',
    },
    {
      label: '24시간 변동률',
      value: `${marketData.change_rate.toFixed(2)}%`,
      type: marketData.change_rate >= 0 ? 'increase' : 'decrease',
    },
    {
      label: '신호 강도',
      value: `${(marketData.signal_strength * 100).toFixed(1)}%`,
      subtext: marketData.signal_strength >= 0.7 ? '강력 매수' : 
              marketData.signal_strength <= 0.3 ? '강력 매도' : '중립',
    },
  ]

  return (
    <Box
      bg={useColorModeValue('white', 'gray.800')}
      p={6}
      rounded="lg"
      shadow="base"
    >
      <Grid templateColumns="repeat(3, 1fr)" gap={6}>
        {stats.map((stat, index) => (
          <GridItem key={index}>
            <Stat>
              <StatLabel>{stat.label}</StatLabel>
              <StatNumber>{stat.value}</StatNumber>
              {stat.type && (
                <StatHelpText>
                  <StatArrow type={stat.type} />
                  {stat.subtext}
                </StatHelpText>
              )}
              {!stat.type && stat.subtext && (
                <StatHelpText>
                  {stat.subtext}
                </StatHelpText>
              )}
            </Stat>
          </GridItem>
        ))}
      </Grid>
    </Box>
  )
} 