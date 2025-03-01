'use client'

import {
  Box,
  SimpleGrid,
  Text,
  Flex,
  useColorModeValue,
  Progress,
} from '@chakra-ui/react'
import { useMarketData } from '@/hooks/useSocket'
import { useStore } from '@/store'
import { MarketData } from '@/types/socket'

export default function MarketOverview() {
  const { markets, updateMarket } = useStore((state) => ({
    markets: state.markets,
    updateMarket: state.updateMarket,
  }))

  useMarketData((data: MarketData) => {
    updateMarket(data)
  })

  const formatVolume = (volume: number) => {
    if (volume >= 1e12) return `${(volume / 1e12).toFixed(1)}조`
    if (volume >= 1e8) return `${(volume / 1e8).toFixed(1)}억`
    if (volume >= 1e4) return `${(volume / 1e4).toFixed(1)}만`
    return volume.toString()
  }

  return (
    <Box
      bg={useColorModeValue('white', 'gray.800')}
      p={6}
      rounded="lg"
      shadow="base"
    >
      <Text fontSize="xl" fontWeight="bold" mb={4}>
        시장 개요
      </Text>
      <SimpleGrid columns={{ base: 1, md: 2, lg: 4 }} spacing={6}>
        {markets.map((market) => (
          <Box
            key={market.market}
            p={4}
            bg={useColorModeValue('gray.50', 'gray.700')}
            rounded="md"
            cursor="pointer"
            onClick={() => useStore.getState().setSelectedMarket(market.market)}
            _hover={{
              transform: 'translateY(-2px)',
              transition: 'transform 0.2s',
            }}
          >
            <Flex justify="space-between" align="center" mb={2}>
              <Text fontWeight="bold">{market.market}</Text>
              <Text
                color={market.change_rate >= 0 ? 'green.500' : 'red.500'}
                fontWeight="semibold"
              >
                {market.change_rate >= 0 ? '+' : ''}{market.change_rate.toFixed(2)}%
              </Text>
            </Flex>
            <Text fontSize="lg" fontWeight="semibold" mb={2}>
              ₩{market.current_price.toLocaleString()}
            </Text>
            <Text fontSize="sm" color="gray.500" mb={2}>
              거래량: {formatVolume(market.acc_trade_volume_24h)}
            </Text>
            <Text fontSize="sm" mb={1}>매수 신호 강도</Text>
            <Progress
              value={market.signal_strength * 100}
              colorScheme={
                market.signal_strength >= 0.7 ? 'green' :
                market.signal_strength <= 0.3 ? 'red' : 'yellow'
              }
              size="sm"
              rounded="full"
            />
          </Box>
        ))}
      </SimpleGrid>
    </Box>
  )
} 