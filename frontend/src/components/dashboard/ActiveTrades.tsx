'use client'

import {
  Box,
  Table,
  Thead,
  Tbody,
  Tr,
  Th,
  Td,
  Text,
  useColorModeValue,
  Badge,
} from '@chakra-ui/react'
import { useTradeData } from '@/hooks/useSocket'
import { useStore } from '@/store'
import { TradeData } from '@/types/socket'

export default function ActiveTrades() {
  const { activeTrades, updateTrade, addTrade, removeTrade } = useStore((state) => ({
    activeTrades: state.activeTrades,
    updateTrade: state.updateTrade,
    addTrade: state.addTrade,
    removeTrade: state.removeTrade,
  }))

  useTradeData((data: TradeData) => {
    if (data.status === 'completed' || data.status === 'failed') {
      removeTrade(data.id)
    } else if (activeTrades.some(trade => trade.id === data.id)) {
      updateTrade(data)
    } else {
      addTrade(data)
    }
  })

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'buying':
        return 'blue'
      case 'selling':
        return 'red'
      case 'holding':
        return 'green'
      default:
        return 'gray'
    }
  }

  const getStatusText = (status: string) => {
    switch (status) {
      case 'buying':
        return '매수 중'
      case 'selling':
        return '매도 중'
      case 'holding':
        return '보유 중'
      default:
        return '대기 중'
    }
  }

  return (
    <Box
      bg={useColorModeValue('white', 'gray.800')}
      p={6}
      rounded="lg"
      shadow="base"
      overflowX="auto"
    >
      <Text fontSize="xl" fontWeight="bold" mb={4}>
        활성 거래 ({activeTrades.length})
      </Text>
      <Table variant="simple">
        <Thead>
          <Tr>
            <Th>마켓</Th>
            <Th isNumeric>매수가</Th>
            <Th isNumeric>현재가</Th>
            <Th isNumeric>수익률</Th>
            <Th>상태</Th>
          </Tr>
        </Thead>
        <Tbody>
          {activeTrades.map((trade) => (
            <Tr key={trade.id}>
              <Td>{trade.market}</Td>
              <Td isNumeric>₩{trade.price.toLocaleString()}</Td>
              <Td isNumeric>₩{(trade.price * (1 + (trade.type === 'buy' ? 0.01 : -0.01))).toLocaleString()}</Td>
              <Td isNumeric>
                <Text
                  color={trade.type === 'buy' ? 'green.500' : 'red.500'}
                >
                  {trade.type === 'buy' ? '+' : '-'}1.00%
                </Text>
              </Td>
              <Td>
                <Badge colorScheme={getStatusColor(trade.status)}>
                  {getStatusText(trade.status)}
                </Badge>
              </Td>
            </Tr>
          ))}
        </Tbody>
      </Table>
    </Box>
  )
} 