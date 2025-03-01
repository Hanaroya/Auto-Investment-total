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
  Badge,
  useColorModeValue,
} from '@chakra-ui/react'

interface TradeHistoryProps {
  period: 'daily' | 'weekly' | 'monthly'
}

export default function TradeHistory({ period }: TradeHistoryProps) {
  // 샘플 데이터 (실제로는 API에서 받아와야 함)
  const trades = [
    {
      id: 1,
      market: 'KRW-BTC',
      type: 'buy',
      price: 52000000,
      amount: 0.1,
      timestamp: '2023-12-28 14:30:00',
      status: 'completed',
      profit: 2.5,
    },
    {
      id: 2,
      market: 'KRW-ETH',
      type: 'sell',
      price: 3100000,
      amount: 1.5,
      timestamp: '2023-12-28 14:15:00',
      status: 'completed',
      profit: -1.2,
    },
    {
      id: 3,
      market: 'KRW-XRP',
      type: 'buy',
      price: 780,
      amount: 1000,
      timestamp: '2023-12-28 14:00:00',
      status: 'completed',
      profit: 3.8,
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
        거래 내역
      </Text>
      
      <Box overflowX="auto">
        <Table variant="simple">
          <Thead>
            <Tr>
              <Th>시간</Th>
              <Th>마켓</Th>
              <Th>유형</Th>
              <Th isNumeric>가격</Th>
              <Th isNumeric>수량</Th>
              <Th isNumeric>수익률</Th>
              <Th>상태</Th>
            </Tr>
          </Thead>
          <Tbody>
            {trades.map((trade) => (
              <Tr key={trade.id}>
                <Td>{trade.timestamp}</Td>
                <Td>{trade.market}</Td>
                <Td>
                  <Badge
                    colorScheme={trade.type === 'buy' ? 'green' : 'red'}
                  >
                    {trade.type === 'buy' ? '매수' : '매도'}
                  </Badge>
                </Td>
                <Td isNumeric>₩{trade.price.toLocaleString()}</Td>
                <Td isNumeric>{trade.amount}</Td>
                <Td isNumeric>
                  <Text
                    color={trade.profit >= 0 ? 'green.500' : 'red.500'}
                  >
                    {trade.profit >= 0 ? '+' : ''}{trade.profit}%
                  </Text>
                </Td>
                <Td>
                  <Badge
                    colorScheme={
                      trade.status === 'completed' ? 'green' :
                      trade.status === 'pending' ? 'yellow' : 'red'
                    }
                  >
                    {
                      trade.status === 'completed' ? '완료' :
                      trade.status === 'pending' ? '대기' : '실패'
                    }
                  </Badge>
                </Td>
              </Tr>
            ))}
          </Tbody>
        </Table>
      </Box>
    </Box>
  )
} 