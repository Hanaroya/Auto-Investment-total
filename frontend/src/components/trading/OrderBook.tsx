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
} from '@chakra-ui/react'
import { useStore } from '@/store'

interface OrderBookEntry {
  price: number
  quantity: number
  total: number
}

export default function OrderBook() {
  // 실제로는 WebSocket으로 데이터를 받아와야 함
  const asks: OrderBookEntry[] = [
    { price: 52000000, quantity: 0.5, total: 26000000 },
    { price: 51900000, quantity: 0.8, total: 41520000 },
    { price: 51800000, quantity: 1.2, total: 62160000 },
  ]

  const bids: OrderBookEntry[] = [
    { price: 51700000, quantity: 1.5, total: 77550000 },
    { price: 51600000, quantity: 2.0, total: 103200000 },
    { price: 51500000, quantity: 1.8, total: 92700000 },
  ]

  return (
    <Box
      bg={useColorModeValue('white', 'gray.800')}
      p={4}
      rounded="lg"
      shadow="base"
    >
      <Text fontSize="lg" fontWeight="bold" mb={4}>
        호가창
      </Text>
      
      <Table variant="simple" size="sm">
        <Thead>
          <Tr>
            <Th>가격</Th>
            <Th isNumeric>수량</Th>
            <Th isNumeric>총액</Th>
          </Tr>
        </Thead>
        <Tbody>
          {asks.reverse().map((ask, i) => (
            <Tr key={`ask-${i}`}>
              <Td>
                <Text color="red.500">
                  ₩{ask.price.toLocaleString()}
                </Text>
              </Td>
              <Td isNumeric>{ask.quantity.toFixed(4)}</Td>
              <Td isNumeric>₩{ask.total.toLocaleString()}</Td>
            </Tr>
          ))}
          
          <Tr>
            <Td colSpan={3} textAlign="center" py={2}>
              <Text fontSize="lg" fontWeight="bold">
                ₩51,800,000
              </Text>
            </Td>
          </Tr>
          
          {bids.map((bid, i) => (
            <Tr key={`bid-${i}`}>
              <Td>
                <Text color="green.500">
                  ₩{bid.price.toLocaleString()}
                </Text>
              </Td>
              <Td isNumeric>{bid.quantity.toFixed(4)}</Td>
              <Td isNumeric>₩{bid.total.toLocaleString()}</Td>
            </Tr>
          ))}
        </Tbody>
      </Table>
    </Box>
  )
} 