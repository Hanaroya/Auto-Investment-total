'use client'

import { useState } from 'react'
import {
  Box,
  Tabs,
  TabList,
  TabPanels,
  Tab,
  TabPanel,
  FormControl,
  FormLabel,
  Input,
  Button,
  VStack,
  Text,
  useColorModeValue,
  NumberInput,
  NumberInputField,
  NumberInputStepper,
  NumberIncrementStepper,
  NumberDecrementStepper,
} from '@chakra-ui/react'
import { useStore } from '@/store'

export default function TradeForm() {
  const [amount, setAmount] = useState('')
  const [price, setPrice] = useState('')
  const { selectedMarket } = useStore((state) => ({
    selectedMarket: state.ui.selectedMarket,
  }))

  const handleSubmit = (type: 'buy' | 'sell') => {
    // 주문 처리 로직 구현 필요
    console.log(`${type} order:`, { amount, price })
  }

  const calculateTotal = () => {
    const amountNum = parseFloat(amount) || 0
    const priceNum = parseFloat(price) || 0
    return (amountNum * priceNum).toLocaleString()
  }

  return (
    <Box
      bg={useColorModeValue('white', 'gray.800')}
      p={4}
      rounded="lg"
      shadow="base"
    >
      <Tabs isFitted variant="enclosed">
        <TabList mb="1em">
          <Tab color="green.500">매수</Tab>
          <Tab color="red.500">매도</Tab>
        </TabList>
        <TabPanels>
          <TabPanel>
            <VStack spacing={4}>
              <FormControl>
                <FormLabel>주문 가격</FormLabel>
                <NumberInput min={0}>
                  <NumberInputField
                    value={price}
                    onChange={(e) => setPrice(e.target.value)}
                    placeholder="KRW"
                  />
                  <NumberInputStepper>
                    <NumberIncrementStepper />
                    <NumberDecrementStepper />
                  </NumberInputStepper>
                </NumberInput>
              </FormControl>

              <FormControl>
                <FormLabel>주문 수량</FormLabel>
                <NumberInput min={0}>
                  <NumberInputField
                    value={amount}
                    onChange={(e) => setAmount(e.target.value)}
                    placeholder="BTC"
                  />
                  <NumberInputStepper>
                    <NumberIncrementStepper />
                    <NumberDecrementStepper />
                  </NumberInputStepper>
                </NumberInput>
              </FormControl>

              <Box w="100%" pt={4}>
                <Text mb={2}>
                  총 주문액: ₩{calculateTotal()}
                </Text>
                <Button
                  colorScheme="green"
                  w="100%"
                  onClick={() => handleSubmit('buy')}
                >
                  매수
                </Button>
              </Box>
            </VStack>
          </TabPanel>

          <TabPanel>
            <VStack spacing={4}>
              <FormControl>
                <FormLabel>주문 가격</FormLabel>
                <NumberInput min={0}>
                  <NumberInputField
                    value={price}
                    onChange={(e) => setPrice(e.target.value)}
                    placeholder="KRW"
                  />
                  <NumberInputStepper>
                    <NumberIncrementStepper />
                    <NumberDecrementStepper />
                  </NumberInputStepper>
                </NumberInput>
              </FormControl>

              <FormControl>
                <FormLabel>주문 수량</FormLabel>
                <NumberInput min={0}>
                  <NumberInputField
                    value={amount}
                    onChange={(e) => setAmount(e.target.value)}
                    placeholder="BTC"
                  />
                  <NumberInputStepper>
                    <NumberIncrementStepper />
                    <NumberDecrementStepper />
                  </NumberInputStepper>
                </NumberInput>
              </FormControl>

              <Box w="100%" pt={4}>
                <Text mb={2}>
                  총 주문액: ₩{calculateTotal()}
                </Text>
                <Button
                  colorScheme="red"
                  w="100%"
                  onClick={() => handleSubmit('sell')}
                >
                  매도
                </Button>
              </Box>
            </VStack>
          </TabPanel>
        </TabPanels>
      </Tabs>
    </Box>
  )
} 