'use client'

import {
  Box,
  VStack,
  FormControl,
  FormLabel,
  Switch,
  Text,
  Select,
  NumberInput,
  NumberInputField,
  NumberInputStepper,
  NumberIncrementStepper,
  NumberDecrementStepper,
  useColorModeValue,
  Button,
} from '@chakra-ui/react'
import { useStore } from '@/store'

export default function TradingSettings() {
  const { settings, updateSettings } = useStore((state) => ({
    settings: state.settings,
    updateSettings: state.updateSettings,
  }))

  return (
    <Box
      bg={useColorModeValue('white', 'gray.800')}
      p={6}
      rounded="lg"
      shadow="base"
    >
      <Text fontSize="xl" fontWeight="bold" mb={6}>
        거래 설정
      </Text>
      
      <VStack spacing={6} align="stretch">
        <FormControl display="flex" alignItems="center">
          <FormLabel mb="0">
            자동 거래 활성화
          </FormLabel>
          <Switch
            isChecked={settings.autoTrade}
            onChange={(e) => updateSettings({ autoTrade: e.target.checked })}
            colorScheme="green"
          />
        </FormControl>

        <FormControl>
          <FormLabel>거래 전략</FormLabel>
          <Select defaultValue="rsi">
            <option value="rsi">RSI 기반 전략</option>
            <option value="macd">MACD 기반 전략</option>
            <option value="bb">볼린저 밴드 전략</option>
            <option value="custom">사용자 정의 전략</option>
          </Select>
        </FormControl>

        <FormControl>
          <FormLabel>최대 동시 거래 수</FormLabel>
          <NumberInput
            defaultValue={settings.maxConcurrentTrades}
            min={1}
            max={10}
            onChange={(_, value) => updateSettings({ maxConcurrentTrades: value })}
          >
            <NumberInputField />
            <NumberInputStepper>
              <NumberIncrementStepper />
              <NumberDecrementStepper />
            </NumberInputStepper>
          </NumberInput>
        </FormControl>

        <FormControl>
          <FormLabel>거래 주기</FormLabel>
          <Select defaultValue="15m">
            <option value="1m">1분</option>
            <option value="5m">5분</option>
            <option value="15m">15분</option>
            <option value="1h">1시간</option>
            <option value="4h">4시간</option>
          </Select>
        </FormControl>

        <FormControl>
          <FormLabel>최소 거래 금액</FormLabel>
          <NumberInput defaultValue={10000} min={5000} step={1000}>
            <NumberInputField />
            <NumberInputStepper>
              <NumberIncrementStepper />
              <NumberDecrementStepper />
            </NumberInputStepper>
          </NumberInput>
        </FormControl>

        <Button colorScheme="blue" size="lg">
          설정 저장
        </Button>
      </VStack>
    </Box>
  )
} 