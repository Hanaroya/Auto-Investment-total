'use client'

import {
  Box,
  VStack,
  FormControl,
  FormLabel,
  Text,
  useColorModeValue,
  Button,
  Select,
  Slider,
  SliderTrack,
  SliderFilledTrack,
  SliderThumb,
  NumberInput,
  NumberInputField,
  NumberInputStepper,
  NumberIncrementStepper,
  NumberDecrementStepper,
  HStack,
} from '@chakra-ui/react'
import { useStore } from '@/store'
import { useState } from 'react'

export default function RiskManagement() {
  const { settings, updateSettings } = useStore((state) => ({
    settings: state.settings,
    updateSettings: state.updateSettings,
  }))

  const [stopLoss, setStopLoss] = useState(5)
  const [takeProfit, setTakeProfit] = useState(10)

  return (
    <Box
      bg={useColorModeValue('white', 'gray.800')}
      p={6}
      rounded="lg"
      shadow="base"
    >
      <Text fontSize="xl" fontWeight="bold" mb={6}>
        리스크 관리
      </Text>
      
      <VStack spacing={6} align="stretch">
        <FormControl>
          <FormLabel>리스크 레벨</FormLabel>
          <Select
            value={settings.riskLevel}
            onChange={(e) => updateSettings({ riskLevel: e.target.value as any })}
          >
            <option value="low">낮음 (보수적)</option>
            <option value="medium">중간 (균형)</option>
            <option value="high">높음 (공격적)</option>
          </Select>
        </FormControl>

        <FormControl>
          <FormLabel>손절 비율 (%)</FormLabel>
          <HStack spacing={4}>
            <Slider
              flex="1"
              value={stopLoss}
              onChange={setStopLoss}
              min={1}
              max={20}
            >
              <SliderTrack>
                <SliderFilledTrack />
              </SliderTrack>
              <SliderThumb />
            </Slider>
            <NumberInput
              maxW="100px"
              value={stopLoss}
              onChange={(_, value) => setStopLoss(value)}
              min={1}
              max={20}
            >
              <NumberInputField />
              <NumberInputStepper>
                <NumberIncrementStepper />
                <NumberDecrementStepper />
              </NumberInputStepper>
            </NumberInput>
          </HStack>
        </FormControl>

        <FormControl>
          <FormLabel>익절 비율 (%)</FormLabel>
          <HStack spacing={4}>
            <Slider
              flex="1"
              value={takeProfit}
              onChange={setTakeProfit}
              min={1}
              max={50}
            >
              <SliderTrack>
                <SliderFilledTrack />
              </SliderTrack>
              <SliderThumb />
            </Slider>
            <NumberInput
              maxW="100px"
              value={takeProfit}
              onChange={(_, value) => setTakeProfit(value)}
              min={1}
              max={50}
            >
              <NumberInputField />
              <NumberInputStepper>
                <NumberIncrementStepper />
                <NumberDecrementStepper />
              </NumberInputStepper>
            </NumberInput>
          </HStack>
        </FormControl>

        <FormControl>
          <FormLabel>최대 투자 금액</FormLabel>
          <NumberInput defaultValue={1000000} min={100000} step={100000}>
            <NumberInputField />
            <NumberInputStepper>
              <NumberIncrementStepper />
              <NumberDecrementStepper />
            </NumberInputStepper>
          </NumberInput>
        </FormControl>

        <FormControl>
          <FormLabel>일일 최대 거래 횟수</FormLabel>
          <NumberInput defaultValue={10} min={1} max={50}>
            <NumberInputField />
            <NumberInputStepper>
              <NumberIncrementStepper />
              <NumberDecrementStepper />
            </NumberInputStepper>
          </NumberInput>
        </FormControl>

        <FormControl>
          <FormLabel>긴급 중지 조건</FormLabel>
          <Select defaultValue="loss">
            <option value="loss">일일 손실 5% 이상</option>
            <option value="consecutive">연속 3회 손실</option>
            <option value="market">시장 급락 10% 이상</option>
            <option value="custom">사용자 정의</option>
          </Select>
        </FormControl>

        <Button colorScheme="blue" size="lg">
          설정 저장
        </Button>
      </VStack>
    </Box>
  )
} 