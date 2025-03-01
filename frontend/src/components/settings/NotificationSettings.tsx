'use client'

import {
  Box,
  VStack,
  FormControl,
  FormLabel,
  Switch,
  Text,
  Input,
  useColorModeValue,
  Button,
  Select,
  HStack,
} from '@chakra-ui/react'
import { useStore } from '@/store'

export default function NotificationSettings() {
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
        알림 설정
      </Text>
      
      <VStack spacing={6} align="stretch">
        <FormControl display="flex" alignItems="center">
          <FormLabel mb="0">
            알림 활성화
          </FormLabel>
          <Switch
            isChecked={settings.notificationsEnabled}
            onChange={(e) => updateSettings({ notificationsEnabled: e.target.checked })}
            colorScheme="green"
          />
        </FormControl>

        <FormControl>
          <FormLabel>알림 채널</FormLabel>
          <VStack spacing={4} align="stretch">
            <HStack>
              <Switch defaultChecked colorScheme="telegram" />
              <Text>텔레그램</Text>
            </HStack>
            
            <HStack>
              <Switch defaultChecked colorScheme="yellow" />
              <Text>카카오톡</Text>
            </HStack>
            
            <HStack>
              <Switch defaultChecked colorScheme="blue" />
              <Text>이메일</Text>
            </HStack>
          </VStack>
        </FormControl>

        <FormControl>
          <FormLabel>알림 유형</FormLabel>
          <VStack spacing={4} align="stretch">
            <HStack justify="space-between">
              <Text>매수/매도 체결</Text>
              <Select size="sm" width="120px" defaultValue="all">
                <option value="all">모두</option>
                <option value="success">성공만</option>
                <option value="fail">실패만</option>
                <option value="none">끄기</option>
              </Select>
            </HStack>
            
            <HStack justify="space-between">
              <Text>수익률 알림</Text>
              <Select size="sm" width="120px" defaultValue="5">
                <option value="3">±3% 이상</option>
                <option value="5">±5% 이상</option>
                <option value="10">±10% 이상</option>
                <option value="none">끄기</option>
              </Select>
            </HStack>
            
            <HStack justify="space-between">
              <Text>일일 리포트</Text>
              <Select size="sm" width="120px" defaultValue="summary">
                <option value="detail">상세</option>
                <option value="summary">요약</option>
                <option value="none">끄기</option>
              </Select>
            </HStack>
          </VStack>
        </FormControl>

        <FormControl>
          <FormLabel>텔레그램 봇 토큰</FormLabel>
          <Input type="password" placeholder="텔레그램 봇 토큰을 입력하세요" />
        </FormControl>

        <FormControl>
          <FormLabel>텔레그램 채팅 ID</FormLabel>
          <Input type="text" placeholder="텔레그램 채팅 ID를 입력하세요" />
        </FormControl>

        <Button colorScheme="blue" size="lg">
          설정 저장
        </Button>
      </VStack>
    </Box>
  )
} 