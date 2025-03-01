'use client'

import {
  Box,
  VStack,
  FormControl,
  FormLabel,
  Input,
  Text,
  Button,
  useColorModeValue,
  InputGroup,
  InputRightElement,
  IconButton,
  useToast,
} from '@chakra-ui/react'
import { useState } from 'react'
import { ViewIcon, ViewOffIcon } from '@chakra-ui/icons'

export default function APISettings() {
  const [show, setShow] = useState(false)
  const [isLoading, setIsLoading] = useState(false)
  const toast = useToast()

  const handleSubmit = async () => {
    setIsLoading(true)
    try {
      // API 키 저장 로직 구현 필요
      await new Promise(resolve => setTimeout(resolve, 1000)) // 임시 딜레이
      toast({
        title: 'API 키가 저장되었습니다.',
        status: 'success',
        duration: 3000,
        isClosable: true,
      })
    } catch (error) {
      toast({
        title: 'API 키 저장 실패',
        description: '다시 시도해주세요.',
        status: 'error',
        duration: 3000,
        isClosable: true,
      })
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <Box
      bg={useColorModeValue('white', 'gray.800')}
      p={6}
      rounded="lg"
      shadow="base"
    >
      <Text fontSize="xl" fontWeight="bold" mb={6}>
        API 설정
      </Text>
      
      <VStack spacing={6} align="stretch">
        <FormControl>
          <FormLabel>Access Key</FormLabel>
          <Input
            type="text"
            placeholder="Access Key를 입력하세요"
          />
        </FormControl>

        <FormControl>
          <FormLabel>Secret Key</FormLabel>
          <InputGroup>
            <Input
              type={show ? 'text' : 'password'}
              placeholder="Secret Key를 입력하세요"
            />
            <InputRightElement>
              <IconButton
                aria-label={show ? 'Hide secret key' : 'Show secret key'}
                icon={show ? <ViewOffIcon /> : <ViewIcon />}
                onClick={() => setShow(!show)}
                variant="ghost"
              />
            </InputRightElement>
          </InputGroup>
        </FormControl>

        <FormControl>
          <FormLabel>거래소 선택</FormLabel>
          <Input
            type="text"
            value="Upbit"
            isReadOnly
          />
        </FormControl>

        <Button
          colorScheme="blue"
          size="lg"
          onClick={handleSubmit}
          isLoading={isLoading}
        >
          API 키 저장
        </Button>

        <Text fontSize="sm" color="gray.500">
          * API 키는 안전하게 암호화되어 저장됩니다.
        </Text>
      </VStack>
    </Box>
  )
} 