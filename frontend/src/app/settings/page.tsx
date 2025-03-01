'use client'

import {
  Box,
  Container,
  Grid,
  GridItem,
  useColorModeValue,
} from '@chakra-ui/react'
import Navbar from '@/components/Navbar'
import TradingSettings from '@/components/settings/TradingSettings'
import APISettings from '@/components/settings/APISettings'
import NotificationSettings from '@/components/settings/NotificationSettings'
import RiskManagement from '@/components/settings/RiskManagement'

export default function SettingsPage() {
  return (
    <Box minH="100vh">
      <Navbar />
      <Container maxW="container.xl" py={6}>
        <Grid templateColumns="repeat(12, 1fr)" gap={6}>
          <GridItem colSpan={{ base: 12, lg: 6 }}>
            <TradingSettings />
          </GridItem>
          
          <GridItem colSpan={{ base: 12, lg: 6 }}>
            <RiskManagement />
          </GridItem>
          
          <GridItem colSpan={{ base: 12, lg: 6 }}>
            <APISettings />
          </GridItem>
          
          <GridItem colSpan={{ base: 12, lg: 6 }}>
            <NotificationSettings />
          </GridItem>
        </Grid>
      </Container>
    </Box>
  )
} 