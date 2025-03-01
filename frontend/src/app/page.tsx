'use client'

import { Box, Container, Grid, GridItem } from '@chakra-ui/react'
import Navbar from '@/components/Navbar'
import PortfolioSummary from '@/components/dashboard/PortfolioSummary'
import TradingChart from '@/components/dashboard/TradingChart'
import ActiveTrades from '@/components/dashboard/ActiveTrades'
import MarketOverview from '@/components/dashboard/MarketOverview'

export default function Home() {
  return (
    <Box minH="100vh">
      <Navbar />
      <Container maxW="container.xl" py={6}>
        <Grid
          templateColumns={{ base: 'repeat(1, 1fr)', lg: 'repeat(3, 1fr)' }}
          gap={6}
        >
          <GridItem colSpan={{ base: 1, lg: 3 }}>
            <PortfolioSummary />
          </GridItem>
          
          <GridItem colSpan={{ base: 1, lg: 2 }}>
            <TradingChart />
          </GridItem>
          
          <GridItem colSpan={1}>
            <ActiveTrades />
          </GridItem>
          
          <GridItem colSpan={{ base: 1, lg: 3 }}>
            <MarketOverview />
          </GridItem>
        </Grid>
      </Container>
    </Box>
  )
} 