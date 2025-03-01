'use client'

import {
  Box,
  Container,
  Grid,
  GridItem,
  useColorModeValue,
} from '@chakra-ui/react'
import Navbar from '@/components/Navbar'
import TradingChart from '@/components/trading/DetailedChart'
import OrderBook from '@/components/trading/OrderBook'
import TradeForm from '@/components/trading/TradeForm'
import MarketInfo from '@/components/trading/MarketInfo'

export default function TradingPage() {
  return (
    <Box minH="100vh">
      <Navbar />
      <Container maxW="container.xl" py={6}>
        <Grid
          templateColumns={{ base: '1fr', lg: '3fr 1fr' }}
          gap={6}
        >
          <GridItem>
            <Grid gap={6}>
              <GridItem>
                <TradingChart />
              </GridItem>
              <GridItem>
                <MarketInfo />
              </GridItem>
            </Grid>
          </GridItem>
          
          <GridItem>
            <Grid gap={6}>
              <GridItem>
                <TradeForm />
              </GridItem>
              <GridItem>
                <OrderBook />
              </GridItem>
            </Grid>
          </GridItem>
        </Grid>
      </Container>
    </Box>
  )
} 