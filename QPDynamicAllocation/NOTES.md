## NOTES:
- Curves: check current bookedLoad and inform of deficit (inventory), using targetBookedLoad corresponding to NDO

- MarketFares: Range of fares for market

- deficit (A/B): identifies strategy

- strategy (MAX/MIN): where to anchorFare on MarketFares range

- Fares: which RBDs to open basis the anchorFare and MarketFares

- A Market = org, dest, days, dow, timeOfDay, [flightNumber, carrier]



## Algo Steps-
```
foreach recInMarketList:
    getBookedLoad(orig, dest, startDate, endDate)
    
    foreach date:
        getTargetbookLoadFromCurve()
        foreach flight:
            calcDeficit()
            getMarketRange()

            if B2CFaresExist():
                # determineRBD2Open(org, dest, deficit, low, high, fareAnchor, fareOffset, channel)
                checkClosestBucketBasisStrategyNOffset()

                allocateBucketsNFares()
                

```

