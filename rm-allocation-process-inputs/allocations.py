from Constants import *


class RBDAllocations:
       
    def __init__(self, rmdatabasehelper, logger, constantsutils, mathutils):
        self.rmdatabasehelper = rmdatabasehelper
        self.logger = logger
        self.constantsutils = constantsutils
        self.mathutils = mathutils

    def b2CAllocation(self, currBookedLoad, openDataB2C, currRec, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter, currParams, profileFare, profileExtract, bookloadPercentile, upsellValue):
        b2cAllocationsUnits = []
        B2CRBDs = self.rmdatabasehelper.fareExtract(currParams.origin, currParams.destin, B2C_CHANNEL, '')
        maxRBDIndex = B2CRBDs['id'].iloc[-1]
        maxRBDPrice = B2CRBDs['Total'].iloc[-1]
        openingRBD = openDataB2C['RBD'][0]
        openbuketFlag = False
        openingRBDPirce = 0
        openRBDIndex = 0
        incremental = -1
        incrementalb2b = -1
        size = B2CRBDs.shape[0] - 1
        jumpingRBDPrice = 0
        entryRDB = True
        d3Allocation = 0
        b2cctr = 0
        b2bctr = 0
        jumpPercent = currParams.skipping_factor
        counter = 0
        status = CONST_NA
        # reset
        # Loop Through
        while b2cctr < B2CRBDs.shape[0] or b2bctr < self.constantsutils.B2B_RDB_COUNT:
            if b2cctr < B2CRBDs.shape[0]:
                RBDLabel = B2CRBDs['RBD'][b2cctr] 
                remainingRBD = maxRBDIndex - openRBDIndex
                firstRDBAllocation = int(currParams.first_rbd_alloc)
                otherRDBAllocation = int(currParams.other_rbd_alloc)
                price = B2CRBDs['Total'][b2cctr]
                b2bFactor = float(currParams.b2b_factor)
                nextPrice = price
                if size > b2cctr:
                    nextPrice = B2CRBDs['Total'][b2cctr + 1]
                currRec.set_class_attribute(b2cctr + 1, 'class_of_service', RBDLabel)
                if RBDLabel == openingRBD:
                    openbuketFlag = True
                    openingRBDPirce = B2CRBDs['Total'][b2cctr]
                    newProfileFares = []
                    count = 0
                    if profileFare !='NA':
                        for x in profileExtract:
                            if (x * (1 + upsellValue/100)) >= openingRBDPirce and (x * (1 + upsellValue/100)) >= float(profileFare):
                                newProfileFares.append(x * (1 + upsellValue/100))
                                count = count + 1
                        newProfileFares = sorted(set(newProfileFares))
                        bookloadPercentile = PROFILE_FARES_QUARTILE - count
            # Allocation
            if openbuketFlag:
                # Valid B2C Allocation
                openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b, counter, status = self.getIncrementalValue(currBookedLoad, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter, currParams, profileFare, maxRBDPrice, openingRBDPirce, openRBDIndex, incremental, incrementalb2b, jumpingRBDPrice, entryRDB, d3Allocation, jumpPercent, remainingRBD, firstRDBAllocation, otherRDBAllocation, price, b2bFactor, nextPrice, newProfileFares, counter, bookloadPercentile)

                if incremental >= totalCapacity:
                    incremental = totalCapacity

                if incrementalb2b >= totalCapacity:
                    incrementalb2b = totalCapacity

                if RBDLabel == self.constantsutils.HIGHEST_B2C_RBD_VALUE:
                    incremental = totalCapacity
                    
                if b2cctr < B2CRBDs.shape[0]:
                    currRec.set_class_attribute(b2cctr + 1, 'class_au', int(incremental))
                b2cAllocationsUnits.append(incrementalb2b)
                b2bctr = b2bctr + 1
            else:
                currRec.set_class_attribute(b2cctr + 1, 'class_au', 0)
            if b2cctr < B2CRBDs.shape[0]:
                b2cctr = b2cctr + 1
            
        return b2cAllocationsUnits, b2cctr+1, status

    def getIncrementalValue(self, currBookedLoad, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter, currParams, profileFare, maxRBDPrice, openingRBDPirce, openRBDIndex, incremental, incrementalb2b, jumpingRBDPrice, entryRDB, d3Allocation, jumpPercent, remainingRBD, firstRDBAllocation, otherRDBAllocation, price, b2bFactor, nextPrice, profileExtract, counter, bookloadPercentile):
        if int(currParams.rbdPushFlag) == 0:
            status = 'Normal AU allocation'
            openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.calculateD3D4Values(currBookedLoad, bookedDeficit, dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPirce, openRBDIndex, incremental, incrementalb2b, jumpingRBDPrice, entryRDB, d3Allocation, jumpPercent, remainingRBD, firstRDBAllocation, otherRDBAllocation, price, b2bFactor, nextPrice)
        if int(currParams.rbdPushFlag) == 1:
            if profileFare == CONST_NA or profileFare == 0 or len(profileExtract) == 0:
                status = 'Normal AU allocation-profileFare NA'
                if currParams.profileFares == 'N':
                    status = 'Normal AU allocation-profileFare skipped'
                openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.calculateD3D4Values(currBookedLoad, bookedDeficit, dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPirce, openRBDIndex, incremental, incrementalb2b, jumpingRBDPrice, entryRDB, d3Allocation, jumpPercent, remainingRBD, firstRDBAllocation, otherRDBAllocation, price, b2bFactor, nextPrice)
            else:
                status = 'AU allocation based on quartiles'
                if counter < len(profileExtract):
                    if profileExtract[-1] <= price:
                        incremental = totalCapacity
                    else:
                        jumpingRBDPrice = profileExtract[counter]

                        if price <= profileFare:
                            firstRDBAllocation = 5
                            otherRDBAllocation = self.mathutils.generateRoundNumber(totalCapacity / PROFILE_FARES_QUARTILE)
                        else:
                            firstRDBAllocation = self.mathutils.generateRoundNumber((totalCapacity / PROFILE_FARES_QUARTILE) * int(bookloadPercentile))
                            otherRDBAllocation = self.mathutils.generateRoundNumber(totalCapacity / PROFILE_FARES_QUARTILE)
                            
                        openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b, counter = self.jumpWithFare(currBookedLoad, bookedDeficit, openRBDIndex, incremental, nextPrice, firstRDBAllocation, otherRDBAllocation, jumpingRBDPrice, b2bFactor, incrementalb2b, price, counter)
        return openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b, counter, status

    def calculateD3D4Values(self, currBookedLoad, bookedDeficit, dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPirce, openRBDIndex, incremental, incrementalb2b, jumpingRBDPrice, entryRDB, d3Allocation, jumpPercent, remainingRBD, firstRDBAllocation, otherRDBAllocation, price, b2bFactor, nextPrice):
        # Positive Allocation
        if firstRDBAllocation >= 0:
            openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, firstRDBAllocation, otherRDBAllocation, jumpingRBDPrice, b2bFactor, incrementalb2b)

        # Negative -99 Allocation
        elif firstRDBAllocation == DYNAMIC_D3_D4:
            openRBDIndex, entryRDB, d3Allocation, d4Allocation, remainingRBD = self.d3d4Calculations(dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPirce, openRBDIndex, entryRDB, remainingRBD, d3Allocation, jumpPercent)
            openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, d3Allocation, d4Allocation, jumpingRBDPrice, b2bFactor, incrementalb2b)

        # Negative -1 Allocation
        elif firstRDBAllocation == DEFICIT_ALLOCATION:
            bookedDef = int(round(bookedDeficit, 0))
            if bookedDef <= 0:
                bookedDef = 0
            openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, bookedDef, otherRDBAllocation, jumpingRBDPrice, b2bFactor, incrementalb2b)
                    
        # Negative -2 Allocation
        elif firstRDBAllocation == LINEAR_ALLOCATION:
            allocation = int(round(remainingCapacity/otherRDBAllocation, 0))
            openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, allocation, allocation, jumpingRBDPrice, b2bFactor, incrementalb2b)

        # Negative -3 Allocation
        elif firstRDBAllocation == LINEAR_ALLOCATION_WITH_JUMP:
            allocation = int(round(remainingCapacity/otherRDBAllocation, 0))
            openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, allocation, allocation, jumpingRBDPrice, b2bFactor, incrementalb2b)
            
        return openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b

    def jumpingAllocation(self, currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, d3Allocation, d4Allocation, jumpingRBDPrice, b2bFactor, incrementalb2b):
        if incremental < 0:
            if d3Allocation >= 0:
                # allocate the FirstRBD
                incremental = currBookedLoad + d3Allocation
                incrementalb2b = currBookedLoad + (d3Allocation * b2bFactor)
            else:
                # Negative number means allocate the full deficit - Only if the deficit is positive - Lagging
                if bookedDeficit > 0:
                    incremental = currBookedLoad + bookedDeficit
                else:
                    incremental = currBookedLoad + d4Allocation
                    incrementalb2b = currBookedLoad + (d4Allocation * b2bFactor)
            jumpingRBDPrice = price + (price * (jumpPercent / 100))
        else:
            nextPriceDiff = abs(nextPrice - jumpingRBDPrice)
            currPriceDiff = abs(price - jumpingRBDPrice)
            if jumpingRBDPrice >= nextPrice:
                pass
            elif currPriceDiff <= nextPriceDiff:
                incremental = incremental + d4Allocation
                incrementalb2b = incrementalb2b + (d4Allocation * b2bFactor)
                jumpingRBDPrice = price + (price * (jumpPercent / 100))
                openRBDIndex = openRBDIndex + 1
            elif currPriceDiff > nextPriceDiff:
                pass
        return openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b

    def jumpWithFare(self, currBookedLoad, bookedDeficit, openRBDIndex, incremental, nextPrice, d3Allocation, d4Allocation, jumpingRBDPrice, b2bFactor, incrementalb2b, price, counter):
        if incremental < 0:
            if d3Allocation >= 0:
                # allocate the FirstRBD
                incremental = currBookedLoad + d3Allocation
                incrementalb2b = currBookedLoad + (d3Allocation * b2bFactor)
            else:
                # negative number means allocate the full deficit - Only if the deficit is positive - Lagging
                if bookedDeficit > 0:
                    incremental = currBookedLoad + bookedDeficit
                else:
                    incremental = currBookedLoad + d4Allocation
                    incrementalb2b = currBookedLoad + (d4Allocation * b2bFactor)
        else:
            nextPriceDiff = abs(nextPrice - jumpingRBDPrice)
            currPriceDiff = abs(price - jumpingRBDPrice)
            if jumpingRBDPrice >= nextPrice:
                pass
            elif currPriceDiff <= nextPriceDiff:
                incremental = incremental + d4Allocation
                incrementalb2b = incrementalb2b + (d4Allocation * b2bFactor)
                openRBDIndex = openRBDIndex + 1
                counter = counter + 1
            elif currPriceDiff > nextPriceDiff:
                pass
                
        return openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b, counter

    def d3d4Calculations(self, dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPirce, openRBDIndex, entryRDB, remainingRBD, d3Allocation, jumpPercent):
        maxLimit = self.mathutils.generateMaxValue(remainingCapacity, ndocounter)
        if entryRDB:
            if jumpPercent > 0 and openingRBDPirce - maxRBDPrice != 0:
                remainingRBD = self.mathutils.calculateLogValueForD3(maxRBDPrice, openingRBDPirce, jumpPercent)

            d3Allocation = self.mathutils.dynamicAllocationD3Formula(dPLF, remainingCapacity, remainingRBD, ndocounter)
            if d3Allocation <= self.constantsutils.MIN_D3_D4_VALUE:
                d3Allocation = self.constantsutils.MIN_D3_D4_VALUE
            elif d3Allocation >= maxLimit:
                d3Allocation = maxLimit
            entryRDB = False

        openRBDIndex = openRBDIndex + 1

        d4Allocation = self.mathutils.dynamicAllocationD4Formula(dPLF, openRBDIndex, d3Allocation)

        if d4Allocation <= self.constantsutils.MIN_D3_D4_VALUE:
            d4Allocation = self.constantsutils.MIN_D3_D4_VALUE
        elif d4Allocation >= maxLimit:
            d4Allocation = maxLimit

        return openRBDIndex, entryRDB, d3Allocation, d4Allocation, remainingRBD

    def b2BAllocation(self, currBookedLoad, openDataB2B, currRec, totalCapacity, dPLF, b2cAllocationsUnits, b2bSkipFlag, b2boverbookingflag, brd_counter, openDataB2C, currParams):
        isUpsellFlag = False
        B2BRBDs = self.rmdatabasehelper.fareExtract(currParams.origin, currParams.destin, B2B_CHANNEL, '')
        openingB2CRBD = openDataB2C['RBD'][0]

        B2CRBDs = self.rmdatabasehelper.fareExtract(currParams.origin, currParams.destin, B2C_CHANNEL, openingB2CRBD)

        openingB2CPrice = self.constantsutils.B2B_FARE_PRICE_COMPARISON * B2CRBDs['Total'][0]

        openbuketFlag = False
        openingRBD = openDataB2B['RBD'][0]
        
        PLF = currBookedLoad / totalCapacity
        counter = 0
        # Loop Through
        
        for b2bctr in range(B2BRBDs.shape[0]):
            RBDLabel = B2BRBDs['RBD'][b2bctr]
            price = B2BRBDs['Total'][b2bctr]
            b2bStatus = openDataB2B['statusInd'][0]
            currRec.set_class_attribute(brd_counter, 'class_of_service', RBDLabel)
            if b2boverbookingflag:
                currRec.set_class_attribute(brd_counter, 'class_au', 0)
            else:
                if b2bSkipFlag:
                    currRec.set_class_attribute(brd_counter, 'class_au', -1)
                else:
                    if RBDLabel == openingRBD:
                        openbuketFlag = True
                        openingRBDPirce = price

                    if openbuketFlag:
                        # Allocation
                        if (b2bStatus == 0) or (b2bStatus == 2):
                            if dPLF < 0 and (PLF < self.constantsutils.PLF_CURVE_VALUE or (openingB2CPrice <= openingRBDPirce)):
                                currRec.set_class_attribute(brd_counter, 'class_au', int(self.mathutils.generateRoundNumber(b2cAllocationsUnits[counter])))
                            else:
                                currRec.set_class_attribute(brd_counter, 'class_au', 0)
                                isUpsellFlag = True
                            counter = counter + 1
                        else:
                            # No B2B Allocaion - Leave it to B2C Fare
                            currRec.set_class_attribute(brd_counter, 'class_au', 0)
                    else:
                        if b2bStatus == 3:
                            currRec.set_class_attribute(brd_counter, 'class_au', -1)
                        else:
                            currRec.set_class_attribute(brd_counter, 'class_au', 0)
            brd_counter = brd_counter+1
            
        return isUpsellFlag, brd_counter
