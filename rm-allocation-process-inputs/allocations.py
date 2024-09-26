import ast
import math

import numpy as np
from Constants import *


class RBDAllocations:

    def __init__(self, rmdatabasehelper, logger, constantsutils, mathutils):
        self.rmdatabasehelper = rmdatabasehelper
        self.logger = logger
        self.constantsutils = constantsutils
        self.mathutils = mathutils

    def b2CAllocation(self, currBookedLoad, openDataB2C, currRec, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter, currParams, profileFare, profileExtract, bookloadPercentile, upsellValue):
        b2cAllocationsUnits = []
        B2CRBDs = self.rmdatabasehelper.fareExtract(currParams.origin, currParams.destin, B2C_CHANNEL, '', 'L')
        maxRBDIndex = B2CRBDs['id'].iloc[-1]
        maxRBDPrice = B2CRBDs['Total'].iloc[-1]
        openingRBD = openDataB2C['RBD'][0]
        openbuketFlag = False
        openingRBDPrice = 0
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
                    openingRBDPrice = B2CRBDs['Total'][b2cctr]
                    newProfileFares = []
                    if profileFare != 'NA':
                        for x in profileExtract:
                            upsellVal = x * (1 + float(upsellValue)/100)
                            if upsellVal >= openingRBDPrice and upsellVal >= float(profileFare):
                                newProfileFares.append(upsellVal)

                        count = len(newProfileFares)
                        bookloadPercentile = PROFILE_FARES_QUARTILE
                        if count != 0 and newProfileFares[0] == openingRBDPrice:
                            bookloadPercentile = PROFILE_FARES_QUARTILE - count + 1
                        elif count != 0:
                            bookloadPercentile = PROFILE_FARES_QUARTILE - count
                        newProfileFares = sorted(set(newProfileFares))

            # Allocation
            if openbuketFlag:
                # Valid B2C Allocation
                openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b, counter, status, bookloadPercentile = self.getIncrementalValue(currBookedLoad, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter, currParams, profileFare, maxRBDPrice, openingRBDPrice, openRBDIndex, incremental, incrementalb2b, jumpingRBDPrice, entryRDB, d3Allocation, jumpPercent, remainingRBD, firstRDBAllocation, otherRDBAllocation, price, b2bFactor, nextPrice, newProfileFares, counter, bookloadPercentile)

                if incremental >= totalCapacity:
                    incremental = totalCapacity

                if incrementalb2b >= totalCapacity:
                    incrementalb2b = totalCapacity

                if RBDLabel == self.constantsutils.HIGHEST_B2C_RBD_VALUE:
                    incremental = totalCapacity
                if currRec.get_class_attribute(b2cctr,'class_au') is not None:
                    if incremental<currRec.get_class_attribute(b2cctr,'class_au'):
                        incremental=currRec.get_class_attribute(b2cctr,'class_au')
                if b2cctr < B2CRBDs.shape[0]:
                    currRec.set_class_attribute(b2cctr + 1, 'class_au', int(incremental))
                b2cAllocationsUnits.append(incrementalb2b)
                b2bctr = b2bctr + 1
            else:
                currRec.set_class_attribute(b2cctr + 1, 'class_au', 0)
            if b2cctr < B2CRBDs.shape[0]:
                b2cctr = b2cctr + 1

        return b2cAllocationsUnits, b2cctr + 1, status

    def getIncrementalValue(self, currBookedLoad, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter, currParams, profileFare, maxRBDPrice, openingRBDPrice, openRBDIndex, incremental, incrementalb2b, jumpingRBDPrice, entryRDB, d3Allocation, jumpPercent, remainingRBD, firstRDBAllocation, otherRDBAllocation, price, b2bFactor, nextPrice, profileExtract, counter, bookloadPercentile):
        if int(currParams.rbdPushFlag) == 0:
            status = 'Normal AU allocation'
            openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.calculateD3D4Values(currBookedLoad, bookedDeficit, dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPrice, openRBDIndex, incremental, incrementalb2b, jumpingRBDPrice, entryRDB, d3Allocation, jumpPercent, remainingRBD, firstRDBAllocation, otherRDBAllocation, price, b2bFactor, nextPrice)
        if int(currParams.rbdPushFlag) == 1:
            if profileFare == CONST_NA or profileFare == 0:
                status = 'Normal AU allocation-profileFare NA'
                if int(currParams.profileFares) == 0:
                    status = 'Normal AU allocation-profileFare skipped'
                openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.calculateD3D4Values(currBookedLoad, bookedDeficit, dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPrice, openRBDIndex, incremental, incrementalb2b, jumpingRBDPrice, entryRDB, d3Allocation, jumpPercent, remainingRBD, firstRDBAllocation, otherRDBAllocation, price, b2bFactor, nextPrice)
            elif len(profileExtract) == 0 and profileFare != CONST_NA:
                status = 'AU allocation based on quartiles'
                if profileFare <= openingRBDPrice:
                    incremental = totalCapacity
            else:
                status = 'AU allocation based on quartiles'
                if counter < len(profileExtract):
                    if profileExtract[-1] <= price:
                        incremental = totalCapacity
                    else:
                        jumpingRBDPrice = profileExtract[counter]
                        if price < profileFare:
                            firstRDBAllocation = currBookedLoad + 5
                            firstRDBAllocationb2b = currBookedLoad + (5 * b2bFactor)
                            otherRDBAllocation = self.mathutils.generateRoundNumber((totalCapacity / PROFILE_FARES_QUARTILE) * int(bookloadPercentile))
                            otherRDBAllocationb2b = self.mathutils.generateRoundNumber((totalCapacity / PROFILE_FARES_QUARTILE) * (int(bookloadPercentile) * b2bFactor))
                        else:
                            firstRDBAllocation = self.mathutils.generateRoundNumber((totalCapacity / PROFILE_FARES_QUARTILE) * int(bookloadPercentile))
                            firstRDBAllocationb2b = self.mathutils.generateRoundNumber((totalCapacity / PROFILE_FARES_QUARTILE) * (int(bookloadPercentile) * b2bFactor)) 
                            otherRDBAllocation = self.mathutils.generateRoundNumber((totalCapacity / PROFILE_FARES_QUARTILE) * int(bookloadPercentile))
                            otherRDBAllocationb2b = self.mathutils.generateRoundNumber((totalCapacity / PROFILE_FARES_QUARTILE) * (int(bookloadPercentile) * b2bFactor))

                        openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b, counter, bookloadPercentile = self.jumpWithFare(bookedDeficit, openRBDIndex, incremental, firstRDBAllocation, otherRDBAllocation, jumpingRBDPrice, b2bFactor, incrementalb2b, counter, bookloadPercentile, price, firstRDBAllocationb2b, otherRDBAllocationb2b)

        return openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b, counter, status, bookloadPercentile

    def calculateD3D4Values(self, currBookedLoad, bookedDeficit, dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPrice, openRBDIndex, incremental, incrementalb2b, jumpingRBDPrice, entryRDB, d3Allocation, jumpPercent, remainingRBD, firstRDBAllocation, otherRDBAllocation, price, b2bFactor, nextPrice):
        # Positive Allocation
        if firstRDBAllocation >= 0:
            openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, firstRDBAllocation, otherRDBAllocation, jumpingRBDPrice, b2bFactor, incrementalb2b)

        # Negative -99 Allocation
        elif firstRDBAllocation == DYNAMIC_D3_D4:
            openRBDIndex, entryRDB, d3Allocation, d4Allocation, remainingRBD = self.d3d4Calculations(dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPrice, openRBDIndex, entryRDB, remainingRBD, d3Allocation, jumpPercent)
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

    def jumpWithFare(self, bookedDeficit, openRBDIndex, incremental, d3Allocation, d4Allocation, profileFare, b2bFactor, incrementalb2b, counter, bookloadPercentile, currPrice, firstRDBAllocationb2b, otherRDBAllocationb2b):
        if incremental < 0:
            if d3Allocation >= 0:
                # allocate the FirstRBD
                incremental = d3Allocation
                incrementalb2b = firstRDBAllocationb2b
            else:
                # negative number means allocate the full deficit - Only if the deficit is positive - Lagging
                if bookedDeficit > 0:
                    incremental = bookedDeficit
                else:
                    incremental = d4Allocation
                    incrementalb2b = otherRDBAllocationb2b
            bookloadPercentile += 1
        else:
            if profileFare > currPrice:
                pass
            else:
                incremental = d4Allocation
                incrementalb2b = otherRDBAllocationb2b
                openRBDIndex = openRBDIndex + 1
                counter = counter + 1
                bookloadPercentile += 1

        return openRBDIndex, incremental, profileFare, incrementalb2b, counter, bookloadPercentile

    def d3d4Calculations(self, dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPrice, openRBDIndex, entryRDB, remainingRBD, d3Allocation, jumpPercent):
        maxLimit = self.mathutils.generateMaxValue(remainingCapacity, ndocounter)
        if entryRDB:
            if jumpPercent > 0 and openingRBDPrice - maxRBDPrice != 0:
                remainingRBD = self.mathutils.calculateLogValueForD3(maxRBDPrice, openingRBDPrice, jumpPercent)

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

    def b2BAllocation(self, currBookedLoad, openDataB2B, currRec, totalCapacity, dPLF, b2cAllocationsUnits, b2bSkipFlag, b2boverbookingflag, brd_counter, openDataB2C, currParams,overBookingCount):
        isUpsellFlag = False
        B2BRBDs = self.rmdatabasehelper.fareExtract(currParams.origin, currParams.destin, B2B_CHANNEL, '', 'L')
        openingB2CRBD = openDataB2C['RBD'][0]

        B2CRBDs = self.rmdatabasehelper.fareExtract(currParams.origin, currParams.destin, B2C_CHANNEL, openingB2CRBD, 'L')

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
            if overBookingCount>0:
                currRec.set_class_attribute(brd_counter, 'class_au', 0)
            else:
                if b2bSkipFlag:
                    currRec.set_class_attribute(brd_counter, 'class_au', -1)
                else:
                    if RBDLabel == openingRBD:
                        openbuketFlag = True
                        openingRBDPrice = price

                    if openbuketFlag:
                        # Allocation
                        if (b2bStatus == 0) or (b2bStatus == 2):
                            if dPLF < 0 and (PLF < self.constantsutils.PLF_CURVE_VALUE or (openingB2CPrice <= openingRBDPrice)):
                                value=int(self.mathutils.generateRoundNumber(b2cAllocationsUnits[counter]))
                                if currRec.get_class_attribute(brd_counter-b2bctr,'class_au') is not None:
                                    if value<currRec.get_class_attribute(brd_counter-b2bctr,'class_au'):
                                        value=currRec.get_class_attribute(brd_counter-b2bctr,'class_au')
                                currRec.set_class_attribute(brd_counter, 'class_au', value)
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
    
    # def group_quote_alloc(self, profilefarextract, brd_counter, currRec, autoGroup, adjustedCapacity,upsell):
    #     rbd_map = ast.literal_eval(self.constantsutils.GROUP_QUOTATION)
    #     profilefarextract = profilefarextract[[col for col in profilefarextract.columns if col in rbd_map]].rename(columns=rbd_map)
    #     profilefarextract = profilefarextract.melt(var_name='RBD', value_name='Total')

    #     # Calculate base allocation unit
    #     base_unit = math.ceil(adjustedCapacity / 10)

    #     # Loop Through
    #     for grpctr in range(profilefarextract.shape[0]):
    #         RBDLabel = profilefarextract['RBD'][grpctr]
    #         price = profilefarextract['Total'][grpctr]
    #         currRec.set_class_attribute(brd_counter, 'class_of_service', RBDLabel)
            
    #         # Calculate allocation for this RBD
    #         allocation = min((grpctr + 1) * base_unit, adjustedCapacity)
            
    #         # Set allocation
    #         currRec.set_class_attribute(brd_counter, 'class_au', allocation)
            
    #         # Break if we've allocated all capacity
    #         if allocation == adjustedCapacity:
    #             break
            
    #         brd_counter = brd_counter + 1

    #     return False, brd_counter


    def group_quote_alloc(self, profilefarextract, brd_counter, currRec, autoGroup, adjustedCapacity, upsell):
        rbd_map = ast.literal_eval(self.constantsutils.GROUP_QUOTATION)
        profilefarextract = profilefarextract[[col for col in profilefarextract.columns if col in rbd_map]].rename(columns=rbd_map)
        profilefarextract = profilefarextract.melt(var_name='RBD', value_name='Total')
        
        # Calculate upsell fares
        profilefarextract['Upsell_Total'] = profilefarextract['Total'] * (1 + int(upsell) / 100)
        
        # Calculate base allocation unit
        base_unit = max(1, math.ceil(adjustedCapacity / 10))  # Ensure base_unit is at least 1
        
        allocated_capacity = 0
        previous_allocation = 0
        
        for grpctr in range(profilefarextract.shape[0]):
            RBDLabel = profilefarextract['RBD'].iloc[grpctr]
            original_price = profilefarextract['Total'].iloc[grpctr]
            upsell_price = profilefarextract['Upsell_Total'].iloc[grpctr]
            
            currRec.set_class_attribute(brd_counter, 'class_of_service', RBDLabel)
            currRec.set_class_attribute(brd_counter, 'fare', original_price)
            
            current_allocation = min((grpctr + 1) * base_unit, adjustedCapacity)
            allocation = current_allocation - previous_allocation
            
            if upsell != 0:
                # Find the nearest original fare that's greater than or equal to the upsell fare
                higher_fares = profilefarextract[profilefarextract['Total'] >= upsell_price]
                if higher_fares.empty or higher_fares.index[0] != grpctr:
                    allocation = 0
            
            currRec.set_class_attribute(brd_counter, 'class_au', allocation)
            
            allocated_capacity += allocation
            previous_allocation = current_allocation
            
            if allocated_capacity >= adjustedCapacity:
                break
            
            brd_counter += 1
        
        # Allocate any remaining capacity to the last fare class
        if allocated_capacity < adjustedCapacity:
            last_allocation = currRec.get_class_attribute(brd_counter - 1, 'class_au')
            currRec.set_class_attribute(brd_counter - 1, 'class_au', last_allocation + (adjustedCapacity - allocated_capacity))
        
        return False, brd_counter





    def b2CAllocation_connections(self, openDataB2C, currRec, totalCapacity, currBookedLoad, SkippingFactor, currParams):

        b2cAllocationsUnits = []
        origin = currParams.sector1[:3]
        destin = currParams.sector2[-3:]
        B2CRBDs = self.rmdatabasehelper.fareExtract(origin, destin, 'B2C', '', 'R')

        openingRBD = openDataB2C['RBD'][0]
        openbuketFlag = False
        openRBDIndex = 0
        incremental = -1
        incrementalb2b = -1
        size = B2CRBDs.shape[0] - 1
        jumpingRBDPrice = 0
        b2cctr = 0
        b2bctr = 0
        jumpPercent = SkippingFactor
        # reset
        # Loop on each rbd
        while b2cctr < B2CRBDs.shape[0] or b2bctr < B2B_RDB_COUNT:

            if b2cctr < B2CRBDs.shape[0]:
                RBDLabel = B2CRBDs['RBD'][b2cctr]
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

            # Allocation
            if openbuketFlag:
                # Valid B2C Allocation
                # Positive Allocation
                if firstRDBAllocation >= 0:
                    openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, 0, openRBDIndex, incremental, jumpPercent, price, nextPrice, firstRDBAllocation, otherRDBAllocation, jumpingRBDPrice, b2bFactor, incrementalb2b)

                if incremental >= totalCapacity:
                    incremental = totalCapacity
                if incrementalb2b >= totalCapacity:
                    incrementalb2b = totalCapacity
                if b2cctr < B2CRBDs.shape[0]:
                    currRec.set_class_attribute(b2cctr + 1, 'class_au', int(incremental))
                b2cAllocationsUnits.append(incrementalb2b)
                b2bctr = b2bctr + 1

            else:
                currRec.set_class_attribute(b2cctr + 1, 'class_au', 0)

            if b2cctr < B2CRBDs.shape[0]:
                b2cctr = b2cctr + 1

        return b2cAllocationsUnits, b2cctr+1

    def b2BAllocation_connections(self, openDataB2B, currRec, b2cAllocationsUnits, brd_counter, currParams):
        origin = currParams.sector1[:3]
        destin = currParams.sector2[-3:]
        B2BRBDs = self.rmdatabasehelper.fareExtract(origin, destin, 'B2B', '', 'R')
        openbuketFlag = False
        openingRBD = openDataB2B['RBD'][0]
        counter = 0
        # Loop on each rbd

        for b2bctr in range(B2BRBDs.shape[0]):
            RBDLabel = B2BRBDs['RBD'][b2bctr]
            b2bStatus = openDataB2B['statusInd'][0]
            currRec.set_class_attribute(brd_counter, 'class_of_service', RBDLabel)
            if RBDLabel == openingRBD:
                openbuketFlag = True

            if openbuketFlag:
                # Allocation
                if (b2bStatus == 0) or (b2bStatus == 2):
                    currRec.set_class_attribute(brd_counter, 'class_au', int(self.mathutils.generateRoundNumber(b2cAllocationsUnits[counter])))
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
        return brd_counter, currRec
