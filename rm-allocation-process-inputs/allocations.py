import ast
import copy
import json
import math

import numpy as np
from Constants import *
import pandas as pd


class RBDAllocations:

    def __init__(self, rmdatabasehelper, logger, constantsutils, mathutils):
        self.rmdatabasehelper = rmdatabasehelper
        self.logger = logger
        self.constantsutils = constantsutils
        self.mathutils = mathutils

    def b2CAllocation(self, currBookedLoad, openDataB2C, currRec, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter, currParams, profileFare, profileExtract, bookloadPercentile, upsellValue):
        b2cAllocationsUnits = []
        B2CRBDs = self.rmdatabasehelper.fareExtract(currParams.origin, currParams.destin, B2C_CHANNEL, '', 'L')
        fullRbds=B2CRBDs
        rbd_list = self.constantsutils.TBF_RBD.split(',')
        rbd_list.extend(list(json.loads(GROUP_QUOTATION).values()))
        B2CRBDs = B2CRBDs[~B2CRBDs['RBD'].isin(rbd_list)].reset_index(drop=True)
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
        jumpingRBDPrice_list = []
        incremental_list = []
        rbdlabel_list=[]
        price_list=[]
        
        if upsellValue is None or str(upsellValue).strip() == '' or str(upsellValue).strip().upper() == 'NA':
            upsellValue = 0
        
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
            jumpingRBDPrice_list.append(jumpingRBDPrice)
            incremental_list.append(incremental)
            rbdlabel_list.append(RBDLabel)
            price_list.append(price)
        data = {
        'jumpingRBDPrice': jumpingRBDPrice_list,
        'incremental': incremental_list,
        'RBD':rbdlabel_list,
        'price':price_list
        }
        df = pd.DataFrame(data)
        df_filtered = df[(df['incremental'] != -1)]
        df_filtered['price_diff'] = df_filtered['jumpingRBDPrice'] - df_filtered['price']
        df_unique = df_filtered.sort_values(['price', 'price_diff']).drop_duplicates('RBD')
        df_unique = df_unique.drop(columns='price_diff')

        return b2cAllocationsUnits, b2cctr + 1, status,df_unique,fullRbds,openingRBDPrice
    
    def b2CAllocations(self, currBookedLoad, openDataB2C, currRec, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter, currParams, profileFare, profileExtract, bookloadPercentile, upsellValue,openingRBDPrice):
        # Store the original rbdPushFlag
        
        # Create a copy of currParams to modify
        
        currRec_copy = copy.deepcopy(currRec)
        
        
        # First allocation with rbdPushFlag = 0
        currParams.rbdPushFlag = 0
        _, _, _, df_unique_0, _,_ = self.b2CAllocation(
            currBookedLoad, openDataB2C, currRec_copy, totalCapacity, bookedDeficit, dPLF, 
            remainingCapacity, ndocounter, currParams, profileFare, profileExtract, 
            bookloadPercentile, upsellValue
        )
        
        
        currRec_copy = copy.deepcopy(currRec)
        
        
        
        # Second allocation with rbdPushFlag = 1
        currParams.rbdPushFlag = 1
        _, _, _, df_unique_1, _,_ = self.b2CAllocation(
            currBookedLoad, openDataB2C, currRec_copy, totalCapacity, bookedDeficit, dPLF, 
            remainingCapacity, ndocounter, currParams, profileFare, profileExtract, 
            bookloadPercentile, upsellValue
        )
        
        df_unique_1_modified = df_unique_1.copy()
    
        # Set incremental to 0 where price difference condition is not met
        price_diff_mask = abs(openingRBDPrice-df_unique_1_modified['price']) >= self.constantsutils.TBF_DIFF_FARE
        df_unique_1_modified.loc[price_diff_mask, 'incremental'] = 0
        merged_df = pd.concat([df_unique_0, df_unique_1_modified], ignore_index=True)
    
        # Select row with higher incremental value for each unique RBD
        final_df = merged_df.sort_values('incremental', ascending=False).drop_duplicates('RBD', keep='first')
    
        # Sort the final DataFrame by price to maintain original order
        final_df = final_df.sort_values('price')
        if int(currParams.rbdPushFlag)==1:
            return df_unique_1
        # Return results of both allocations
        return final_df

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
                            firstRDBAllocation = currBookedLoad + self.constantsutils.PROF_OPEN_COUNT
                            firstRDBAllocationb2b = currBookedLoad + (self.constantsutils.PROF_OPEN_COUNT * b2bFactor)
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
    
    def group_quote_alloc(self, profilefarextract, brd_counter, currRec, autoGroup, adjustedCapacity, upsell,B2CRBDs,openingRBDPrice):
        rbd_list = list(json.loads(GROUP_QUOTATION).values())
        group_alloc_df = B2CRBDs[B2CRBDs['RBD'].isin(rbd_list)]
        group_alloc_df=group_alloc_df.sort_values(by='Total').reset_index(drop=True)
        rbd_map = ast.literal_eval(self.constantsutils.GROUP_QUOTATION)
        if int(autoGroup) == 1 and (not profilefarextract.empty):
            profilefarextract = profilefarextract[[col for col in profilefarextract.columns if col in rbd_map]].rename(columns=rbd_map)
            profilefarextract = profilefarextract.melt(var_name='RBD', value_name='Total')

            # Calculate base allocation unit
            base_unit = math.ceil(adjustedCapacity / 10)
            try:
                upsell_value = int(upsell) if upsell and str(upsell).strip().upper() != 'NA' else 0
            except (ValueError, TypeError):
                upsell_value = 0
                
            profilefarextract['Upsell'] = profilefarextract['Total'] * (1 + upsell_value / 100)

            # Sort the dataframe by original fares
            profilefarextract = profilefarextract.sort_values(by='Total').reset_index(drop=True)

            # Initialize allocation dictionary
            allocation_dict = {RBD: {'allocation': 0, 'price_diff': 0} for RBD in profilefarextract['RBD']}

            # Initialize allocation counter
            allocation_counter = 0
            profilefarextract = profilefarextract.merge(
            group_alloc_df[['RBD', 'Total']],
            on='RBD',
            how='left',
            suffixes=('', '_QP')
        )
            # Loop through upsell fares and allocate based on nearest original fare
            for current_index, row in profilefarextract.iterrows():
                upsell_price = row['Upsell']
                # profilefarextract = profilefarextract[(profilefarextract['RBD'].map(lambda x: allocation_dict[x] == 0)) &
                # (profilefarextract.index >= current_index)]
                # Find the nearest original fare that hasn't been fully allocated
                if profilefarextract.empty:
                    break
                nearest_index = (profilefarextract['Total_QP'] - upsell_price).abs().idxmin()
                nearest_price = profilefarextract['Total'][nearest_index]
                RBDLabel = profilefarextract['RBD'][nearest_index]
                price_diff = openingRBDPrice - profilefarextract.loc[nearest_index, 'Total_QP']
                if RBDLabel == profilefarextract.iloc[-1]['RBD'] and allocation_dict[RBDLabel]['allocation'] > 0:
                    continue
                # Calculate allocation for this RBD
                allocation = min((allocation_counter + 1) * base_unit, adjustedCapacity)

                # Set allocation
                allocation_dict[RBDLabel] = {
                    'allocation': allocation,
                    'price_diff': price_diff
                }

                # Increment allocation counter
                allocation_counter += 1

                # Break if we've allocated all capacity
                if allocation == adjustedCapacity:
                    break
            last_non_zero_allocation = 0
            # Apply allocations to the current record
            for RBDLabel, value in allocation_dict.items():
                
                price_diff = value['price_diff'] 
                if price_diff<self.constantsutils.GROUP_DIFF_FARE:
                    allocation = value['allocation']
                else:
                    allocation=0
                if allocation == 0 and price_diff<self.constantsutils.GROUP_DIFF_FARE:
                    # If it is, use the last non-zero allocation
                    allocation = last_non_zero_allocation
                else:
                    # If it's not, update the last non-zero allocation
                    last_non_zero_allocation = allocation
                currRec.set_class_attribute(brd_counter, 'class_of_service', RBDLabel)
                currRec.set_class_attribute(brd_counter, 'class_au', allocation)
                brd_counter += 1
        elif int(autoGroup) == 0 or profilefarextract.empty:
            # Allocate all RBDs with -1
            for RBDLabel in rbd_map.values():
                currRec.set_class_attribute(brd_counter, 'class_of_service', RBDLabel)
                currRec.set_class_attribute(brd_counter, 'class_au', 0)
                brd_counter += 1

        return  brd_counter
    
    
    def tbf_allocation(self,alloc_df,brd_counter,currRec,tbfFlag,B2CRBDs,currtbfBookedLoad,currBookedLoad,adjustedCapacity,openingRBDPrice,month,sector):
        max_au = currBookedLoad-currtbfBookedLoad+30
        max_au = min(max_au,adjustedCapacity-20)
        rbd_list = self.constantsutils.TBF_RBD.split(',')
        tbf_rbds = B2CRBDs[B2CRBDs['RBD'].isin(rbd_list)]
        
        alloc_df = alloc_df.sort_values(by='jumpingRBDPrice').reset_index(drop=True)
        tbf_rbds = tbf_rbds.sort_values(by='Total').reset_index(drop=True)
        remaining_tbf_rbds = tbf_rbds.copy()
        latest_allocations = {}
        desired_order = ['TF', 'TE', 'TD', 'TC', 'TB', 'TY']
        default_values = {
        'index': 'NA',
        'min_other_diff': 0
    }
    
    # Create new dictionary with desired order
        result = {}
        if int(tbfFlag) == 1:
            discounted_df=self.rmdatabasehelper.get_tbf_discount(month,sector)
            for idx, alloc_row in alloc_df.iterrows():
                jumping_price = alloc_row['price']
                incremental = alloc_row['incremental']
                
                # Calculate differences with all TBF RBDs
                differences = abs(tbf_rbds['Total'] - jumping_price)
                min_diff = differences.min()
                nearest_idx = differences.idxmin()
                nearest_rbd = tbf_rbds.loc[nearest_idx, 'RBD']
                nearest_total = tbf_rbds.loc[nearest_idx, 'Total']
                
                # Check if this jumping price has the closest match to any TBF RBD
                # Compare with all other jumping prices
                other_prices_diff = abs(alloc_df['price'] - nearest_total)
                min_other_diff = other_prices_diff.min()
                
                # If this is the closest match for the TBF RBD
                    # Update the latest allocation for this class_of_service
                latest_allocations[nearest_rbd] = {
                    'index': idx,
                    'min_other_diff': min_other_diff
                }

            if not discounted_df.empty:
                # Get the first allocated RBD (will be one with lowest Total based on sorting)
                first_allocated_rbd = None
                for rbd in desired_order:
                    if rbd in latest_allocations and latest_allocations[rbd]['index'] != 'NA':
                        first_allocated_rbd = rbd
                        break
                
                if first_allocated_rbd:
                    # Get discount for this RBD from discounted_df
                    if first_allocated_rbd in discounted_df.columns:
                        discount_amount = discounted_df[first_allocated_rbd].iloc[0]
                        
                        # Get the original price
                        idx = latest_allocations[first_allocated_rbd]['index']
                        original_price = openingRBDPrice
                        
                        # Calculate new price after discount
                        adjusted_price = original_price - discount_amount
                        
                        # Find the next highest RBD based on the adjusted price
                        next_rbd_differences = tbf_rbds['Total'] - adjusted_price
                        next_rbd_differences = next_rbd_differences[next_rbd_differences > 0]  # Only higher prices
                        
                        if not next_rbd_differences.empty:
                            # Find all subsequent RBDs in sequence
                            remaining_tbf = tbf_rbds.copy()
                            current_price = adjusted_price
                            
                            while not remaining_tbf.empty:
                                # Find the next nearest RBD above current_price
                                next_rbd_diffs = remaining_tbf['Total'] - current_price
                                next_rbd_diffs = next_rbd_diffs[next_rbd_diffs > 0]  # Only higher prices
                                
                                if not next_rbd_diffs.empty:
                                    next_nearest_idx = next_rbd_diffs.idxmin()
                                    next_nearest_rbd = remaining_tbf.loc[next_nearest_idx, 'RBD']
                                    next_rbd_total = remaining_tbf.loc[next_nearest_idx, 'Total']
                                    
                                    # Find the index in alloc_df that's closest to this next RBD's Total
                                    next_alloc_diff = abs(alloc_df['price'] - next_rbd_total)
                                    next_alloc_idx = next_alloc_diff.idxmin()
                                    
                                    # Update the allocation for this RBD
                                    latest_allocations[next_nearest_rbd] = {
                                        'index': next_alloc_idx,
                                        'min_other_diff': next_alloc_diff.min()
                                    }
                                    
                                    # Update current_price and remove this RBD from remaining_tbf
                                    current_price = next_rbd_total
                                    remaining_tbf = remaining_tbf[remaining_tbf['RBD'] != next_nearest_rbd]
                                else:
                                    # No more higher-priced RBDs
                                    break
                        else:
                            latest_allocations={}    
            for key in desired_order:
                if key in latest_allocations:
                    result[key] = latest_allocations[key]
                else:
                    result[key] = default_values.copy()
            last_index = None
            for key, value in result.items():
                if value['index'] == 'NA':
                    # If index is 'NA', replace it with the last known non-'NA' index
                    value['index'] = last_index
                else:
                    # Update the last known non-'NA' index
                    last_index = value['index']
            for nearest_rbd, allocation in result.items():
                idx=allocation['index']
                currRec.set_class_attribute(brd_counter, 'class_of_service', nearest_rbd)
                if idx is not None:
                    incremental_value = int(alloc_df.loc[idx, 'incremental'])
                
                    # Check if difference between price and openingRBDPrice is less than 1000
                    class_au = min(incremental_value, max_au)
                    
                else:
                    class_au = 0
                currRec.set_class_attribute(brd_counter, 'class_au', class_au)
                brd_counter += 1
            # for _, tbf_row in tbf_rbds.iterrows():
            #     rbd = tbf_row['RBD']
            #     if rbd not in latest_allocations:
            #         # Find the next available index in alloc_df
            #         next_index = alloc_df.index[-1] + 1 if not alloc_df.empty else 0
            #         currRec.set_class_attribute(brd_counter, 'class_of_service', rbd)
            #         currRec.set_class_attribute(brd_counter, 'class_au', 0)
            #         brd_counter += 1
                
        else:
            for RBDLabel in rbd_list:
                currRec.set_class_attribute(brd_counter, 'class_of_service', RBDLabel)
                currRec.set_class_attribute(brd_counter, 'class_au', -1)
                brd_counter += 1

    


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
