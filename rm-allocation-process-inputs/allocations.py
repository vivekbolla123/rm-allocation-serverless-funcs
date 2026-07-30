import ast
import copy
from datetime import datetime
import json
import math

import numpy as np
from Constants import *
from common.compute_utils import calculate_profilefare_quartile, calculate_variance_for_load, calculate_group_adjusted_load, calculate_group_fare_anchor
import pandas as pd


class RBDAllocations:

    def __init__(self, rmdatabasehelper, logger, constantsutils, mathutils, timeutils=None):
        self.rmdatabasehelper = rmdatabasehelper
        self.logger = logger
        self.constantsutils = constantsutils
        self.mathutils = mathutils
        self.timeutils = timeutils

    def b2CAllocation(self, currBookedLoad, openDataB2C, currRec, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter, currParams, profileFare, profileExtract, bookloadPercentile, upsellValue, imp_forecast,startegyfare,hard_anchor_fare,plfthreshold,bookedload, overBookingFare=None, adjustedCapacity=None,is_spoilage_reduction_active=False, last_seven_avg_fare=None, b2c_backstop_final=None):
        # If adjustedCapacity is None, use totalCapacity as the base
        baseCapacity = adjustedCapacity if adjustedCapacity is not None else totalCapacity
        
        if isinstance(plfthreshold, str):
            plfthreshold = plfthreshold.replace('%','')
        # Calculate overbooking seats (if any)
        overBookingSeats = totalCapacity - baseCapacity if adjustedCapacity is not None else 0
        
        b2cAllocationsUnits = []
        B2CRBDs = self.rmdatabasehelper.fareExtract(currParams.origin, currParams.destin, B2C_CHANNEL, '', 'L')
        
        farecap = getattr(currParams, 'farecap', None)
        cappedRBDIndex = -1
        if farecap is not None and farecap != CONST_NA and str(farecap).strip() != "":
            try:
                farecap_val = float(farecap)
                valid_rbds = B2CRBDs[B2CRBDs['TOTAL'] < farecap_val]
                if not valid_rbds.empty:
                    cappedRBDIndex = valid_rbds.index[-1]
            except ValueError:
                pass
                
        fullRbds=B2CRBDs
        # rbd_list = self.constantsutils.TBF_RBD.split(',')+self.constantsutils.CUG_RBD.split(',')
        # rbd_list.extend(list(json.loads(GROUP_QUOTATION).values()))
        # B2CRBDs = B2CRBDs[~B2CRBDs['RBD'].isin(rbd_list)].reset_index(drop=True)
        B2CRBDs['id'] = B2CRBDs.reset_index().index + 1
        maxRBDIndex = B2CRBDs['id'].iloc[-1]
        maxRBDPrice = B2CRBDs['TOTAL'].iloc[-1]
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
        jumpPercent = float(currParams.skipping_factor) if currParams.skipping_factor else 0
        counter = 0
        status = CONST_NA
        first_rbd_alloc = CONST_NA
        other_rbd_alloc = CONST_NA
        # reset
        # Loop Through
        jumpingRBDPrice_list = []
        incremental_list = []
        rbdlabel_list=[]
        price_list=[]
        
        # Determine the overbooking RBD index if overBookingFare is provided
        obRBDIndex = -1
        if overBookingFare is not None and overBookingFare > 0 and overBookingSeats > 0:
            # Find the closest RBD to the overbooking fare
            B2CRBDs['FareDiff'] = abs(B2CRBDs['TOTAL'] - overBookingFare)
            obRBDIndex = B2CRBDs['FareDiff'].idxmin()
            B2CRBDs = B2CRBDs.drop('FareDiff', axis=1)
        
        if upsellValue is None or str(upsellValue).strip() == '' or str(upsellValue).strip().upper() == 'NA':
            upsellValue = 0
        
        while b2cctr < B2CRBDs.shape[0] or b2bctr < self.constantsutils.B2B_RDB_COUNT:
            if b2cctr < B2CRBDs.shape[0]:
                RBDLabel = B2CRBDs['RBD'][b2cctr]
                remainingRBD = maxRBDIndex - openRBDIndex
                firstRDBAllocation = int(currParams.first_rbd_alloc)
                otherRDBAllocation = int(currParams.other_rbd_alloc)
                price = B2CRBDs['TOTAL'][b2cctr]
                b2bFactor = float(currParams.b2b_factor)
                nextPrice = price
                if size > b2cctr:
                    nextPrice = B2CRBDs['TOTAL'][b2cctr + 1]
                currRec.set_class_attribute(b2cctr + 1, 'class_of_service', RBDLabel)
                if RBDLabel == openingRBD:
                    openbuketFlag = True
                    openingRBDPrice = B2CRBDs['TOTAL'][b2cctr]
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
                # Step 1: Normal allocation using baseCapacity
                openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b, counter, status, bookloadPercentile, first_rbd_alloc_value, other_rbd_alloc_value = self.getIncrementalValue(
                    currBookedLoad, baseCapacity, bookedDeficit, dPLF, 
                    remainingCapacity, ndocounter, currParams, profileFare, 
                    maxRBDPrice, openingRBDPrice, openRBDIndex, incremental, 
                    incrementalb2b, jumpingRBDPrice, entryRDB, d3Allocation, 
                    jumpPercent, remainingRBD, firstRDBAllocation, otherRDBAllocation, 
                    price, b2bFactor, nextPrice, newProfileFares, counter, bookloadPercentile, imp_forecast
                )

                if first_rbd_alloc == CONST_NA:
                    first_rbd_alloc = first_rbd_alloc_value
                    initial_incremental = incremental
                if other_rbd_alloc == CONST_NA and initial_incremental != incremental:
                    other_rbd_alloc = other_rbd_alloc_value

                # Ensure we don't exceed base capacity in the normal allocation
                if incremental > baseCapacity:
                    incremental = baseCapacity

                if incrementalb2b > baseCapacity:
                    incrementalb2b = baseCapacity

                # Step 2: Apply overbooking for RBDs at or above the threshold
                if obRBDIndex != -1 and b2cctr >= obRBDIndex and overBookingSeats > 0:
                    # Add overbooking seats to allocation for RBDs at or above the threshold
                    incremental += overBookingSeats
                    incrementalb2b += overBookingSeats
                    status = f"Normal allocation ({incremental - overBookingSeats}) + Overbooking ({overBookingSeats}) for RBD {RBDLabel}"
                   
                if cappedRBDIndex != -1:
                    if b2cctr == cappedRBDIndex:
                        incremental = totalCapacity
                        incrementalb2b = totalCapacity
                    elif b2cctr > cappedRBDIndex:
                        incremental = 0
                        incrementalb2b = 0
                elif b2cctr == B2CRBDs.shape[0]-1:
                    incremental = totalCapacity
                
                # Apply virtual nesting (ensure AU isn't strictly decreasing)
                # Skip nesting override if we're purposefully closing the subclass due to farecap
                is_above_capped = (cappedRBDIndex != -1 and b2cctr > cappedRBDIndex)
                if not is_above_capped and currRec.get_class_attribute(b2cctr,'class_au') is not None:
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
        if hard_anchor_fare !="" and  pd.notna(hard_anchor_fare) and hard_anchor_fare!=0 and hard_anchor_fare !="NA" :
            self.logger.info("Hard Anchor Fare detected. Skipping Spoilage Reduction override.")
            try:
                plfthreshold = int(plfthreshold)
                # 1. Find the RBD closest to the last seven sold average fare
                B2CRBDs['FareDiff'] = B2CRBDs['TOTAL'] - float(hard_anchor_fare)
                higher_fares  = B2CRBDs[B2CRBDs['FareDiff'] >= 0]
                target_rbd_df_index = higher_fares['FareDiff'].idxmin()
                
                opening_rbd_df_index = B2CRBDs.loc[B2CRBDs['RBD'] == openingRBD].index[0]

                # Ensure the system isn't already opening at a price point higher than the target
                if opening_rbd_df_index == target_rbd_df_index and bookedload<plfthreshold:
                    self.logger.info("Spoilage Reduction: System opening RBD is higher than Last 7 Sold RBD. Applying 97% allocation at target RBD.")
            
                    # Calculate 97% of total capacity for the target RBD
                    ninety_seven_percent_seats = max(round(totalCapacity * (plfthreshold/100)), currBookedLoad+first_rbd_alloc)
                    remaining_three_percent_seats = totalCapacity - ninety_seven_percent_seats
                    
                    # Find the next level RBD (one level higher in price than target)
                    next_level_rbd_df_index = target_rbd_df_index + 1 if target_rbd_df_index + 1 < B2CRBDs.shape[0] else target_rbd_df_index
                    
                    # Override allocations in currRec
                    status = "D-0 Spoilage Reduction - 97% at Target RBD"
                    au_at_next_rbds = ninety_seven_percent_seats
                    # Initialize jumping price for subsequent allocations
                    target_price = B2CRBDs['TOTAL'].iloc[target_rbd_df_index]
                    jumping_price = target_price + (target_price * (jumpPercent / 100))
                    
                    # Set allocations for all RBDs
                    for i in range(B2CRBDs.shape[0]):
                        au_class_index_to_update = i + 1
                        current_price = B2CRBDs['TOTAL'].iloc[i]
                        next_price = B2CRBDs['TOTAL'].iloc[i + 1] if i + 1 < B2CRBDs.shape[0] else current_price
                        
                        if i < target_rbd_df_index:
                            # For RBDs below target: set to current booked load (restrictive)
                            currRec.set_class_attribute(au_class_index_to_update, 'class_au', 0)
                        elif i == target_rbd_df_index:
                            # At target RBD: allocate plfthreshold% of total capacity
                            currRec.set_class_attribute(au_class_index_to_update, 'class_au', int(ninety_seven_percent_seats))
                        else:
                            # Apply jumping logic - only increment allocation when price meets jumping threshold
                            next_price_diff = abs(next_price - jumping_price)
                            curr_price_diff = abs(current_price - jumping_price)
                            
                            if jumping_price >= next_price:
                                # Jumping price is higher than next RBD, skip allocation increment
                                pass
                            elif curr_price_diff <= next_price_diff:
                                # Current price is closer to jumping price, allocate and update jumping price
                                au_at_next_rbds = au_at_next_rbds + other_rbd_alloc_value
                                jumping_price = current_price + (current_price * (jumpPercent / 100))
                            # else: skip allocation increment for this RBD
                            
                            # For the final RBD, allocate all remaining seats
                            if i == B2CRBDs.shape[0] - 1:
                                currRec.set_class_attribute(au_class_index_to_update, 'class_au', int(totalCapacity))
                            else:
                                # Set the current AU level for this RBD
                                currRec.set_class_attribute(au_class_index_to_update, 'class_au', int(min(au_at_next_rbds, totalCapacity)))
                        
                    
                elif opening_rbd_df_index < target_rbd_df_index and bookedload<plfthreshold:
                    # 2. Determine the number of seats allocated normally *before* the target RBD
                    allocated_seats_before_target = 0
                    if target_rbd_df_index > 0:
                        # The AU index in currRec is the DataFrame index + 1. We need the one *before* our target.
                        au_index_before_target = target_rbd_df_index 
                        allocated_seats_before_target = currRec.get_class_attribute(au_index_before_target, 'class_au')
                        # If the value is None (should not happen but good practice), default to current booked load
                        if allocated_seats_before_target is None:
                            allocated_seats_before_target = currBookedLoad
                    else:
                        # If the target RBD is the first one, the base is simply the current booked load
                        allocated_seats_before_target = currBookedLoad

                    # 3. Calculate the pool of seats to be distributed from this point forward
                    seats_to_distribute = totalCapacity 

                    if seats_to_distribute > 0:
                        # 4. Calculate the 97% split based on this *correct* pool of seats
                        ninety_seven_percent_seats = round(seats_to_distribute * (plfthreshold/100))
                        
                        # The new AU values are cumulative, built upon the allocation of the previous class
                        au_at_target_rbd=max(ninety_seven_percent_seats,allocated_seats_before_target+other_rbd_alloc_value)
                        # The rest will open up fully

                        # 5. Override allocations in currRec from the target RBD onwards
                        status = "D-0 Spoilage Reduction" # Update the status message
                        au_at_next_rbds = au_at_target_rbd
                        # Initialize jumping price for subsequent allocations
                        target_price = B2CRBDs['TOTAL'].iloc[target_rbd_df_index]
                        jumping_price = target_price + (target_price * (jumpPercent / 100))
                        
                        # Iterate through the DataFrame from the target index to the end
                        for i in range(target_rbd_df_index, B2CRBDs.shape[0]):
                            
                            au_class_index_to_update = i + 1
                            current_price = B2CRBDs['TOTAL'].iloc[i]
                            next_price = B2CRBDs['TOTAL'].iloc[i + 1] if i + 1 < B2CRBDs.shape[0] else current_price
                            
                            if i == target_rbd_df_index:
                                # At the target RBD, dump plfthreshold% of the remaining seats
                                currRec.set_class_attribute(au_class_index_to_update, 'class_au', int(min(au_at_target_rbd, totalCapacity)))
                            else:
                                # Apply jumping logic - only increment allocation when price meets jumping threshold
                                next_price_diff = abs(next_price - jumping_price)
                                curr_price_diff = abs(current_price - jumping_price)
                                
                                if jumping_price >= next_price:
                                    # Jumping price is higher than next RBD, skip allocation increment
                                    pass
                                elif curr_price_diff <= next_price_diff:
                                    # Current price is closer to jumping price, allocate and update jumping price
                                    au_at_next_rbds = au_at_next_rbds + other_rbd_alloc_value
                                    jumping_price = current_price + (current_price * (jumpPercent / 100))
                                # else: skip allocation increment for this RBD
                                
                                # For the final RBD, allocate all remaining seats
                                if i == B2CRBDs.shape[0] - 1:
                                    currRec.set_class_attribute(au_class_index_to_update, 'class_au', int(totalCapacity))
                                else:
                                    # For all subsequent (more expensive) RBDs, set the current AU level
                                    currRec.set_class_attribute(au_class_index_to_update, 'class_au', int(min(au_at_next_rbds, totalCapacity)))
                    else:
                        self.logger.info("Spoilage Reduction: No remaining seats to distribute from the target RBD onwards.")

            except Exception as e:
                self.logger.error(f"Error during spoilage reduction override: {e}")
        
        if is_spoilage_reduction_active and pd.notna(last_seven_avg_fare):
            self.logger.info("Applying D-0 Spoilage Reduction override.")
            try:
                param_b2c_backstop_full = str(b2c_backstop_final if b2c_backstop_final is not None and b2c_backstop_final != CONST_NA else getattr(currParams, 'b2c_backstop', CONST_NA))
                b2c_backstop_val = 0
                if param_b2c_backstop_full != CONST_NA and param_b2c_backstop_full.strip() != "":
                    try:
                        b2c_backstop_val = float(param_b2c_backstop_full)
                    except ValueError:
                        pass

                # 1. Find the RBD closest to the last seven sold average fare
                target_fare = float(max(last_seven_avg_fare,0.75*float(startegyfare),b2c_backstop_val))
                B2CRBDs['FareDiff'] = B2CRBDs['TOTAL'] - target_fare
                higher_fares  = B2CRBDs[B2CRBDs['FareDiff'] > 0]
                target_rbd_df_index = higher_fares['FareDiff'].idxmin()
                
                opening_rbd_df_index = B2CRBDs.loc[B2CRBDs['RBD'] == openingRBD].index[0]

                # Ensure the system isn't already opening at a price point higher than the target
                if opening_rbd_df_index > target_rbd_df_index:
                    self.logger.info("Spoilage Reduction: System opening RBD is higher than Last 7 Sold RBD. Applying 97% allocation at target RBD.")
            
                    # Calculate 97% of total capacity for the target RBD
                    ninety_seven_percent_seats = round(totalCapacity * 0.97)
                    remaining_three_percent_seats = totalCapacity - ninety_seven_percent_seats
                    
                    # Find the next level RBD (one level higher in price than target)
                    next_level_rbd_df_index = target_rbd_df_index + 1 if target_rbd_df_index + 1 < B2CRBDs.shape[0] else target_rbd_df_index
                    
                    # Override allocations in currRec
                    status = "D-0 Spoilage Reduction - 97% at Target RBD"
                    
                    target_rbd_label = B2CRBDs['RBD'].iloc[target_rbd_df_index]
                    target_rbd_fare = B2CRBDs['TOTAL'].iloc[target_rbd_df_index]
                    
                    if isinstance(openDataB2C.get('RBD'), list) and len(openDataB2C['RBD']) > 0:
                        openDataB2C['RBD'][0] = target_rbd_label
                    if isinstance(openDataB2C.get('Fare'), list) and len(openDataB2C['Fare']) > 0:
                        openDataB2C['Fare'][0] = target_rbd_fare
                    if isinstance(openDataB2C.get('FareResult'), list) and len(openDataB2C['FareResult']) > 0:
                        openDataB2C['FareResult'][0] = str(openDataB2C['FareResult'][0]) + " Spoilage Reduction"
                        
                    openingRBDPrice = target_rbd_fare
                    
                    # Set allocations for all RBDs
                    for i in range(B2CRBDs.shape[0]):
                        au_class_index_to_update = i + 1
                        
                        if i < target_rbd_df_index:
                            # For RBDs below target: set to current booked load (restrictive)
                            currRec.set_class_attribute(au_class_index_to_update, 'class_au', 0)
                        elif i == target_rbd_df_index:
                            # At target RBD: allocate 97% of total capacity
                            currRec.set_class_attribute(au_class_index_to_update, 'class_au', int(ninety_seven_percent_seats))
                        elif i == next_level_rbd_df_index and i != target_rbd_df_index:
                            # At next level RBD: allocate remaining 3% (cumulative, so 97% + 3% = 100%)
                            currRec.set_class_attribute(au_class_index_to_update, 'class_au', int(totalCapacity))
                        else:
                            # For all other higher RBDs: open up fully
                            currRec.set_class_attribute(au_class_index_to_update, 'class_au', int(totalCapacity))
                    
                else:
                    # 2. Determine the number of seats allocated normally *before* the target RBD
                    allocated_seats_before_target = 0
                    if target_rbd_df_index > 0:
                        # The AU index in currRec is the DataFrame index + 1. We need the one *before* our target.
                        au_index_before_target = target_rbd_df_index 
                        allocated_seats_before_target = currRec.get_class_attribute(au_index_before_target, 'class_au')
                        # If the value is None (should not happen but good practice), default to current booked load
                        if allocated_seats_before_target is None:
                            allocated_seats_before_target = currBookedLoad
                    else:
                        # If the target RBD is the first one, the base is simply the current booked load
                        allocated_seats_before_target = currBookedLoad

                    # 3. Calculate the pool of seats to be distributed from this point forward
                    seats_to_distribute = totalCapacity - allocated_seats_before_target

                    if seats_to_distribute > 0:
                        # 4. Calculate the 97% split based on this *correct* pool of seats
                        ninety_seven_percent_seats = round(seats_to_distribute * 0.97)
                        
                        # The new AU values are cumulative, built upon the allocation of the previous class
                        au_at_target_rbd = allocated_seats_before_target + ninety_seven_percent_seats
                        au_at_next_rbds = totalCapacity # The rest will open up fully

                        # 5. Override allocations in currRec from the target RBD onwards
                        status = "D-0 Spoilage Reduction" # Update the status message
                        if isinstance(openDataB2C.get('FareResult'), list) and len(openDataB2C['FareResult']) > 0:
                            openDataB2C['FareResult'][0] = str(openDataB2C['FareResult'][0]) + " Spoilage Reduction"
                        
                        # Iterate through the DataFrame from the target index to the end
                        for i in range(target_rbd_df_index, B2CRBDs.shape[0]):
                            au_class_index_to_update = i + 1
                            if i == target_rbd_df_index:
                                # At the target RBD, dump 97% of the remaining seats
                                currRec.set_class_attribute(au_class_index_to_update, 'class_au', int(au_at_target_rbd))
                            else:
                                # For all subsequent (more expensive) RBDs, open up fully
                                currRec.set_class_attribute(au_class_index_to_update, 'class_au', int(au_at_next_rbds))
                    else:
                        self.logger.info("Spoilage Reduction: No remaining seats to distribute from the target RBD onwards.")

            except Exception as e:
                self.logger.error(f"Error during spoilage reduction override: {e}")
        if cappedRBDIndex != -1:
            # Ensure the capped RBD (final valid RBD under farecap) gets all seats
            currRec.set_class_attribute(cappedRBDIndex + 1, 'class_au', int(totalCapacity))
            for i in range(cappedRBDIndex + 1, B2CRBDs.shape[0]):
                currRec.set_class_attribute(i + 1, 'class_au', 0)
                
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

        return b2cAllocationsUnits, b2cctr + 1, status, df_unique, fullRbds, openingRBDPrice, first_rbd_alloc, other_rbd_alloc

    def b2CAllocations(self, currBookedLoad, openDataB2C, currRec, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter, currParams, profileFare, profileExtract, bookloadPercentile, upsellValue, openingRBDPrice, imp_forecast,strategyfare,hard_anchor_fare,plfthreshold,bookload, overBookingFare=None, adjustedCapacity=None,is_spoilage_reduction_active=False, last_seven_avg_fare=None, b2c_backstop_final=None):
        # Store the original rbdPushFlag
        
        # Create a copy of currParams to modify
        currRec_copy = copy.deepcopy(currRec)
        
        # First allocation with rbdPushFlag = 0
        currParams.rbdPushFlag = 0
        _, _, _, df_unique_0, _, _, _, _ = self.b2CAllocation(
            currBookedLoad, openDataB2C, currRec_copy, totalCapacity, bookedDeficit, dPLF, 
            remainingCapacity, ndocounter, currParams, profileFare, profileExtract, 
            bookloadPercentile, upsellValue, imp_forecast,strategyfare,hard_anchor_fare,plfthreshold,bookload, overBookingFare, adjustedCapacity,is_spoilage_reduction_active, last_seven_avg_fare, b2c_backstop_final
        )
        
        currRec_copy = copy.deepcopy(currRec)
        
        # Second allocation with rbdPushFlag = 1
        currParams.rbdPushFlag = 1
        _, _, _, df_unique_1, _, _, _, _ = self.b2CAllocation(
            currBookedLoad, openDataB2C, currRec_copy, totalCapacity, bookedDeficit, dPLF, 
            remainingCapacity, ndocounter, currParams, profileFare, profileExtract, 
            bookloadPercentile, upsellValue, imp_forecast,strategyfare,hard_anchor_fare,plfthreshold,bookload, overBookingFare, adjustedCapacity,is_spoilage_reduction_active, last_seven_avg_fare, b2c_backstop_final
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

    def getIncrementalValue(self, currBookedLoad, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter, currParams, profileFare, maxRBDPrice, openingRBDPrice, openRBDIndex, incremental, incrementalb2b, jumpingRBDPrice, entryRDB, d3Allocation, jumpPercent, remainingRBD, firstRDBAllocation, otherRDBAllocation, price, b2bFactor, nextPrice, profileExtract, counter, bookloadPercentile, imp_forecast):
        first_rbd_alloc = firstRDBAllocation
        other_rbd_alloc = otherRDBAllocation
        if int(currParams.rbdPushFlag) == 0:
            status = 'Normal AU allocation'
            openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b, first_rbd_alloc, other_rbd_alloc = self.calculateD3D4Values(currBookedLoad, bookedDeficit, dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPrice, openRBDIndex, incremental, incrementalb2b, jumpingRBDPrice, entryRDB, d3Allocation, jumpPercent, remainingRBD, firstRDBAllocation, otherRDBAllocation, price, b2bFactor, nextPrice, imp_forecast)
        if int(currParams.rbdPushFlag) == 1:
            if profileFare == CONST_NA or profileFare == 0:
                status = 'Normal AU allocation-profileFare NA'
                if int(currParams.profileFares) == 0:
                    status = 'Normal AU allocation-profileFare skipped'
                openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b, first_rbd_alloc, other_rbd_alloc = self.calculateD3D4Values(currBookedLoad, bookedDeficit, dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPrice, openRBDIndex, incremental, incrementalb2b, jumpingRBDPrice, entryRDB, d3Allocation, jumpPercent, remainingRBD, firstRDBAllocation, otherRDBAllocation, price, b2bFactor, nextPrice, imp_forecast)
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

        return openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b, counter, status, bookloadPercentile, first_rbd_alloc, other_rbd_alloc

    def calculateD3D4Values(self, currBookedLoad, bookedDeficit, dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPrice, openRBDIndex, incremental, incrementalb2b, jumpingRBDPrice, entryRDB, d3Allocation, jumpPercent, remainingRBD, firstRDBAllocation, otherRDBAllocation, price, b2bFactor, nextPrice, imp_forecast):
        first_rbd_alloc = firstRDBAllocation
        other_rbd_alloc = otherRDBAllocation
        # Positive Allocation
        if firstRDBAllocation >= 0:
            openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, firstRDBAllocation, otherRDBAllocation, jumpingRBDPrice, b2bFactor, incrementalb2b)

        # Negative -99 Allocation
        elif firstRDBAllocation == DYNAMIC_D3_D4 or (firstRDBAllocation == FORECAST_ALLOCATION and (pd.isna(imp_forecast) or imp_forecast in (CONST_NA, ''))):
            openRBDIndex, entryRDB, d3Allocation, d4Allocation, remainingRBD = self.d3d4Calculations(dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPrice, openRBDIndex, entryRDB, remainingRBD, d3Allocation, jumpPercent)
            openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, d3Allocation, d4Allocation, jumpingRBDPrice, b2bFactor, incrementalb2b)
            first_rbd_alloc = d3Allocation
            other_rbd_alloc = d4Allocation

        # Negative -1 Allocation
        elif firstRDBAllocation == FORECAST_ALLOCATION:
            openRBDIndex, entryRDB, d3Allocation, d4Allocation, remainingRBD = self.d3d4_forecast_calculations(dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPrice, openRBDIndex, entryRDB, remainingRBD, d3Allocation, jumpPercent, imp_forecast)
            openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, d3Allocation, d4Allocation, jumpingRBDPrice, b2bFactor, incrementalb2b)
            first_rbd_alloc = d3Allocation
            other_rbd_alloc = d4Allocation

        return openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b, first_rbd_alloc, other_rbd_alloc

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

    def d3d4_forecast_calculations(self, dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPrice, openRBDIndex, entryRDB, remainingRBD, d3Allocation, jumpPercent, imp_forecast):
        maxLimit = self.mathutils.generateMaxValue(remainingCapacity, ndocounter)
        if entryRDB:
            if jumpPercent > 0 and openingRBDPrice - maxRBDPrice != 0:
                remainingRBD = self.mathutils.calculateLogValueForD3(maxRBDPrice, openingRBDPrice, jumpPercent)

            DEFAULT_D3_VALUE = self.constantsutils.DEFAULT_D3_VALUE
            D3_MIN_RULES = self.constantsutils.D3_MIN_RULES
            D3_MAX_RULES = self.constantsutils.D3_MAX_RULES

            d3Allocation = DEFAULT_D3_VALUE
            if imp_forecast != CONST_NA:
                try:
                    forecast_factor = self.mathutils.calc_forecast_factor(imp_forecast)
                    d3Allocation = self.mathutils.calculate_forecast_d3(
                        forecast_factor, remainingCapacity, remainingRBD, ndocounter
                    )

                    # ---------- MIN RULE ----------
                    min_d3_value = None
                    for rule in D3_MIN_RULES.values():
                        if rule['forecast_start'] <= imp_forecast <= rule['forecast_end']:
                            min_d3_value = rule['min_value']
                            break

                    if min_d3_value is not None:
                        d3Allocation = round(max(d3Allocation, min_d3_value))
                    else:
                        d3Allocation = DEFAULT_D3_VALUE

                    # ---------- MAX RULE ----------
                    max_band = None
                    max_value = None
                    for rule_name, rule in D3_MAX_RULES.items():
                        if rule['forecast_start'] <= imp_forecast <= rule['forecast_end']:
                            if 'ndo_start' in rule and 'ndo_end' in rule:
                                if not (rule['ndo_start'] <= ndocounter <= rule['ndo_end']):
                                    continue
                            max_band = rule_name
                            max_value = rule['max_value']
                            break

                    if max_band == 'L':
                        max_d3_value = max(remainingCapacity / (1 + ndocounter), max_value)
                    elif max_band == 'M':
                        max_d3_value = max(remainingCapacity, max_value)
                    elif max_band == 'H1':
                        max_d3_value = max(0.2 * remainingCapacity, max_value)
                    elif max_band == 'H2':
                        max_d3_value = max(remainingCapacity / (1 + ndocounter), max_value)
                    else:
                        max_d3_value = None

                    if max_d3_value is not None:
                        d3Allocation = round(min(d3Allocation, max_d3_value))
                    else:
                        d3Allocation = DEFAULT_D3_VALUE

                except Exception as e:
                    self.logger.error(f'Defaulting to default D3 value {DEFAULT_D3_VALUE}, error: {e}')
                    d3Allocation = DEFAULT_D3_VALUE
            
            entryRDB = False

        openRBDIndex = openRBDIndex + 1

        d4Allocation = self.mathutils.dynamicAllocationD4Formula(dPLF, openRBDIndex, d3Allocation)

        if d4Allocation <= self.constantsutils.MIN_D3_D4_VALUE:
            d4Allocation = self.constantsutils.MIN_D3_D4_VALUE
        elif d4Allocation >= maxLimit:
            d4Allocation = maxLimit

        return openRBDIndex, entryRDB, d3Allocation, d4Allocation, remainingRBD
    
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

    def b2BAllocation(self, currBookedLoad, openDataB2B, currRec, totalCapacity, dPLF, b2cAllocationsUnits, b2bSkipFlag, b2boverbookingflag, brd_counter, openDataB2C, currParams,overBookingCount,B2B_thresholdflag, close_b2b):
        isUpsellFlag = False
        B2BRBDs = self.rmdatabasehelper.fareExtract(currParams.origin, currParams.destin, B2B_CHANNEL, '', 'L')

        farecap = getattr(currParams, 'farecap', None)
        cappedRBDIndex = -1
        if farecap is not None and farecap != CONST_NA and str(farecap).strip() != "":
            try:
                farecap_val = float(farecap)
                valid_rbds = B2BRBDs[B2BRBDs['TOTAL'] < farecap_val]
                if not valid_rbds.empty:
                    cappedRBDIndex = valid_rbds.index[-1]
            except ValueError:
                pass

        openingB2CRBD = openDataB2C['RBD'][0]

        B2CRBDs = self.rmdatabasehelper.fareExtract(currParams.origin, currParams.destin, B2C_CHANNEL, openingB2CRBD, 'L')

        openingB2CPrice = self.constantsutils.B2B_FARE_PRICE_COMPARISON * B2CRBDs['TOTAL'][0]

        openbuketFlag = False
        openingRBD = openDataB2B['RBD'][0]

        PLF = currBookedLoad / totalCapacity
        counter = 0
        # Loop Through

        for b2bctr in range(B2BRBDs.shape[0]):
            RBDLabel = B2BRBDs['RBD'][b2bctr]
            price = B2BRBDs['TOTAL'][b2bctr]
            b2bStatus = openDataB2B['statusInd'][0]
            currRec.set_class_attribute(brd_counter, 'class_of_service', RBDLabel)
            if overBookingCount>0 or B2B_thresholdflag:
                currRec.set_class_attribute(brd_counter, 'class_au', 0)
            else:
                if b2bSkipFlag:
                    currRec.set_class_attribute(brd_counter, 'class_au', -1)
                elif close_b2b:
                    currRec.set_class_attribute(brd_counter, 'class_au', 0)
                else:
                    if RBDLabel == openingRBD:
                        openbuketFlag = True
                        openingRBDPrice = price

                    if cappedRBDIndex != -1 and b2bctr > cappedRBDIndex:
                        currRec.set_class_attribute(brd_counter, 'class_au', 0)
                        if openbuketFlag and ((b2bStatus == 0) or (b2bStatus == 2)):
                            counter = counter + 1
                    elif openbuketFlag:
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
                            currRec.set_class_attribute(brd_counter, 'class_au', 0)
                        else:
                            currRec.set_class_attribute(brd_counter, 'class_au', 0)
            brd_counter = brd_counter+1

        return isUpsellFlag, brd_counter
    
    def group_quote_alloc(self, brd_counter, currRec, autoGroup, adjustedCapacity, upsell, B2CRBDs, openingRBDPrice, groupbookload, currBookedLoad, month, sector, deptTime_str_hhmm, dow_str_for_profile, final_totalCapacity, variance_full, b2c_selling_fare, b2c_backstop_final, ndocounter=0, imp_forecast=None, sod_tgtAtCurrentHour_abs=0, sod_startOfDayBookLoad_factor=0, sod_startOfDayTgtBookedLoad_abs=0, departure_year=None):
        """
        Group quotation allocation using iterative fare anchor approach.
        
        For every batch of 5 seats:
        1. Calculate adjusted current load (using variance*scaling or imputed forecast - 95)
        2. Get profile fare quartile and profile fare value
        3. Apply increment factor from group_profile_fare_increments collection
        4. Calculate discounted B2C selling fare using group_b2c_fare_discounts
        5. Determine fare anchor: MIN(adj profile, disc B2C) for bands 0-2, MAX for bands 3+
        6. Assign 5 seats to nearest higher group RBD
        7. Repeat with +5 booked load until fare anchor > GA (highest RBD) or capacity reached
        """

        rbd_list = list(json.loads(GROUP_QUOTATION).values())
        group_alloc_df = pd.DataFrame()
        if not B2CRBDs.empty:
            group_alloc_df = B2CRBDs[B2CRBDs['RBD'].isin(rbd_list)]
            group_alloc_df = group_alloc_df.sort_values(by='TOTAL').reset_index(drop=True)
        rbd_map = ast.literal_eval(self.constantsutils.GROUP_QUOTATION)

        # First iteration metrics for output summary
        first_iter_adjusted_profile_fare = CONST_NA
        first_iter_adjusted_plf = CONST_NA
        first_iter_fare_anchor = CONST_NA

        if int(autoGroup) == 1 and not group_alloc_df.empty:
            try:
                profilefarextract = self.rmdatabasehelper.profileFaresExtract(
                    origin=sector[:3],
                    destin=sector[3:],
                    month=month,
                    dow=dow_str_for_profile,
                    currTime=deptTime_str_hhmm,
                    incremented_profile_fare_flag=1
                )
                potential_seats = self.rmdatabasehelper.get_group_potential_seats(sector, month)
                
                try:
                    groupbookload = float(groupbookload) if groupbookload not in (None, '', 'NA', 'None') else 0.0
                except (ValueError, TypeError):
                    groupbookload = 0.0
                    
                max_au = currBookedLoad - groupbookload + int(potential_seats)
                max_au = min(adjustedCapacity - 50, max_au)

                # Get highest group RBD fare (GA is the highest)
                max_group_fare = group_alloc_df['TOTAL'].max()

                # Determine whether to use forecast or dplf(variance) for band calculation
                use_forecast = (imp_forecast is not None and imp_forecast != CONST_NA and not pd.isna(imp_forecast) and ndocounter <= 30)

                # Build month string for group_profile_fare_increments: "April_H1_May_H2" format
                # Based on current date, take last 2 halves
                today = self.timeutils.getCurrentDateTime() if self.timeutils else datetime.now()
                current_day = today.day
                current_month_name = today.strftime('%B')  # e.g., "May"

                if current_day <= 15:
                    increment_month_str = f"{current_month_name}_H1"
                else:
                    increment_month_str = f"{current_month_name}_H2"

                current_year = today.year

                # Get cluster from departure time
                try:
                    time_obj = datetime.strptime(deptTime_str_hhmm, '%H:%M').time()
                    cluster = self.rmdatabasehelper.getCluster(time_obj)
                except Exception:
                    cluster = 0

                # Fetch increment factors
                increment_doc = self.rmdatabasehelper.get_group_profile_fare_increments(
                    sector, current_year, increment_month_str, dow_str_for_profile, cluster
                )

                # Get B2C selling fare as float
                b2c_fare_numeric = 0.0
                try:
                    if b2c_selling_fare is not None and b2c_selling_fare != CONST_NA and str(b2c_selling_fare).strip() != '':
                        b2c_fare_numeric = float(b2c_selling_fare)
                except (ValueError, TypeError):
                    b2c_fare_numeric = 0.0

                # Pre-fetch all group forecast bands once to avoid repeated MongoDB calls
                all_forecast_bands = self.rmdatabasehelper.get_all_group_forecast_bands()

                # Initialize allocation dict for group RBDs (ordered low to high fare)
                allocation_dict = {row['RBD']: 0 for _, row in group_alloc_df.iterrows()}

                # Iterative allocation: 5 seats per batch
                simulated_booked_load = float(currBookedLoad)
                is_first_iteration = True
                last_target_idx = None  # Tracks last allocated RBD index (sorted low-to-high by fare)

                while simulated_booked_load < int(final_totalCapacity):
                    # Determine band (dplf or forecast-based)
                    simulated_imp_forecast = CONST_NA
                    var_for_band = CONST_NA
                    if use_forecast:
                        # Adjust imputed forecast for simulated seats sold
                        simulated_imp_forecast = imp_forecast + ((simulated_booked_load - float(currBookedLoad)) / final_totalCapacity) * 100
                        band_result = self._lookup_forecast_band(all_forecast_bands, simulated_imp_forecast)
                        band = band_result['band']
                        discount_pct = band_result['b2c_discount']
                        fare_anchor_strategy = band_result['fare_anchor_strategy']
                        b2c_fare_weight = band_result['b2c_fare_weight']
                        profile_fare_weight = band_result['profile_fare_weight']
                    else:
                        var_for_band = calculate_variance_for_load(
                            simulated_booked_load, final_totalCapacity, ndocounter,
                            sod_tgtAtCurrentHour_abs, sod_startOfDayTgtBookedLoad_abs
                        )
                        band = self.rmdatabasehelper.getdpflBand(var_for_band)
                        if band is None:
                            band = 0
                        # Use dplf band to look up from cached bands
                        band_result = self._lookup_band_by_number(all_forecast_bands, band)
                        discount_pct = band_result['b2c_discount']
                        fare_anchor_strategy = band_result['fare_anchor_strategy']
                        b2c_fare_weight = band_result['b2c_fare_weight']
                        profile_fare_weight = band_result['profile_fare_weight']

                    # --- Step 1: Calculate adjusted PLF (percentage) ---
                    adjusted_plf = calculate_group_adjusted_load(
                        simulated_booked_load, final_totalCapacity, adjustedCapacity, ndocounter,
                        sod_tgtAtCurrentHour_abs, sod_startOfDayTgtBookedLoad_abs,
                        simulated_imp_forecast, use_forecast, self.constantsutils
                    )

                    # --- Step 2: Get profile fare quartile and profile fare from profilefarextract ---
                    # Convert adjusted PLF back to load for quartile calculation
                    adjusted_load_for_quartile = (adjusted_plf / 100) * adjustedCapacity
                    bookload_percentile = calculate_profilefare_quartile(
                        final_totalCapacity, adjusted_load_for_quartile, self.mathutils
                    )
                    percentile_str = f'Q{str(int(bookload_percentile))}'

                    # Look up profile fare directly from profilefarextract DataFrame
                    if percentile_str in profilefarextract.columns:
                        profile_fare_value = float(profilefarextract[percentile_str].iloc[0])
                    else:
                        profile_fare_value = 0.0

                    # Apply upsell to profile fare value
                    adjusted_profile_fare = profile_fare_value
                    if profile_fare_value > 0 and upsell is not None and str(upsell).strip() not in ('', 'NA', 'None'):
                        try:
                            upsell_pct = float(upsell)
                            if upsell_pct != 0:
                                adjusted_profile_fare = profile_fare_value * (1 + upsell_pct / 100)
                        except (ValueError, TypeError):
                            pass

                    # --- Step 3: Apply increment factor from group_profile_fare_increments ---
                    # increment_str = f'I{str(int(bookload_percentile))}'
                    # increment_factor = 1.0
                    # if increment_doc and increment_str in increment_doc:
                    #     try:
                    #         increment_factor = float(increment_doc[increment_str])
                    #     except (ValueError, TypeError):
                    #         increment_factor = 1.0

                    # adjusted_profile_fare = profile_fare_value * increment_factor


                    # --- Step 4: Calculate discounted B2C selling fare ---
                    # Apply discount to B2C selling fare
                    discounted_b2c_fare = b2c_fare_numeric * (1 + float(discount_pct) / 100) if b2c_fare_numeric > 0 else 0.0

                    # --- Step 5: Determine fare anchor based on strategy ---
                    fare_anchor = calculate_group_fare_anchor(adjusted_profile_fare, discounted_b2c_fare, fare_anchor_strategy, b2c_fare_weight, profile_fare_weight)
                    if fare_anchor is None:
                        break

                    # Capture first iteration metrics for output summary
                    if is_first_iteration:
                        first_iter_adjusted_profile_fare = round(adjusted_profile_fare, 2)
                        first_iter_adjusted_plf = round(adjusted_plf, 2)
                        first_iter_fare_anchor = round(fare_anchor, 2)
                        is_first_iteration = False


                    # --- Step 6: Check if fare anchor exceeds highest group RBD fare ---
                    if fare_anchor > max_group_fare:
                        # Fare anchor crossed GA, stop iteration
                        break

                    if fare_anchor < b2c_backstop_final:
                        fare_anchor = b2c_backstop_final

                    # --- Step 7: Find nearest higher group RBD and assign seats ---
                    # Find the nearest RBD with fare >= fare_anchor
                    higher_rbds = group_alloc_df[group_alloc_df['TOTAL'] >= fare_anchor]
                    if higher_rbds.empty:
                        # No RBD at or above fare anchor, stop
                        break

                    # Pick the nearest higher (lowest fare that is >= fare_anchor)
                    target_idx = higher_rbds['TOTAL'].idxmin()

                    # Never step down to a lower-fare RBD than previously allocated.
                    # If the new fare anchor points to a lower RBD, stay at the last allocated one.
                    if last_target_idx is not None and target_idx < last_target_idx:
                        target_idx = last_target_idx

                    target_rbd = group_alloc_df.loc[target_idx, 'RBD']

                    # Add 5 seats (or remaining to capacity) to this RBD's AU
                    seats_to_add = min(self.constantsutils.GROUP_AU_BATCH, int(final_totalCapacity) - simulated_booked_load)
                    if seats_to_add <= 0:
                        break

                    # Update allocation: all RBDs at or above target get at least this AU
                    target_fare = group_alloc_df.loc[target_idx, 'TOTAL']
                    allocation_dict[target_rbd] += seats_to_add
                    last_target_idx = target_idx

                    # Simulate adding seats to booked load for next iteration
                    simulated_booked_load += seats_to_add
                    # print(
                    #     f"{adjusted_profile_fare=:.2f}, {adjusted_plf=:.2f}, "
                    #     f"{fare_anchor=:.2f}, {target_rbd=}, {simulated_booked_load=}, "
                    #     f"{simulated_imp_forecast=:}, {var_for_band=:}, "
                    # )

                    # Safety: don't exceed total capacity
                    if simulated_booked_load >= int(final_totalCapacity):
                        break

                # Apply allocations to the AU grid
                cummulative_au = 0
                for rbd_name in allocation_dict:
                    currRec.set_class_attribute(brd_counter, 'class_of_service', rbd_name)
                    if cummulative_au == 0 and allocation_dict[rbd_name] > 0:
                        cummulative_au = currBookedLoad
                    cummulative_au += allocation_dict[rbd_name]
                    currRec.set_class_attribute(brd_counter, 'class_au', cummulative_au)
                    brd_counter += 1

            except Exception as e:
                self.logger.error(f"Error in group_quote_alloc: {e}")
                # Fallback: set all group RBDs to 0
                for rbd_name in rbd_list:
                    currRec.set_class_attribute(brd_counter, 'class_of_service', rbd_name)
                    currRec.set_class_attribute(brd_counter, 'class_au', 0)
                    brd_counter += 1

        elif int(autoGroup) == 0 or group_alloc_df.empty:
            # Allocate all RBDs with 0
            for RBDLabel in rbd_map.values():
                currRec.set_class_attribute(brd_counter, 'class_of_service', RBDLabel)
                currRec.set_class_attribute(brd_counter, 'class_au', 0)
                brd_counter += 1

        return brd_counter, first_iter_adjusted_profile_fare, first_iter_adjusted_plf, first_iter_fare_anchor

    def _lookup_forecast_band(self, all_bands, imp_forecast):
        """Look up the matching band from the pre-fetched list using forecast value."""
        default = {'band': 0, 'b2c_discount': 0, 'fare_anchor_strategy': 'min', 'b2c_fare_weight': 0, 'profile_fare_weight': 1}
        try:
            val = float(imp_forecast)
            for b in all_bands:
                if b['start'] <= val <= b['end']:
                    return b
        except (ValueError, TypeError):
            pass
        return default

    def _lookup_band_by_number(self, all_bands, band_number):
        """Look up the matching band from the pre-fetched list using band number."""
        default = {'b2c_discount': 0, 'fare_anchor_strategy': 'min', 'b2c_fare_weight': 0, 'profile_fare_weight': 1}
        try:
            target = int(band_number)
            for b in all_bands:
                if b['band'] == target:
                    return b
        except (ValueError, TypeError):
            pass
        return default
    
    
    def tbf_allocation(self,alloc_df,brd_counter,currRec,tbfFlag,B2CRBDs,currtbfBookedLoad,currBookedLoad,adjustedCapacity,openingRBDPrice,month,sector,tbfenhflag,depdate):
        max_au = currBookedLoad+int(self.constantsutils.TBF_SEATS)
        rbd_list = self.constantsutils.TBF_RBD.split(',')
        blocked_seats = self.rmdatabasehelper.get_tbf_seats_blocked(self.constantsutils.TBF_ORG_CODE,depdate)
        alloc_df = alloc_df.sort_values(by='jumpingRBDPrice').reset_index(drop=True)
        tbf_rbds = pd.DataFrame()
        if not B2CRBDs.empty:
            tbf_rbds = B2CRBDs[B2CRBDs['RBD'].isin(rbd_list)]
            tbf_rbds = tbf_rbds.sort_values(by='TOTAL').reset_index(drop=True)
        latest_allocations = {}
        desired_order = ['TL', 'TK', 'TJ', 'TI', 'TH', 'TG']
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
                differences = abs(tbf_rbds['TOTAL'] - jumping_price)
                min_diff = differences.min()
                nearest_idx = differences.idxmin()
                nearest_rbd = tbf_rbds.loc[nearest_idx, 'RBD']
                nearest_total = tbf_rbds.loc[nearest_idx, 'TOTAL']
                
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
                # Get the first allocated RBD (will be one with lowest TOTAL based on sorting)
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
                        for _, row in tbf_rbds.iterrows():
                            if row['TOTAL'] <= adjusted_price:
                                latest_allocations[row['RBD']] = {
                                    'index': None,
                                    'min_other_diff': 0
                                }
                        # Find the next highest RBD based on the adjusted price
                        next_rbd_differences = tbf_rbds['TOTAL'] - adjusted_price
                        next_rbd_differences = next_rbd_differences[next_rbd_differences > 0]  # Only higher prices
                        
                        if not next_rbd_differences.empty:
                            # Find all subsequent RBDs in sequence
                            remaining_tbf = tbf_rbds.copy()
                            current_price = adjusted_price
                            
                            while not remaining_tbf.empty:
                                # Find the next nearest RBD above current_price
                                next_rbd_diffs = remaining_tbf['TOTAL'] - current_price
                                next_rbd_diffs = next_rbd_diffs[next_rbd_diffs > 0]  # Only higher prices
                                
                                if not next_rbd_diffs.empty:
                                    next_nearest_idx = next_rbd_diffs.idxmin()
                                    next_nearest_rbd = remaining_tbf.loc[next_nearest_idx, 'RBD']
                                    next_rbd_total = remaining_tbf.loc[next_nearest_idx, 'TOTAL']
                                    
                                    # Find the index in alloc_df that's closest to this next RBD's TOTAL
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
                if str(tbfenhflag)=='1' and currtbfBookedLoad<=(blocked_seats-2):
                    if nearest_rbd=="TF":
                        open=currBookedLoad+self.constantsutils.TBF_OPEN_SEATS
                        class_au = max(open,min(incremental_value, max_au))
                    
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
                
        return brd_counter

    def cug_allocation(self, brd_counter, currParams, currRec, B2CRBDs, adjustedCapacity, profile_fare_value, currFltDate, ndo, b2copeningfare, cug_bookings_dict, cug_flag, cug_mkt_flag, sector):
        """
        Simplified W-series RBD allocation:
        - Allocates only if forecastedPLF < 90 and 8 <= ndo <= 30
        - AND (B2C - opening fare) <= 30% of B2C fare
        """
        w_rbd_list = self.constantsutils.CUG_RBD.split(',')
        w_rbds = pd.DataFrame()  # Initialize empty DataFrame
        if not B2CRBDs.empty:
            w_rbds = B2CRBDs[B2CRBDs['RBD'].isin(w_rbd_list) & B2CRBDs['TOTAL'].notna()].copy()

        open_rbd = None
        opening_fare = None
        allocation_value = 0

        # Flight details
        try:
            origin, destin, flight_number = currParams.origin, currParams.destin, currParams.flight_number
            departure_date = currFltDate
            if currParams.forecastPLF != CONST_NA:
                forecastedPLF = currParams.forecastPLF
            else:
                forecastedPLF = 0
        except Exception as e:
            self.logger.error(f"Error getting flight forecast data: {str(e)}")
            forecastedPLF = 0

        # Convert profile_fare_value to numeric
        try:
            if profile_fare_value == CONST_NA or profile_fare_value is None:
                profile_fare_numeric = None
            else:
                profile_fare_numeric = float(profile_fare_value)
        except (ValueError, TypeError):
            profile_fare_numeric = None

        # Condition 1: Valid range for PLF and NDO
        is_applicable = 10 <= ndo <= 30 and forecastedPLF < 90 and int(cug_flag) == 1 and int(cug_mkt_flag) == 1 and not w_rbds.empty

        if is_applicable:
            # Find best RBD with fare <= profile_fare_value
            valid_rbds = w_rbds
            fare1 = b2copeningfare - (self.constantsutils.CUG_DISCOUNT * b2copeningfare) / 100
            
            # Set fare2 to numeric value or default to fare1
            if profile_fare_numeric is not None:
                fare2 = profile_fare_numeric
            else:
                fare2 = b2copeningfare - (self.constantsutils.CUG_DISCOUNT * b2copeningfare) / 100
            
            if min(fare1, fare2) == fare2:
                # Only filter if profile_fare_numeric is valid
                if profile_fare_numeric is not None:
                    less_than_profile = valid_rbds[valid_rbds['TOTAL'] < profile_fare_numeric]
                else:
                    less_than_profile = pd.DataFrame()  # Empty DataFrame
                    
                if less_than_profile.empty:
                    fare = b2copeningfare - (self.constantsutils.CUG_B2C_OPENING_THRESHOLD * b2copeningfare)
                    if fare < max(valid_rbds['TOTAL'].tolist()):
                        
                        # Get values greater than fare
                        higher_values = valid_rbds[valid_rbds['TOTAL'] > fare]['TOTAL']
                        
                        if not higher_values.empty:
                            # Find the minimum of values greater than fare (next highest)
                            next_higher_value = higher_values.min()
                            closest_idx = higher_values.idxmin()
                                                    
                            open_rbd = valid_rbds.loc[closest_idx, 'RBD']
                            opening_fare = valid_rbds.loc[closest_idx, 'TOTAL']

                            print(f"Next higher value: {next_higher_value} at index: {closest_idx}")
                        else:
                            print("No values higher than fare found")
                            open_rbd = None
                else:
                    closest_idx = (less_than_profile['TOTAL'] - profile_fare_numeric).abs().idxmin()
                    open_rbd = valid_rbds.loc[closest_idx, 'RBD']
                    opening_fare = valid_rbds.loc[closest_idx, 'TOTAL']
            else:
                less_than_profile = valid_rbds[valid_rbds['TOTAL'] < fare1]
                if less_than_profile.empty:
                    # Handle empty case - open at 'WE'
                    we_rbd = valid_rbds[valid_rbds['RBD'] == 'WE']
                    if not we_rbd.empty:
                        open_rbd = 'WE'
                        opening_fare = we_rbd.iloc[0]['TOTAL']
                    else:
                        open_rbd = None
                        opening_fare = None
                else:
                    closest_idx = (less_than_profile['TOTAL'] - fare1).abs().idxmin()
                    open_rbd = valid_rbds.loc[closest_idx, 'RBD']
                    opening_fare = valid_rbds.loc[closest_idx, 'TOTAL']

            # Condition 2: Fare gap check
            if open_rbd is not None:
                if (b2copeningfare - opening_fare) > (self.constantsutils.CUG_B2C_OPENING_THRESHOLD * b2copeningfare):
                    fare = b2copeningfare - (self.constantsutils.CUG_B2C_OPENING_THRESHOLD * b2copeningfare)
                    if fare < max(valid_rbds['TOTAL'].tolist()):
                        
                        # Get values greater than fare
                        higher_values = valid_rbds[valid_rbds['TOTAL'] > fare]['TOTAL']
                        
                        if not higher_values.empty:
                            # Find the minimum of values greater than fare (next highest)
                            next_higher_value = higher_values.min()
                            closest_idx = higher_values.idxmin()
                                                    
                            open_rbd = valid_rbds.loc[closest_idx, 'RBD']
                            opening_fare = valid_rbds.loc[closest_idx, 'TOTAL']

                            print(f"Next higher value: {next_higher_value} at index: {closest_idx}")
                        else:
                            print("No values higher than fare found")
                            open_rbd = None
                            
                    else:
                        self.logger.info(f"Skipping cug allocation: fare ladder is less than opening fare")
                        open_rbd = None   
                        
                if opening_fare and opening_fare > b2copeningfare:
                    self.logger.info(f"Skipping cug allocation: Opening fare is greater than b2c selling fare")
                    open_rbd = None

            # If both conditions pass, calculate allocation
            if open_rbd:
                try:
                    open_rbd_bookings = cug_bookings_dict.get(open_rbd, 0) if cug_bookings_dict else 0
                except Exception as e:
                    self.logger.error(f"Error getting bookings for {open_rbd}: {str(e)}")
                    open_rbd_bookings = 0

                plf_gap = 90 - forecastedPLF
                allocation_percent = plf_gap * self.constantsutils.CUG_SEATS_PERCENT
                allocation_value = int(round(((allocation_percent / 100) * adjustedCapacity) + open_rbd_bookings))
                if allocation_value < 1:
                    allocation_value = 1

            # Final Allocation: only to open_rbd, others get 0
            for rbd in w_rbd_list:
                currRec.set_class_attribute(brd_counter, 'class_of_service', rbd)
                currRec.set_class_attribute(brd_counter, 'class_au', allocation_value if rbd == open_rbd else 0)
                brd_counter += 1
                
        elif (10 <= ndo <= 30 and forecastedPLF > 90 and profile_fare_numeric is not None and int(cug_flag) == 1 and int(cug_mkt_flag) == 1):
            for rbd in w_rbd_list:
                currRec.set_class_attribute(brd_counter, 'class_of_service', rbd)
                currRec.set_class_attribute(brd_counter, 'class_au', 0)
                brd_counter += 1
                
        else:
            for rbd in w_rbd_list:
                currRec.set_class_attribute(brd_counter, 'class_of_service', rbd)
                currRec.set_class_attribute(brd_counter, 'class_au', -1)
                brd_counter += 1

        return brd_counter


    def b2CAllocation_connections(self, openDataB2C, currRec, minLid, currBookedLoad, SkippingFactor, currParams,should_allocate_to_y):

        b2cAllocationsUnits = []
        origin = currParams.sector1[:3]
        destin = currParams.sector2[-3:]
        B2CRBDs = self.rmdatabasehelper.fareExtract(origin, destin, B2C_CHANNEL, '', 'R')
        rbd_list = self.constantsutils.TBF_RBD.split(',')+self.constantsutils.CUG_RBD.split(',')
        rbd_list.extend(list(json.loads(GROUP_QUOTATION).values()))
        B2CRBDs = B2CRBDs[~B2CRBDs['RBD'].isin(rbd_list)].reset_index(drop=True)

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
        while b2cctr < B2CRBDs.shape[0] or b2bctr < self.constantsutils.B2B_RDB_COUNT:

            if b2cctr < B2CRBDs.shape[0]:
                RBDLabel = B2CRBDs['RBD'][b2cctr]
                firstRDBAllocation = int(currParams.first_rbd_alloc)
                otherRDBAllocation = int(currParams.other_rbd_alloc)
                price = B2CRBDs['TOTAL'][b2cctr]
                b2bFactor = float(currParams.b2b_factor)
                nextPrice = price
                if size > b2cctr:
                    nextPrice = B2CRBDs['TOTAL'][b2cctr + 1]

                currRec.set_class_attribute(b2cctr + 1, 'class_of_service', RBDLabel)
                if RBDLabel == openingRBD:
                    openbuketFlag = True

            # Allocation
            if openbuketFlag:
                # Valid B2C Allocation
                # Positive Allocation
                if firstRDBAllocation >= 0:
                    openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, 0, openRBDIndex, incremental, jumpPercent, price, nextPrice, firstRDBAllocation, otherRDBAllocation, jumpingRBDPrice, b2bFactor, incrementalb2b)

                if incremental >= minLid:
                    incremental = minLid
                if incrementalb2b >= minLid:
                    incrementalb2b = minLid
                if should_allocate_to_y:
                    incremental = minLid
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
        B2BRBDs = self.rmdatabasehelper.fareExtract(origin, destin, B2B_CHANNEL, '', 'R')
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

    def sales_inv_allocation(self, distress_pax_count, brd_counter, currRec, rbd_list,ndocounter,dateFormatted,origin,destin,saleflag):
        # Get sales inventory allocation based on distress pax count

        # If no distress passengers, set all allocations to 0
        if distress_pax_count <= 0:
            for rbd in rbd_list:
                currRec.set_class_attribute(brd_counter, 'class_au', 0)
                currRec.set_class_attribute(brd_counter, 'class_of_service', rbd)
                brd_counter += 1
            return brd_counter
        
        if str(saleflag)=="0":
            for rbd in rbd_list:
                currRec.set_class_attribute(brd_counter, 'class_au', -1)
                currRec.set_class_attribute(brd_counter, 'class_of_service', rbd)
                brd_counter += 1
            return brd_counter

        # Get inventory value based on pax count and ND
            
            
        # Get departure date in mm/dd/yyyy format
        dep_date = datetime.strptime(dateFormatted, '%Y-%m-%d').strftime('%m/%d/%Y')
        
        # Check distress calendar value for departure date and sector
        distress_calendar_df = self.rmdatabasehelper.get_distress_calendar_value(
            dep_date,
            origin + destin
        )
        distress_calendar_value = 0 if distress_calendar_df.empty else distress_calendar_df['value'].iloc[0]
        
        # If distress calendar value is 1, set all allocations to 0 and return
        if distress_calendar_value == 1:
            for rbd in rbd_list:
                currRec.set_class_attribute(brd_counter, 'class_au', 0)
                currRec.set_class_attribute(brd_counter, 'class_of_service', rbd)
                brd_counter += 1
            return brd_counter
        # Get inventory value from table based on NDO or month
        date_obj = datetime.strptime(dateFormatted, '%Y-%m-%d')
        if ndocounter > 30:
            # Convert date format from YYYY-MM-DD to YYMMDD for closer dates
            # And YYMM00 for farther dates
            
            
            date_farther = date_obj.strftime('%Y%m') + '00'
            # For farther dates
            sales_inventory_df = self.rmdatabasehelper.get_sales_inventory(date_farther)
            if not sales_inventory_df.empty:
                seats = sales_inventory_df['seatsallocation'].iloc[0]
            else:
                seats = 0

            # Get RBD distribution for farther dates
            rbd_dist_df = self.rmdatabasehelper.get_farther_rbd_distribution(seats)
            if not rbd_dist_df.empty:
                rbd_allocations = {}
                for _, row in rbd_dist_df.iterrows():
                    rbd_allocations[row['rbd']] = row['value'] 
            else:
                rbd_allocations = {}
        else:
            date_closer = date_obj.strftime('%Y%m%d')
            # For closer dates
            sales_inventory_df = self.rmdatabasehelper.get_sales_inventory(date_closer) 
            if not sales_inventory_df.empty:
                seats = sales_inventory_df['seatsallocation'].iloc[0]
            else:
                seats = 0

            # Get RBD distribution for closer dates
            rbd_dist_df = self.rmdatabasehelper.get_closer_rbd_distribution(seats)
            if not rbd_dist_df.empty:
                rbd_allocations = {}
                for _, row in rbd_dist_df.iterrows():
                    rbd_allocations[row['rbd']] = row['value']
            else:
                rbd_allocations = {}
        

        # Set allocations for each RBD
        for rbd in rbd_list:
            allocation = rbd_allocations.get(rbd, 0)
            currRec.set_class_attribute(brd_counter, 'class_au', int(allocation))
            currRec.set_class_attribute(brd_counter, 'class_of_service', rbd)
            brd_counter += 1
            
        return brd_counter
