import io
import boto3
import pandas as pd
import logging
import os
import configsettings
from constants_utils import ConstantsUtils
from datetime import datetime, timedelta, date
import traceback
import uuid
import pytz
import numpy as np
from helpers.pa_db_helpers.fare_pa_helper import FarePAHelper

class FareFiling:
    def __init__(self, logger, params, overrides, context):
        """Initialize with required connections and constants."""
        self.logger = logger
        self.db = params['mongo']
        self.constants = ConstantsUtils(self.db)
        self.pa_helper = FarePAHelper(self.db, logger)
        self.type = overrides['type']
        self.analystName = overrides['analyst']
                
        # Initialize data containers
        self.qp_fares_data = None
        self.product_upsell_data = None
        self.product_upsell_master_data = None
        self.product_rules_data = None
        self.tax_master_dom_data = None
        self.tax_master_intl_data = None
        self.saver_data = None
        self.currentVersion = None
        self.isSaver = True if self.type == "Saver" else False
        
        # RBD list for iteration
        self.rbd_list = None
        
        self.existing_qp_sectors = set()

    def collect_all_data(self):
        """Collect all required data for fare filing based on type."""
        try:
            if self.isSaver:
                # For Saver type: Start with saver table
                self.saver_data = self.pa_helper.collect_saver_data(self.isSaver)
                
                if self.saver_data.empty:
                    raise ValueError("Saver table is empty")
                
                # Get unique sectors from saver table - THIS IS THE SOURCE OF TRUTH
                saver_sectors = self.saver_data['sector'].unique().tolist()
                self.logger.info(f"Found {len(saver_sectors)} unique sectors in saver table: {saver_sectors}")
                
                # NEW: Check which sectors exist in qp_fares
                self.existing_qp_sectors = self.pa_helper._identify_existing_qp_sectors(saver_sectors)
            
                # Collect product upsell data
                self.product_upsell_data = self.pa_helper.collect_upsell_data(self.isSaver)
                
                # Check which saver sectors are in product_upsell
                upsell_sectors = self.product_upsell_data['sector'].unique().tolist() if not self.product_upsell_data.empty else []
                missing_sectors = [s for s in saver_sectors if s not in upsell_sectors]
                
                if missing_sectors:
                    self.logger.info(f"Found {len(missing_sectors)} sectors in saver not in product_upsell, checking master")
                    
                    # Collect product upsell master data
                    self.product_upsell_master_data = self.pa_helper.collect_upsell_master_data(self.isSaver)
                    
                    if self.product_upsell_master_data.empty:
                        error_msg = f"Sectors {missing_sectors} not found in product_upsell and product_upsell_master is empty"
                        self.logger.error(error_msg)
                        raise ValueError(error_msg)
                    
                    # Check which missing sectors are in master
                    master_sectors = self.product_upsell_master_data['sector'].unique().tolist()
                    still_missing = [s for s in missing_sectors if s not in master_sectors]
                    
                    if still_missing:
                        error_msg = f"Sectors {still_missing} not found in product_upsell or product_upsell_master"
                        self.logger.error(error_msg)
                        raise ValueError(error_msg)
                    
                    # Get upsell data for missing sectors from master
                    master_rows = self.product_upsell_master_data[
                        self.product_upsell_master_data['sector'].isin(missing_sectors)
                    ]
                    
                    # Append master rows to product_upsell_data
                    self.product_upsell_data = pd.concat([self.product_upsell_data, master_rows], ignore_index=True)
                    self.logger.info(f"Added {len(master_rows)} rows from master for missing sectors")
                
                # CRITICAL FIX: Filter product_upsell_data to ONLY include saver sectors
                # This ensures we only process upsells for sectors that exist in saver table
                if not self.product_upsell_data.empty:
                    original_count = len(self.product_upsell_data)
                    
                    self.product_upsell_data = self.product_upsell_data[
                        self.product_upsell_data['sector'].isin(saver_sectors)
                    ]
                    filtered_count = len(self.product_upsell_data)
                    
                    
                    if original_count > filtered_count:
                        removed_sectors = original_count - filtered_count
                        self.logger.info(f"Filtered out {removed_sectors} upsell rows for sectors not in saver table")
                    
                    if self.product_upsell_data.empty:
                        error_msg = f"No product upsell data found for saver sectors: {saver_sectors}"
                        self.logger.error(error_msg)
                        raise ValueError(error_msg)
                
            else:
                # For Product Upsell type: Start with product_upsell
                self.product_upsell_data = self.pa_helper.collect_upsell_data(self.isSaver)
                
                if self.product_upsell_data.empty:
                    raise ValueError("Product upsell table is empty")
                
                # Get unique sectors from product upsell
                unique_sectors = self.product_upsell_data['sector'].unique().tolist()
                
                # NEW: Check which sectors exist in qp_fares
                self.existing_qp_sectors = self.pa_helper._identify_existing_qp_sectors(unique_sectors)
                
                # Extract unique base footnotes and NDOs for optimized QP fares query
                base_footnotes = []
                base_ndos = []
                
                if 'base_footnote' in self.product_upsell_data.columns:
                    base_footnotes = self.product_upsell_data['base_footnote'].dropna().unique().tolist()
                    # Include empty footnotes as well
                    base_footnotes.extend(['', None])
                
                if 'base_ndo' in self.product_upsell_data.columns:
                    base_ndos = self.product_upsell_data['base_ndo'].dropna().unique().tolist()
                    # Include default NDO as well
                    base_ndos.extend(['X', '', None])
                
                # Fetch from QP fares for rules 1001 and 2001 with footnote/NDO filtering
                self.qp_fares_data = self.pa_helper.collect_qp_fares(
                    unique_sectors,
                    base_footnotes=base_footnotes if base_footnotes else None,
                    base_ndos=base_ndos if base_ndos else None
                )
                
                # ===== NEW: VALIDATE ALL SECTORS EXIST IN QP FARES =====
                if self.qp_fares_data.empty:
                    error_msg = f"No QP fares found for any of the sectors: {unique_sectors}"
                    self.logger.error(error_msg)
                    raise ValueError(error_msg)
                
                # Check which sectors are present in fetched data
                fetched_sectors = self.qp_fares_data['SECTOR'].unique().tolist() if not self.qp_fares_data.empty else []
                missing_sectors = [s for s in unique_sectors if s not in fetched_sectors]
                
                if missing_sectors:
                    error_msg = (
                        f"VALIDATION FAILED: {len(missing_sectors)} sector(s) from product_upsell "
                        f"not found in qp_fares (rules 1001/2001).\n"
                        f"Missing sectors: {missing_sectors}\n"
                        f"Found sectors: {fetched_sectors}\n"
                        f"Expected sectors: {unique_sectors}"
                    )
                    self.logger.error(error_msg)
                    raise ValueError(error_msg)

            # Collect product rules data
            self.product_rules_data = self.pa_helper.collect_product_rules_data()
            # Convert to dict for easier lookup
            self.product_rules_dict = self.product_rules_data.set_index('rule_nbr')['letter'].to_dict()
            
            # Collect tax master data for all-in fare calculations
            self.tax_master_dom_data, self.tax_master_intl_data, error = self.pa_helper.collect_tax_master_data()
            
            if error:
                self.logger.warning(f"Tax master data issue: {error}")
            
            # Get RBD list from constants
            self.rbd_list = self.constants.RBD_VALUES.split(',')
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error in data collection: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise
        
    def get_pos_from_saver(self, sector, rbd):
        """Get POS value from saver table."""
        try:
            if self.isSaver:
                fare_row = self.saver_data[
                    (self.saver_data['sector'] == sector) & 
                    (self.saver_data['rbd'] == rbd)
                ]
                if not fare_row.empty:
                    pos = fare_row.iloc[0].get('pos', 'B')
                    return pos if pd.notna(pos) and pos != '' else 'B'
            
            return 'B'
            
        except Exception as e:
            self.logger.warning(f"Error getting POS for {sector}-{rbd}: {str(e)}")
            return 'B'

    def get_saver_fares(self, sector, rbd, base_footnote=None, base_ndo=None):
        """Get base saver fare for a given sector and RBD with footnote/NDO matching.
        
        Args:
            sector: Sector code (e.g., 'BOMDEL')
            rbd: RBD code (e.g., 'V9', 'Y0')
            base_footnote: Footnote to match (None = match any, '' = match empty)
            base_ndo: NDO to match (None = match any, '' or 'X' = match default/empty)
        """
        try:
            if self.isSaver:
                # Get from saver table - this is ALL-IN FARE
                fare_conditions = (
                    (self.saver_data['sector'] == sector) & 
                    (self.saver_data['rbd'] == rbd)
                )
                
                # ===== FIX: Handle None (match any) vs '' (match empty) =====
                # If base_footnote is None, don't filter by footnote at all (match any)
                # If base_footnote is '', match only empty footnotes
                if base_footnote is not None:
                    if base_footnote == '':
                        # Match empty footnotes only
                        fare_conditions = fare_conditions & (
                            (self.saver_data['footnote'].isna()) |
                            (self.saver_data['footnote'] == '') |
                            (self.saver_data['footnote'] == None)
                        )
                    else:
                        # Match specific footnote
                        fare_conditions = fare_conditions & (self.saver_data['footnote'] == base_footnote)
                # else: base_footnote is None, don't filter by footnote (match any)
                
                # If base_ndo is None, don't filter by NDO at all (match any)
                # If base_ndo is '' or 'X', match only default/empty NDOs
                if base_ndo is not None:
                    if base_ndo == '' or base_ndo == 'X':
                        # Match default/empty NDO only
                        fare_conditions = fare_conditions & (
                            (self.saver_data['ndo'].isna()) |
                            (self.saver_data['ndo'] == '') |
                            (self.saver_data['ndo'] == 'X') |
                            (self.saver_data['ndo'] == None)
                        )
                    else:
                        # Match specific NDO
                        fare_conditions = fare_conditions & (self.saver_data['ndo'] == base_ndo)
                # else: base_ndo is None, don't filter by NDO (match any)
                # ==============================================================
                
                fare_row = self.saver_data[fare_conditions]
                if not fare_row.empty:
                    return fare_row.iloc[0]['fare']
            else:
                # Get from QP fares table - TOTAL is BASE FARE
                fare_conditions = (
                    (self.qp_fares_data['SECTOR'] == sector) & 
                    (self.qp_fares_data['RBD'] == rbd)
                )
                
                # ===== FIX: Handle None (match any) vs '' (match empty) =====
                # If base_footnote is None, don't filter by footnote at all (match any)
                # If base_footnote is '', match only empty footnotes
                if base_footnote is not None:
                    if base_footnote == '':
                        # Match empty footnotes only
                        fare_conditions = fare_conditions & (
                            (self.qp_fares_data['footnote'].isna()) |
                            (self.qp_fares_data['footnote'] == '') |
                            (self.qp_fares_data['footnote'] == None)
                        )
                    else:
                        # Match specific footnote
                        fare_conditions = fare_conditions & (self.qp_fares_data['footnote'] == base_footnote)
                # else: base_footnote is None, don't filter by footnote (match any)
                
                # If base_ndo is None, don't filter by NDO at all (match any)
                # If base_ndo is '' or 'X', match only default/empty NDOs
                if base_ndo is not None:
                    if base_ndo == '' or base_ndo == 'X':
                        # Match default/empty NDO only
                        fare_conditions = fare_conditions & (
                            (self.qp_fares_data['NDO'].isna()) |
                            (self.qp_fares_data['NDO'] == '') |
                            (self.qp_fares_data['NDO'] == 'X') |
                            (self.qp_fares_data['NDO'] == None)
                        )
                    else:
                        # Match specific NDO
                        fare_conditions = fare_conditions & (self.qp_fares_data['NDO'] == base_ndo)
                # else: base_ndo is None, don't filter by NDO (match any)
                # ==============================================================
                
                fare_row = self.qp_fares_data[fare_conditions]
                if not fare_row.empty:
                    return fare_row.iloc[0]['TOTAL']
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Error getting saver fare for {sector}-{rbd} (base_footnote: {base_footnote}, base_ndo: {base_ndo}): {str(e)}")
            return None
        
    def calculate_all_in_fare(self, base_fare, origin, destination, ftype):
        """Calculate all-in fare or extract base fare depending on ftype.
        """
        try:
            taxes = self._calculate_taxes_domestic(base_fare, origin, destination, ftype)
            
            return taxes            
        except Exception as e:
            self.logger.warning(f"Error calculating all-in fare: {str(e)}")
            return base_fare
        
    def _calculate_taxes_domestic(self, base_fare, origin, destination,ftype):
        """Calculate all taxes for domestic fare to get all-in fare."""
        try:
            if self.tax_master_dom_data is None or self.tax_master_dom_data.empty:
                return 0
                
            tax_data = self.tax_master_dom_data[self.tax_master_dom_data['Code'] == origin]
            if tax_data.empty:
                return 0
            
            tax_row = tax_data.iloc[0]
            gst_rate = float(tax_row.get('GST', 0)) / 100
            rcs = float(tax_row.get('RCS', 0)) if pd.notna(tax_row.get('RCS')) else 0
            cute = float(tax_row.get('CUTE', 0)) if pd.notna(tax_row.get('CUTE')) else 0
            asf = float(tax_row.get('ASF', 0)) if pd.notna(tax_row.get('ASF')) else 0
            psf = float(tax_row.get('PSF', 0)) if pd.notna(tax_row.get('PSF')) else 0
            udf_df = float(tax_row.get('UDF_DF', 0)) if pd.notna(tax_row.get('UDF_DF')) else 0
            
            if destination in self.constants.PRICING_RCS_SECTORS:
                rcs = 0
            
            duf = 0
            if destination in self.constants.PRICING_DUF_SECTORS:
                duf = float(self.constants.PRICING_DUF_SECTORS[destination])
            
            # Calculate GST
            if origin in self.constants.PRICING_GST_SECTORS or destination in self.constants.PRICING_GST_SECTORS:
                gst_rate = 0
            
            if(ftype == 'base'):
                total_taxes = base_fare + asf + psf + udf_df + rcs + cute + gst_rate * (base_fare + rcs + cute) + duf
            else:
                # Base Fare=Gross Fare−[GST×(RCS+CUTE) +RCS + Other Taxes]/ (1+GST) 
                # Base Fare={Gross Fare−[GST×(RCS+CUTE) +RCS + Other Taxes]}/ (1+GST)
                total_taxes = (base_fare - ((gst_rate * (rcs + cute)) + rcs + cute + duf + asf + psf + udf_df))/(1+gst_rate)
                
            return total_taxes
            
        except Exception as e:
            return 0

    def apply_upsell(self, base_value, upsell_type, upsell_value, min_value, max_value):
        """Apply upsell to base value with min/max constraints on upsell amount."""
        try:
            # Calculate upsell amount based on type
            if upsell_type == 'PERCENTAGE':
                # For PERCENTAGE: calculate percentage of base value
                upsell_amount = base_value * (upsell_value / 100)
            else:  # VALUE
                # For VALUE: use the value directly as upsell amount
                upsell_amount = upsell_value
            
            # Apply constraints to upsell amount itself (not to final fare)
            if pd.notna(min_value) and upsell_amount < min_value:
                upsell_amount = min_value
            if pd.notna(max_value) and upsell_amount > max_value:
                upsell_amount = max_value
            
            return base_value + upsell_amount
            
        except Exception as e:
            self.logger.warning(f"Error applying upsell: {str(e)}")
            return base_value

    def get_rbd_range(self, rbd_start, rbd_end):
        """Get list of RBDs between start and end (inclusive)."""
        try:
            start_idx = self.rbd_list.index(rbd_start)
            end_idx = self.rbd_list.index(rbd_end)
            
            # Return RBDs in range (inclusive)
            return self.rbd_list[start_idx:end_idx + 1]
            
        except ValueError as e:
            self.logger.warning(f"Invalid RBD range {rbd_start}-{rbd_end}: {str(e)}")
            return []

    def generate_farebasis(self, rbd, rule_nbr, ow_rt, ndo, sector, pos=None, route_type='D', footnote=''):
        """Generate fare basis code following the 8-character format.
        
        Args:
            rbd: RBD code
            rule_nbr: Rule number
            ow_rt: One way or return
            ndo: NDO value
            sector: Sector code (e.g., 'BOMDEL')
            pos: POS value ('B' or 'W')
            route_type: Route type ('D' or 'I')
        """
        try:
            # Get letter for rule number
            letter = self.product_rules_dict.get(rule_nbr, 'R')
            
            # OW/RT mapping
            ow_rt_char = 'O' if ow_rt == 'O' else 'R' if ow_rt == 'R' else 'C'
            
            # Route type: I for domestic, Z for international
            route_char = 'I' if route_type == 'D' else 'Z'
            
            # NDO character - if footnote exists, use footnote, otherwise use NDO
            ndo_char = ndo if pd.notna(ndo) and ndo else 'X'
            
            # Determine 6th character (POS)
            if pos is not None and pos != '':
                sixth_char = pos
            else:
                # Fallback to existing logic
                sixth_char = 'B' if sector in self.existing_qp_sectors else 'W'
            
            # Format: RBD(2) + OW/RT(1) + '7' + Letter(1) + POS(1) + Route(1) + NDO/Footnote(1)
            farebasis = f"{rbd}{ow_rt_char}7{letter}{sixth_char}{route_char}{ndo_char}"
            
            return farebasis
            
        except Exception as e:
            self.logger.warning(f"Error generating farebasis: {str(e)}")
            return f"{rbd}O7RWIX"

    def calculate_product_upsells(self):
        """Calculate product upsells for all sectors and rule numbers."""
        try:
            all_output_rows = []
            
            # First, get existing sort prices from QP fares for rules 5001, 5002, 5006
            existing_sort_prices = self.pa_helper._get_existing_sort_prices()
            
            # Track which primary rules were processed (for companion generation)
            primary_rules_processed = {}  # Key: (sector, rbd, ow_rt, footnote, ndo, pos), Value: {rule: fare}
            
            # Get unique combinations of sector, rule_nbr, and RBD range
            unique_combos = self.product_upsell_data.groupby(
                ['sector', 'rule_nbr', 'rbd_start', 'rbd_end']
            ).first().reset_index()
            
            for _, combo_row in unique_combos.iterrows():
                sector = combo_row['sector']
                rule_nbr = combo_row['rule_nbr']
                rbd_start = combo_row['rbd_start']
                rbd_end = combo_row['rbd_end']
                
                # Skip companion rules in main loop - process them separately later
                if rule_nbr in ['5002', '5006', '2001']:
                    continue
                
                # Get all upsell rows for this combination
                upsell_rows = self.product_upsell_data[
                    (self.product_upsell_data['sector'] == sector) &
                    (self.product_upsell_data['rule_nbr'] == rule_nbr) &
                    (self.product_upsell_data['rbd_start'] == rbd_start) &
                    (self.product_upsell_data['rbd_end'] == rbd_end)
                ]
                
                # Get RBD range
                rbd_range = self.get_rbd_range(rbd_start, rbd_end)
                
                if not rbd_range:
                    self.logger.warning(f"Invalid RBD range for {sector}-{rule_nbr}: {rbd_start} to {rbd_end}")
                    continue
                
                # Extract origin and destination from sector
                origin = sector[:3]
                destination = sector[3:]
                
                # Process each RBD in the range
                for rbd in rbd_range:
                    # Process each upsell row (different footnotes/OW_RT combinations)
                    for _, upsell_row in upsell_rows.iterrows():
                        # Get POS from upsell if not from saver
                        pos_value = upsell_row.get('pos', 'B')
                        if pd.isna(pos_value) or pos_value == '':
                            pos_value = 'B'
                        
                        # ===== FIX: Get base_footnote and base_ndo from THIS upsell row =====
                        base_footnote = upsell_row.get('base_footnote', None)
                        if pd.isna(base_footnote):
                            base_footnote = None
                        
                        base_ndo = upsell_row.get('base_ndo', None)
                        if pd.isna(base_ndo):
                            base_ndo = None
                        
                        # Get saver fare for this RBD with THIS row's base_footnote/base_ndo
                        saver_allin_fare = self.get_saver_fares(sector, rbd, base_footnote, base_ndo)
                        # ====================================================================
                        
                        if saver_allin_fare is None:
                            self.logger.warning(f"No saver fare found for {sector}-{rbd} (base_footnote: {base_footnote}, base_ndo: {base_ndo})")
                            continue
                        
                        # Extract base fare from saver all-in fare
                        saver_base_fare = self.calculate_all_in_fare(saver_allin_fare, origin, destination, ftype="allin")
                        
                        # Determine if we need base or all-in fare for upsell calculation
                        base_all_in = upsell_row.get('base_all_in', 'All-in')
                        if pd.isna(base_all_in) or base_all_in == '':
                            base_all_in = 'All-in'
                        
                        # Get the appropriate fare to apply upsell on
                        if base_all_in == 'Base':
                            fare_for_upsell = saver_base_fare
                        else:  # All-in
                            fare_for_upsell = saver_allin_fare
                        
                        # Apply upsell
                        upsell_type = upsell_row['type']
                        upsell_value = float(upsell_row['value']) if pd.notna(upsell_row['value']) else 0
                        min_value = float(upsell_row['min_value']) if (pd.notna(upsell_row['min_value']) and upsell_row['min_value'] != '') else None
                        max_value = float(upsell_row['max_value']) if (pd.notna(upsell_row['max_value']) and upsell_row['max_value'] != '') else None
                        
                        upsold_fare = self.apply_upsell(
                            fare_for_upsell, 
                            upsell_type, 
                            upsell_value, 
                            min_value, 
                            max_value
                        )
                        
                        # Now determine the final base fare for output
                        if base_all_in == 'Base':
                            # Upsold fare is already base fare
                            final_base_fare = round(upsold_fare)
                        else:
                            # Upsold fare is all-in fare, extract base fare
                            final_base_fare = round(self.calculate_all_in_fare(upsold_fare, origin, destination, ftype="allin"))
                        
                        # Get other parameters - use upsell row's OWN footnote for output
                        ow_rt = upsell_row.get('ow_rt', 'O')
                        if pd.isna(ow_rt) or ow_rt == '':
                            ow_rt = 'O'
                        
                        ndo = upsell_row.get('ndo', 'X')
                        if pd.isna(ndo) or ndo == '':
                            ndo = 'X'
                        
                        footnote = upsell_row.get('footnote', '')
                        if pd.isna(footnote):
                            footnote = ''
                        
                        # Store fare for companion rule generation (1002 -> 5002, 1006 -> 5006, 1007 -> 2001)
                        # Key uses the OUTPUT footnote (not base_footnote)
                        if rule_nbr in ['1002', '1006', '1007']:
                            key = (sector, rbd, ow_rt, footnote, ndo, pos_value)
                            if key not in primary_rules_processed:
                                primary_rules_processed[key] = {}
                            primary_rules_processed[key][rule_nbr] = final_base_fare
                        
                        # Sort price logic
                        sort_price = upsell_row.get('sort_price')
                        
                        if rule_nbr in ['5001', '5002', '5006']:
                            if pd.notna(sort_price) and sort_price != '':
                                sort_price = float(sort_price)
                            else:
                                if sector in self.existing_qp_sectors:
                                    existing_price = existing_sort_prices.get(f"{sector}_{rbd}_{rule_nbr}", None)
                                    if existing_price is not None:
                                        sort_price = existing_price
                                    else:
                                        sort_price = final_base_fare
                                else:
                                    sort_price = final_base_fare
                        else:
                            if pd.isna(sort_price) or sort_price == '':
                                sort_price = final_base_fare
                            else:
                                sort_price = float(sort_price)
                        
                        # Generate farebasis
                        farebasis = self.generate_farebasis(rbd, rule_nbr, ow_rt, ndo, sector, pos=pos_value, route_type='D', footnote=footnote)
                        
                        # Determine actual origin, destination and fare direction
                        actual_origin, actual_destination, fare_direction = self.pa_helper._determine_fare_direction(sector)
                        
                        # Create output row
                        output_row = {
                            'Carrier_Code': 'QP',
                            'Action_Code': 'I',
                            'Origin': actual_origin,
                            'Destination': actual_destination,
                            'Fare_Basis_Code': farebasis,
                            'OW_RT': ow_rt,
                            'Routing': '',
                            'Footnote': footnote,
                            'Currency': 'INR',
                            'Fare_Amount': final_base_fare,
                            'Point_Price': 0,
                            'Effective_Date': '',
                            'Tariff_No': '',
                            'Rule_Nbr': str(rule_nbr),
                            'Link': '',
                            'Sequence': '',
                            'SOS_Charge': 0,
                            'Sort_Price': sort_price,
                            'Fare_Direction': fare_direction,
                            'Trip_Direction': '',
                            'Inbound_Outbound': '',
                            'Discontinue_Date': '',
                            'In_Batch': '',
                            'OpenJaw': 'TRUE',
                            'RBD': rbd,
                            'Sector': sector
                        }
                        
                        all_output_rows.append(output_row)
            
            # Now process companion rules (5002 from 1002, 5006 from 1006, 2001 from 1007)
            
            # Log all primary rule keys for debugging
            for key, rules in primary_rules_processed.items():
                sector, rbd, ow_rt, footnote, ndo, pos = key
            companion_rows = self._generate_companion_rules_simple(primary_rules_processed, existing_sort_prices)
            all_output_rows.extend(companion_rows)
            
            # If it's Saver type, also include the saver fares themselves
            if self.isSaver:
                saver_output_rows = self.generate_saver_output()
                all_output_rows.extend(saver_output_rows)
            
            return pd.DataFrame(all_output_rows)
            
        except Exception as e:
            self.logger.error(f"Error calculating product upsells: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise

    def _generate_companion_rules_simple(self, primary_rules_processed, existing_sort_prices):
        """Generate companion rules from primary rules.
        
        Companion rule mappings:
        - 1002 -> 5002
        - 1006 -> 5006  
        - 1007 -> 2001 (B2B RBD range: ZJ to Y0, 84 RBDs)
        
        CRITICAL: Companion rules search for primary rules using base_footnote.
        The companion rule's own footnote is used for output only.
        
        FAILURE CONDITION: If a companion rule configuration exists but no matching primary rule
        is found, the run will fail with detailed error information.
        """
        try:
            companion_rows = []
            missing_primary_rules = []  # Track missing primary rules for failure reporting
            
            # Mapping of primary to companion rules
            companion_mapping = {
                '1002': '5002',
                '1006': '5006',
                '1007': '2001'
            }
            
            # Get all companion rule configurations from product_upsell_data
            companion_configs = self.product_upsell_data[
                self.product_upsell_data['rule_nbr'].isin(['5002', '5006', '2001'])
            ].copy()
            
            # If not found in main data, check master
            if companion_configs.empty and self.product_upsell_master_data is not None:
                companion_configs = self.product_upsell_master_data[
                    self.product_upsell_master_data['rule_nbr'].isin(['5002', '5006', '2001'])
                ].copy()
            
            if companion_configs.empty:
                self.logger.info("No companion rule configurations found!")
                return companion_rows
            
            # Process each companion rule configuration
            for _, config_row in companion_configs.iterrows():
                companion_rule = config_row['rule_nbr']
                sector = config_row['sector']
                
                # Get companion rule's OWN footnote (for output)
                companion_footnote = config_row.get('footnote', '')
                if pd.isna(companion_footnote):
                    companion_footnote = ''
                
                # ===== FIX: Get base_footnote to search for primary rule =====
                base_footnote_for_search = config_row.get('base_footnote', '')
                if pd.isna(base_footnote_for_search):
                    base_footnote_for_search = ''
                
                # ===== FIX: Get base_ndo to search for primary rule =====
                base_ndo_for_search = config_row.get('base_ndo', 'X')
                if pd.isna(base_ndo_for_search) or base_ndo_for_search == '':
                    base_ndo_for_search = 'X'
                # =============================================================
                # Find the corresponding primary rule
                primary_rule = None
                for primary, companion in companion_mapping.items():
                    if companion == companion_rule:
                        primary_rule = primary
                        break
                
                if not primary_rule:
                    continue
                
                # Get RBD range for this companion rule
                rbd_start = config_row['rbd_start']
                rbd_end = config_row['rbd_end']
                rbd_range = self.get_rbd_range(rbd_start, rbd_end)
                
                # Special handling for rule 2001 - check if it should use B2B RBD range
                if companion_rule == '2001':
                    # Rule 2001 should cover ZJ to Y0 (84 RBDs) when generated from 1007
                    # Override the RBD range from config if needed
                    b2b_rbds = self.get_rbd_range('ZJ', 'Y0')  # 84 RBDs for B2B
                    if len(b2b_rbds) == 84:  # Verify we have the right range
                        rbd_range = b2b_rbds
                
                if not rbd_range:
                    continue
                
                # Get companion rule parameters
                companion_ow_rt = config_row.get('ow_rt', 'O')
                if pd.isna(companion_ow_rt) or companion_ow_rt == '':
                    companion_ow_rt = 'O'
                
                companion_ndo = config_row.get('ndo', 'X')
                if pd.isna(companion_ndo) or companion_ndo == '':
                    companion_ndo = 'X'
                
                companion_pos = config_row.get('pos', 'B')
                if pd.isna(companion_pos) or companion_pos == '':
                    companion_pos = 'B'
                
                # ========================================================
                
                # Process each RBD in the range
                for rbd in rbd_range:
                    primary_base_fare = None
                    
                    # ===== FIX: Search for primary rule using BASE_FOOTNOTE, not companion's footnote =====
                    exact_search_key = (sector, rbd, companion_ow_rt, base_footnote_for_search, base_ndo_for_search, companion_pos)
                    
                    if exact_search_key in primary_rules_processed and primary_rule in primary_rules_processed[exact_search_key]:
                        primary_base_fare = primary_rules_processed[exact_search_key][primary_rule]
                    else:
                        # If exact match not found, try with flexible ow_rt, ndo, pos but STRICT base_footnote matching
                        for key, rule_fares in primary_rules_processed.items():
                            key_sector, key_rbd, key_ow_rt, key_footnote, key_ndo, key_pos = key
                            
                            # STRICT footnote matching - use base_footnote_for_search, not companion_footnote
                            footnote_match = (
                                (base_footnote_for_search == '' and (key_footnote == '' or pd.isna(key_footnote))) or
                                (base_footnote_for_search == key_footnote)
                            )
                            
                            if (key_sector == sector and 
                                key_rbd == rbd and 
                                footnote_match and 
                                primary_rule in rule_fares):
                                primary_base_fare = rule_fares[primary_rule]
                                break
                    # ====================================================================================
                    
                    if primary_base_fare is None:
                        # CRITICAL: Track missing primary rule for failure reporting
                        missing_info = {
                            'companion_rule': companion_rule,
                            'primary_rule': primary_rule,
                            'sector': sector,
                            'rbd': rbd,
                            'companion_footnote': companion_footnote,
                            'base_footnote': base_footnote_for_search,
                            'ow_rt': companion_ow_rt,
                            'ndo': companion_ndo,
                            'pos': companion_pos
                        }
                        missing_primary_rules.append(missing_info)
                        
                        self.logger.error(f"MISSING PRIMARY RULE: Companion rule {companion_rule} (footnote='{companion_footnote}') requires primary rule {primary_rule} "
                                        f"for {sector}-{rbd} with base_footnote '{base_footnote_for_search}', ow_rt '{companion_ow_rt}', "
                                        f"ndo '{companion_ndo}', pos '{companion_pos}' but it was not found")
                        continue
                    
                    origin = sector[:3]
                    destination = sector[3:]
                    
                    # Convert primary base to all-in
                    primary_allin_fare = self.calculate_all_in_fare(primary_base_fare, origin, destination, ftype="base")
                    
                    # Determine base or all-in for companion upsell
                    base_all_in = config_row.get('base_all_in', 'All-in')
                    if pd.isna(base_all_in) or base_all_in == '':
                        base_all_in = 'All-in'
                    
                    # Apply companion upsell
                    if base_all_in == 'Base':
                        fare_for_upsell = primary_base_fare
                    else:
                        fare_for_upsell = primary_allin_fare
                    
                    upsell_type = config_row['type']
                    upsell_value = float(config_row['value']) if pd.notna(config_row['value']) else 0
                    min_value = float(config_row['min_value']) if (pd.notna(config_row['min_value']) and config_row['min_value'] != '') else None
                    max_value = float(config_row['max_value']) if (pd.notna(config_row['max_value']) and config_row['max_value'] != '') else None
                    
                    upsold_fare = self.apply_upsell(fare_for_upsell, upsell_type, upsell_value, min_value, max_value)
                    
                    # Convert to base fare
                    if base_all_in == 'Base':
                        final_base_fare = round(upsold_fare)
                    else:
                        final_base_fare = round(self.calculate_all_in_fare(upsold_fare, origin, destination, ftype="allin"))
                    
                    # Sort price logic
                    sort_price = config_row.get('sort_price')
                    if pd.notna(sort_price) and sort_price != '':
                        sort_price = float(sort_price)
                    else:
                        if sector in self.existing_qp_sectors:
                            existing_price = existing_sort_prices.get(f"{sector}_{rbd}_{companion_rule}", None)
                            if existing_price is not None:
                                sort_price = existing_price
                            else:
                                sort_price = final_base_fare
                        else:
                            sort_price = final_base_fare
                    
                    # ===== FIX: Generate farebasis with companion rule's OWN footnote (not base_footnote) =====
                    farebasis = self.generate_farebasis(rbd, companion_rule, companion_ow_rt, companion_ndo, sector, pos=companion_pos, route_type='D', footnote=companion_footnote)
                    # ========================================================================================
                    
                    # Determine fare direction
                    actual_origin, actual_destination, fare_direction = self.pa_helper._determine_fare_direction(sector)
                    
                    # Create output row with companion's OWN footnote
                    output_row = {
                        'Carrier_Code': 'QP',
                        'Action_Code': 'I',
                        'Origin': actual_origin,
                        'Destination': actual_destination,
                        'Fare_Basis_Code': farebasis,
                        'OW_RT': companion_ow_rt,
                        'Routing': '',
                        'Footnote': companion_footnote,  # Use companion's own footnote
                        'Currency': 'INR',
                        'Fare_Amount': final_base_fare,
                        'Point_Price': 0,
                        'Effective_Date': '',
                        'Tariff_No': '',
                        'Rule_Nbr': str(companion_rule),
                        'Link': '',
                        'Sequence': '',
                        'SOS_Charge': 0,
                        'Sort_Price': sort_price,
                        'Fare_Direction': fare_direction,
                        'Trip_Direction': '',
                        'Inbound_Outbound': '',
                        'Discontinue_Date': '',
                        'In_Batch': '',
                        'OpenJaw': 'TRUE',
                        'RBD': rbd,
                        'Sector': sector
                    }
                    
                    companion_rows.append(output_row)
                    self.logger.debug(f"Generated {companion_rule} (footnote='{companion_footnote}') from {primary_rule} (base_footnote='{base_footnote_for_search}'): {primary_base_fare} -> {final_base_fare}")
            
            # Check if any primary rules were missing and fail the run if so
            if missing_primary_rules:
                self._handle_missing_primary_rules_failure(missing_primary_rules)
            
            return companion_rows
            
        except Exception as e:
            self.logger.error(f"Error generating companion rules: {str(e)}")
            self.logger.error(traceback.format_exc())
            return []
    
    def _handle_missing_primary_rules_failure(self, missing_primary_rules):
        """Handle failure when companion rules cannot be generated due to missing primary rules."""
        try:
            # Group missing rules by companion rule for better reporting
            missing_by_companion = {}
            for missing in missing_primary_rules:
                companion_rule = missing['companion_rule']
                if companion_rule not in missing_by_companion:
                    missing_by_companion[companion_rule] = []
                missing_by_companion[companion_rule].append(missing)
            
            # Build detailed error message
            error_lines = [
                "COMPANION RULE GENERATION FAILED",
                "=" * 50,
                "The following companion rules are configured but their corresponding primary rules are missing:",
                ""
            ]
            
            for companion_rule, missing_list in missing_by_companion.items():
                primary_rule = missing_list[0]['primary_rule']  # All will have same primary rule
                error_lines.append(f"Companion Rule {companion_rule} (requires Primary Rule {primary_rule}):")
                
                # Group by footnote for cleaner reporting
                by_footnote = {}
                for missing in missing_list:
                    footnote_key = missing['footnote'] if missing['footnote'] else 'EMPTY'
                    if footnote_key not in by_footnote:
                        by_footnote[footnote_key] = []
                    by_footnote[footnote_key].append(missing)
                
                for footnote_key, footnote_missing in by_footnote.items():
                    error_lines.append(f"  Footnote '{footnote_key}':")
                    
                    # Group by sector for even cleaner reporting
                    by_sector = {}
                    for missing in footnote_missing:
                        sector = missing['sector']
                        if sector not in by_sector:
                            by_sector[sector] = []
                        by_sector[sector].append(missing['rbd'])
                    
                    for sector, rbds in by_sector.items():
                        rbd_count = len(rbds)
                        rbd_sample = rbds[:3]  # Show first 3 RBDs
                        rbd_display = ', '.join(rbd_sample)
                        if rbd_count > 3:
                            rbd_display += f" ... (+{rbd_count - 3} more)"
                        
                        error_lines.append(f"    Sector {sector}: {rbd_count} RBDs ({rbd_display})")
                
                error_lines.append("")
            
            error_lines.extend([
                "RESOLUTION:",
                "1. Add the missing primary rules to your input data with matching footnotes, OR",
                "2. Remove the companion rule configurations that cannot be generated",
                "",
                "MISSING PRIMARY RULE DETAILS:",
                "The following primary rule combinations need to be added to your input:"
            ])
            
            # Add specific missing combinations for easier resolution
            for companion_rule, missing_list in missing_by_companion.items():
                primary_rule = missing_list[0]['primary_rule']
                error_lines.append(f"  For {companion_rule} generation, add {primary_rule} with:")
                
                by_footnote = {}
                for missing in missing_list:
                    footnote_key = missing['footnote'] if missing['footnote'] else 'EMPTY'
                    if footnote_key not in by_footnote:
                        by_footnote[footnote_key] = set()
                    by_footnote[footnote_key].add(missing['sector'])
                
                for footnote_key, sectors in by_footnote.items():
                    footnote_display = f"footnote='{footnote_key}'" if footnote_key != 'EMPTY' else "footnote='' (empty)"
                    sectors_display = ', '.join(sorted(sectors))
                    error_lines.append(f"    - {footnote_display} for sectors: {sectors_display}")
                
                error_lines.append("")
            
            error_lines.append(f"Total missing primary rule instances: {len(missing_primary_rules)}")
            
            error_message = "\n".join(error_lines)
            self.logger.error(error_message)
            
            # Raise exception to fail the run
            raise ValueError(f"Companion rule generation failed: {len(missing_primary_rules)} missing primary rule instances. "
                           f"See logs for detailed breakdown.")
            
        except Exception as e:
            # Fallback error handling
            self.logger.error(f"Error in failure handling: {str(e)}")
            raise ValueError(f"Companion rule generation failed due to missing primary rules. "
                           f"Count: {len(missing_primary_rules)}. Additional error in reporting: {str(e)}")
        
    def generate_saver_output(self):
        """Generate output rows for saver fares (rule 1001 and 2001)."""
        try:
            saver_rows = []
            
            # Get RBD ranges
            b2c_rbds = self.get_rbd_range('V9', 'Y0')  # 64 RBDs for B2C
            b2b_rbds = self.get_rbd_range('ZJ', 'Y0')  # 148 RBDs for B2B (includes B2C)
            
            # Process each saver fare
            for _, saver_row in self.saver_data.iterrows():
                sector = saver_row['sector']
                rbd = saver_row['rbd']
                allin_fare = saver_row['fare']  # This is ALL-IN FARE from saver table
                
                # Get POS from saver
                pos_value = saver_row.get('pos', 'B')
                if pd.isna(pos_value) or pos_value == '':
                    pos_value = 'B'
                
                # Get footnote from saver
                saver_footnote = saver_row.get('footnote', '')
                if pd.isna(saver_footnote):
                    saver_footnote = ''
                
                # Get NDO from saver
                saver_ndo = saver_row.get('ndo', 'X')
                if pd.isna(saver_ndo) or saver_ndo == '':
                    saver_ndo = 'X'
                
                # Extract base fare from all-in fare
                origin = sector[:3]
                destination = sector[3:]
                base_fare = round(self.calculate_all_in_fare(allin_fare, origin, destination, ftype="allin"))
                
                # Determine rule number(s) based on RBD
                rules_to_generate = []
                
                if rbd in b2c_rbds:
                    # B2C RBDs generate both 1001 and 2001
                    rules_to_generate.append('1001')
                    rules_to_generate.append('2001')
                elif rbd in b2b_rbds:
                    # B2B only RBDs (ZJ to Z0, excluding V9 to Y0) generate only 2001
                    rules_to_generate.append('2001')
                
                # Determine actual origin, destination and fare direction based on QP fares convention
                actual_origin, actual_destination, fare_direction = self.pa_helper._determine_fare_direction(sector)
                
                # Generate fare records for each applicable rule
                for rule_nbr in rules_to_generate:
                    # Generate farebasis with footnote and NDO
                    farebasis = self.generate_farebasis(rbd, rule_nbr, 'O', saver_ndo, sector, pos=pos_value, route_type='D', footnote=saver_footnote)
                    
                    # Create output row matching qp_fares schema
                    output_row = {
                        'Carrier_Code': 'QP',
                        'Action_Code': 'I',
                        'Origin': actual_origin,
                        'Destination': actual_destination,
                        'Fare_Basis_Code': farebasis,
                        'OW_RT': 'O',
                        'Routing': '',
                        'Footnote': saver_footnote,
                        'Currency': 'INR',
                        'Fare_Amount': base_fare,
                        'Point_Price': 0,
                        'Effective_Date': '',
                        'Tariff_No': '',
                        'Rule_Nbr': str(rule_nbr),
                        'Link': '',
                        'Sequence': '',
                        'SOS_Charge': 0,
                        'Sort_Price': base_fare,
                        'Fare_Direction': fare_direction,
                        'Trip_Direction': '',
                        'Inbound_Outbound': '',
                        'Discontinue_Date': '',
                        'In_Batch': '',
                        'OpenJaw': 'TRUE',
                        # Additional columns for reference
                        'RBD': rbd,
                        'Sector': sector
                    }
                    
                    saver_rows.append(output_row)
            
            return saver_rows
            
        except Exception as e:
            self.logger.error(f"Error generating saver output: {str(e)}")
            return []

    def generate_fare_ladder(self):
        """Generate complete fare ladder output."""
        try:
            return self.calculate_product_upsells()
            
        except Exception as e:
            self.logger.error(f"Error generating fare ladder: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise
 
    def run(self):
        """Main method to run complete fare filing process."""
        try:
            self.start_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            
            # Get current version information
            self.currentVersion = self.pa_helper.get_current_version()
            
            # Create initial audit record with IN_PROGRESS status
            self.run_id = str(uuid.uuid4())
            route = 'D'  # Assuming domestic for now
            run_type = "Saver" if self.isSaver else "Product Upsell"
            
            self.pa_helper.save_run_audit(
                self.currentVersion, route, self.analystName, 
                self.start_time, self.run_id, None, 
                run_type=run_type,
                sectors_list=None,
                status='IN_PROGRESS', is_initial=True
            )
            
            # Collect all data
            self.collect_all_data()
            
            # Generate fare ladder
            fare_ladder = self.generate_fare_ladder()
            
            # Get list of unique sectors processed
            sectors_list = fare_ladder['Sector'].unique().tolist() if not fare_ladder.empty else []
            
            # Save to database
            table_name = self.pa_helper.save_ladder_to_database(
                route, self.analystName, fare_ladder, self.run_id
            )
            
            # Update audit record with final results
            self.pa_helper.save_run_audit(
                self.currentVersion, route, self.analystName, 
                self.start_time, self.run_id, fare_ladder,
                run_type=run_type,
                sectors_list=sectors_list,
                status='SUCCESS'
            )
            
            # Log summary
            type_desc = "Saver Revision" if self.isSaver else "Product Upsell"
            
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"Fare Filing Complete - {type_desc}")
            self.logger.info(f"{'='*60}")
            self.logger.info(f"Total fares generated: {len(fare_ladder)}")
            self.logger.info(f"Unique sectors: {len(sectors_list)} - {sectors_list}")
            self.logger.info(f"Unique rule numbers: {fare_ladder['Rule_Nbr'].nunique()}")
            self.logger.info(f"Output saved to: {table_name}")
            self.logger.info(f"Run ID: {self.run_id}")
            
            return {
                'fare_ladder': fare_ladder,
                'table_name': table_name,
                'run_id': self.run_id,
                'type': type_desc,
                'run_type': run_type,
                'sectors': sectors_list,
                'summary': {
                    'total_fares': len(fare_ladder),
                    'unique_sectors': len(sectors_list),
                    'sectors_list': sectors_list,
                    'unique_rules': fare_ladder['Rule_Nbr'].nunique(),
                    'rule_breakdown': fare_ladder['Rule_Nbr'].value_counts().to_dict()
                }
            }
            
        except Exception as e:
            error_message = str(e)
            self.logger.error(f"Error in run method: {error_message}")
            self.logger.error(traceback.format_exc())
            
            # Update audit with FAILED status and failure reason
            try:
                self.pa_helper.save_run_audit(
                    self.currentVersion, route, self.analystName, 
                    self.start_time, self.run_id, None,
                    run_type=run_type if 'run_type' in locals() else "Unknown",
                    sectors_list=None,
                    status='FAILED', 
                    is_initial=False,
                    failure_reason=error_message
                )
            except Exception as audit_error:
                self.logger.error(f"Failed to update audit record: {str(audit_error)}")
            
            raise