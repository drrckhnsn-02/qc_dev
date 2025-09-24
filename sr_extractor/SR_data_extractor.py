#!/usr/bin/env python3
"""
Modified SR extractor to output Parquet files instead of JSON.
Outputs two files:
- summarydata.parquet: single-value fields (200+ fields)
- cycle_data.parquet: per-cycle arrays in long format

Extraction logic unchanged from sr_full_extractor_latest.py
"""

import re
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
import argparse
import pandas as pd
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CombinedDataExtractor:
    """Extracts both summarydata and cycle data from sequencing HTML reports."""
    
    def __init__(self, account: Optional[str] = None):
        """Initialize the extractor with field mappings.
        
        Args:
            account: Optional customer account name to include in extracted data
        """
        self.account = account
        
        # Field mapping dictionary for renaming extracted fields
        self.field_mapping = {
            'ChipProductivity(%)': 'chip_productivity_pct',
            'ImageArea': 'image_area',
            'TotalReads(M)': 'total_reads_m',
            'Q30(%)': 'q30_pct',
            'SplitRate(%)': 'split_rate_pct',
            'ESR(%)': 'esr_pct',
            'MaxOffsetX': 'max_offset_x',
            'MaxOffsetY': 'max_offset_y',
            'InitialOffsetX': 'initial_offset_x',
            'InitialOffsetY': 'initial_offset_y',
            'RecoverValue(A)': 'recover_value_a',
            'RecoverValue(C)': 'recover_value_c',
            'RecoverValue(G)': 'recover_value_g',
            'RecoverValue(T)': 'recover_value_t',
            'RecoverValue(AVG)': 'recover_value_avg',
            'R1 declining': 'r1_declining',
            'R2 declining': 'r2_declining',
            '3NreadsRate(%)': 'three_n_reads_rate_pct',
            'Runon1(%)': 'runon1_pct',
            'Runon2(%)': 'runon2_pct',
            'Lag1(%)': 'lag1_pct',
            'Lag2(%)': 'lag2_pct',
            'platform': 'platform',
            'readType': 'read_type',
            # REMOVED: 'reportTitle': 'report_title',
            'reportTime': 'report_time',
            'SoftwareVersion': 'software_version',
            'TemplateVersion': 'template_version',
            'Reference': 'reference',
            'CycleNumber': 'cycle_number',
            'Flow Cell ID': 'flow_cell_id',  # Now properly mapped
            'Machine ID': 'machine_id',
            'Sequence Type': 'sequence_type',
            'ISW Version': 'isw_version',
            'Recipe Version': 'recipe_version',
            'Sequence Start Date': 'sequence_start_date',
            'Sequence Start Time': 'sequence_start_time',
            'Sequencing Cartridge ID': '_temp_sequencing_cartridge_id',  # Temporary for parsing
            'Cleaning Cartridge ID': '_temp_cleaning_cartridge_id',  # Temporary for parsing
            'Flow Cell Pos': 'flow_cell_pos',
            'Barcode Type': 'barcode_type',
            'Barcode File': 'barcode_file',
            'Read1': 'read1',
            'Read2': 'read2',
            'Barcode': 'barcode',
            'Dual Barcode': 'dual_barcode',
            'Full Sequencing Cartridge ID': 'full_sequencing_cartridge_id',
            'Full Cleaning Cartridge ID': 'full_cleaning_cartridge_id',
            'Read1 Dark Reaction': 'read1_dark_reaction',
            'Read2 Dark Reaction': 'read2_dark_reaction',
            'Resume Cycles': 'resume_cycles',
            'Read1 Camera Digital Gains': 'read1_camera_digital_gains',
            'Read2 Camera Digital Gains': 'read2_camera_digital_gains'
        }
        
        # Define cycle data variable mappings (WITHOUT _cycles suffix)
        self.cycle_variables = {
            # Multi-series variables (ATCG)
            'signal': ['raw_intensity_A', 'raw_intensity_T', 
                      'raw_intensity_C', 'raw_intensity_G'],
            'background': ['background_A', 'background_T', 
                          'background_C', 'background_G'],
            'snr': ['snr_A', 'snr_T', 'snr_C', 'snr_G'],
            'rho': ['rho_intensity_A', 'rho_intensity_T', 'rho_intensity_C', 'rho_intensity_G'],  # Changed to rho_intensity
            
            # Single series variables
            'CycQ30': ['q30_unfiltered'],
            
            # Complex multi-series variables
            'movement': ['offset_a_x', 'offset_a_y', 'offset_c_x', 'offset_c_y',
                        'offset_g_x', 'offset_g_y', 'offset_t_x', 'offset_t_y'],
            'crosstalk': ['crosstalk_a_c', 'crosstalk_a_g', 'crosstalk_a_t',
                         'crosstalk_c_a', 'crosstalk_c_g', 'crosstalk_c_t',
                         'crosstalk_g_a', 'crosstalk_g_c', 'crosstalk_g_t',
                         'crosstalk_t_a', 'crosstalk_t_c', 'crosstalk_t_g'],
            
            # Dual series variables
            'BicFit': ['bic', 'fit'],
            
            # Multi-series ATCG variables with average
            'runon': ['runon_A', 'runon_T', 'runon_C', 
                     'runon_G', 'runon_avg'],
            'lag': ['lag_A', 'lag_T', 'lag_C', 
                   'lag_G', 'lag_avg'],
            
            # Discrepancy series
            'signalDiff': ['raw_intensity_dsc_A', 'raw_intensity_dsc_T', 
                          'raw_intensity_dsc_C', 'raw_intensity_dsc_G'],
            'backgroundDiff': ['background_dsc_A', 'background_dsc_T', 
                              'background_dsc_C', 'background_dsc_G'],
            'snrDiff': ['snr_dsc_A', 'snr_dsc_T', 'snr_dsc_C', 'snr_dsc_G'],
            'rhoDiff': ['rho_intensity_dsc_A', 'rho_intensity_dsc_T', 
                       'rho_intensity_dsc_C', 'rho_intensity_dsc_G'],  # Changed to rho_intensity
            'CycQ30Diff': ['q30_unfiltered_dsc'],
            'BicFitDiff': ['bic_dsc', 'fit_dsc'],
            'runonDiff': ['runon_dsc_A', 'runon_dsc_T', 'runon_dsc_C', 'runon_dsc_G'],
            'lagDiff': ['lag_dsc_A', 'lag_dsc_T', 'lag_dsc_C', 'lag_dsc_G'],
            
            # Variables that can be both summary and cycle data
            'baseTypeDist': ['base_distribution_A', 'base_distribution_T', 
                           'base_distribution_C', 'base_distribution_G', 
                           'base_distribution_N'],
            'qualPortion': ['quality_distribution_0_10', 'quality_distribution_10_20',
                          'quality_distribution_20_30', 'quality_distribution_30_40'],
            'gcDist': ['gc_content'],
            'estError': ['est_err_rate'],
            'qual': ['quality_avg']
        }
    
    def get_mapped_key(self, original_key: str) -> str:
        """Get the mapped field name if it exists."""
        return self.field_mapping.get(original_key, original_key)
    
    def parse_cartridge_ids_smart(self, non_full_id: str, full_id: str, cartridge_type: str) -> Dict[str, Any]:
        """
        Parse cartridge IDs using non-full ID as anchor for parsing full ID.
        
        Args:
            non_full_id: Format like "530-002079-00W0125051603051" ([pn][sn])
            full_id: Format like "530-002079-00W0037W01250516030515/16/2026 12:00:00 AM"
            cartridge_type: Either "sequencing_cartridge" or "cleaning_cartridge"
            
        Returns:
            Dictionary with parsed components
        """
        result = {}
        suffix = f"_{cartridge_type}"
        
        # Initialize all fields to None
        result[f"pn{suffix}"] = None
        result[f"ln{suffix}"] = None
        result[f"sn{suffix}"] = None
        result[f"ed{suffix}"] = None
        
        # Check if we have valid IDs to work with
        if not non_full_id or non_full_id in ['NULL', '']:
            logger.warning(f"Non-full {cartridge_type} ID is empty/NULL, cannot parse")
            return result
            
        if not full_id or full_id in ['NULL', '']:
            logger.warning(f"Full {cartridge_type} ID is empty/NULL, cannot parse")
            return result
        
        try:
            # Extract PN and SN from non-full ID
            # Format is [pn][sn] where pn is like "530-002079-00" and sn is like "W0125051603051"
            # Find the W that starts the serial number
            sn_match = re.search(r'(W\d+)', non_full_id)
            if not sn_match:
                logger.warning(f"Could not find SN (W-number) in non-full {cartridge_type} ID: {non_full_id}")
                return result
            
            sn = sn_match.group(1)
            pn = non_full_id[:sn_match.start()]  # Everything before the SN
            
            # Now use these to parse the full ID
            if pn not in full_id:
                logger.warning(f"PN '{pn}' not found in full {cartridge_type} ID: {full_id}")
                return result
                
            if sn not in full_id:
                logger.warning(f"SN '{sn}' not found in full {cartridge_type} ID: {full_id}")
                return result
            
            # Find positions in full ID
            pn_start = full_id.find(pn)
            pn_end = pn_start + len(pn)
            sn_start = full_id.find(sn)
            sn_end = sn_start + len(sn)
            
            # Extract LN (everything between PN and SN)
            ln = full_id[pn_end:sn_start]
            
            # Extract ED (date after SN, up to first whitespace)
            after_sn = full_id[sn_end:]
            # Look for date pattern
            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', after_sn)
            ed = date_match.group(1) if date_match else None
            
            if not ed:
                # Try to extract until first whitespace as fallback
                whitespace_pos = after_sn.find(' ')
                if whitespace_pos > 0:
                    ed = after_sn[:whitespace_pos]
                else:
                    ed = after_sn  # Use everything if no whitespace
            
            # Store parsed values
            result[f"pn{suffix}"] = pn
            result[f"ln{suffix}"] = ln
            result[f"sn{suffix}"] = sn
            result[f"ed{suffix}"] = ed
            
            logger.debug(f"Successfully parsed {cartridge_type}: PN={pn}, LN={ln}, SN={sn}, ED={ed}")
            
        except Exception as e:
            logger.warning(f"Failed to parse {cartridge_type} IDs: {e}")
            logger.warning(f"  Non-full ID: {non_full_id}")
            logger.warning(f"  Full ID: {full_id}")
        
        return result
    
    def add_with_duplicate_check(self, result_dict: Dict[str, Any], key: str, value: Any, source: str = ""):
        """Add key-value pair with duplicate checking and field mapping."""
        # Skip reportTitle completely
        if key == 'reportTitle':
            logger.debug(f"Skipping excluded field: {key}")
            return
        
        mapped_key = self.get_mapped_key(key)
        
        if mapped_key in result_dict:
            existing_value = result_dict[mapped_key]
            if existing_value == value:
                logger.debug(f"Skipping duplicate key '{mapped_key}' with identical value from {source}")
                return
            logger.error(f"DUPLICATE KEY CONFLICT: '{mapped_key}' already exists with different value")
            logger.error(f"  Existing value: {existing_value}")
            logger.error(f"  New value from {source}: {value}")
            raise ValueError(f"Duplicate key '{mapped_key}' found with conflicting values.")
        
        # Special handling for resume_cycles - wrap in list
        if mapped_key == 'resume_cycles' and value is not None:
            # Parse comma-separated string into list of integers
            value = [int(x.strip()) for x in value.split(',') if x.strip()]
        
        result_dict[mapped_key] = value
        logger.debug(f"Added key '{mapped_key}' (original: '{key}') from {source}")
    
    def extract_lane_from_filename(self, file_path: str) -> Optional[str]:
        """Extract lane number from filename (e.g., E####_L01_summaryReport.html)."""
        filename = Path(file_path).name
        lane_match = re.search(r'_L(\d+)', filename)
        
        if lane_match:
            lane_num = lane_match.group(1)
            return f"L{lane_num.zfill(2)}"  # Ensure it's L## format
        
        logger.warning(f"Could not extract lane from filename: {filename}")
        return None
    
    def extract_script_content(self, html_content: str) -> Optional[str]:
        """Extract JavaScript content between VALUES BEGIN and VALUES END markers."""
        # First find the script section after data.js comment
        data_js_pattern = r'<!-- data\.js -->.*?<script[^>]*>(.*?)</script>'
        data_js_match = re.search(data_js_pattern, html_content, re.DOTALL)
        
        if not data_js_match:
            logger.warning("Could not find data.js script section")
            return None
        
        script_content = data_js_match.group(1)
        
        # Extract only content between VALUES BEGIN and VALUES END
        values_pattern = r'// VALUES BEGIN(.*?)// VALUES END'
        values_match = re.search(values_pattern, script_content, re.DOTALL)
        
        if values_match:
            logger.info("Found JavaScript section between VALUES markers")
            return values_match.group(1)
        else:
            logger.warning("Could not find VALUES BEGIN/END markers, using full script")
            return script_content
    
    def extract_variables(self, script_content: str) -> Dict[str, Any]:
        """Extract JavaScript variables from script content."""
        variables = {}
        
        # Pattern to extract var statements (handles both simple and complex)
        var_pattern = r'var\s+(\w+)\s*=\s*(\{.*?\}|\[.*?\]|"[^"]*");'
        matches = re.findall(var_pattern, script_content, re.DOTALL)
        
        for var_name, var_content in matches:
            try:
                if var_content.startswith('"') and var_content.endswith('"'):
                    parsed_data = var_content[1:-1]  # String variable
                elif var_content.startswith('{') or var_content.startswith('['):
                    # Clean JavaScript to valid JSON
                    clean_content = var_content.replace("'", '"')
                    clean_content = re.sub(r'"\[(\d+-\d+)\)"', r'"\1"', clean_content)
                    clean_content = re.sub(r'"\[(\d+-\d+)\]"', r'"\1"', clean_content)
                    parsed_data = json.loads(clean_content)
                else:
                    parsed_data = var_content
                
                variables[var_name] = parsed_data
                logger.debug(f"Successfully parsed variable: {var_name}")
                
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse variable {var_name}: {e}")
                continue
        
        logger.info(f"Extracted {len(variables)} variables from script")
        return variables
    
    def process_simple_variables(self, variables: Dict[str, Any], result: Dict[str, Any]):
        """Extract simple string variables."""
        # Removed 'reportTitle' from this list
        simple_vars = ['platform', 'readType', 'reportTime']
        
        for var_name in simple_vars:
            if var_name in variables:
                self.add_with_duplicate_check(result, var_name, variables[var_name], var_name)
    
    def process_table_variables(self, variables: Dict[str, Any], result: Dict[str, Any]):
        """Process table variables (bioTable, summaryTable)."""
        table_vars = ['bioTable', 'summaryTable']
        
        for table_name in table_vars:
            if table_name in variables:
                table_data = variables[table_name]
                if isinstance(table_data, list) and len(table_data) > 1:
                    for row in table_data[1:]:
                        if isinstance(row, list) and len(row) >= 2:
                            key = row[0]
                            value = row[1]
                            if value == 'NULL' or value == '':
                                value = None
                            self.add_with_duplicate_check(result, key, value, table_name)
    
    def process_qc_table(self, variables: Dict[str, Any], result: Dict[str, Any]):
        """Process qcTable to extract DNB number."""
        if 'qcTable' in variables:
            qc_table = variables['qcTable']
            if isinstance(qc_table, list) and len(qc_table) > 1:
                data_row = qc_table[1]
                if isinstance(data_row, list) and len(data_row) > 0:
                    self.add_with_duplicate_check(result, 'dnb_number', data_row[0], 'qcTable')
    
    def process_phase_table(self, variables: Dict[str, Any], result: Dict[str, Any]):
        """Process phaseTable to extract phasing statistics."""
        if 'phaseTable' not in variables:
            return
        
        phase_table = variables['phaseTable']
        if not isinstance(phase_table, list):
            return
        
        read1_start_idx = None
        read2_start_idx = None
        
        for i, row in enumerate(phase_table):
            if isinstance(row, list) and len(row) > 0:
                if row[0] == 'Read1':
                    read1_start_idx = i + 1
                elif row[0] == 'Read2':
                    read2_start_idx = i + 1
        
        # Process Read1 data
        if read1_start_idx is not None:
            for i in range(read1_start_idx, min(read1_start_idx + 5, len(phase_table))):
                if i >= len(phase_table):
                    break
                row = phase_table[i]
                if isinstance(row, list) and len(row) >= 7:
                    base = row[0].lower() if row[0] != 'AVG' else 'avg'
                    if base == 'avg':
                        self.add_with_duplicate_check(result, 'r1_runon_pct', self._to_numeric(row[1]), 'phaseTable')
                        self.add_with_duplicate_check(result, 'r1_runon_intercept', self._to_numeric(row[2]), 'phaseTable')
                        self.add_with_duplicate_check(result, 'r1_runon_r2', self._to_numeric(row[3]), 'phaseTable')
                        self.add_with_duplicate_check(result, 'r1_lag_pct', self._to_numeric(row[4]), 'phaseTable')
                        self.add_with_duplicate_check(result, 'r1_lag_intercept', self._to_numeric(row[5]), 'phaseTable')
                        self.add_with_duplicate_check(result, 'r1_lag_r2', self._to_numeric(row[6]), 'phaseTable')
                    else:
                        self.add_with_duplicate_check(result, f'r1_{base}_runon_pct', self._to_numeric(row[1]), 'phaseTable')
                        self.add_with_duplicate_check(result, f'r1_{base}_runon_intercept', self._to_numeric(row[2]), 'phaseTable')
                        self.add_with_duplicate_check(result, f'r1_{base}_runon_r2', self._to_numeric(row[3]), 'phaseTable')
                        self.add_with_duplicate_check(result, f'r1_{base}_lag_pct', self._to_numeric(row[4]), 'phaseTable')
                        self.add_with_duplicate_check(result, f'r1_{base}_lag_intercept', self._to_numeric(row[5]), 'phaseTable')
                        self.add_with_duplicate_check(result, f'r1_{base}_lag_r2', self._to_numeric(row[6]), 'phaseTable')
        
        # Process Read2 data
        if read2_start_idx is not None:
            for i in range(read2_start_idx, min(read2_start_idx + 5, len(phase_table))):
                if i >= len(phase_table):
                    break
                row = phase_table[i]
                if isinstance(row, list) and len(row) >= 7:
                    base = row[0].lower() if row[0] != 'AVG' else 'avg'
                    if base == 'avg':
                        self.add_with_duplicate_check(result, 'r2_runon_pct', self._to_numeric(row[1]), 'phaseTable')
                        self.add_with_duplicate_check(result, 'r2_runon_intercept', self._to_numeric(row[2]), 'phaseTable')
                        self.add_with_duplicate_check(result, 'r2_runon_r2', self._to_numeric(row[3]), 'phaseTable')
                        self.add_with_duplicate_check(result, 'r2_lag_pct', self._to_numeric(row[4]), 'phaseTable')
                        self.add_with_duplicate_check(result, 'r2_lag_intercept', self._to_numeric(row[5]), 'phaseTable')
                        self.add_with_duplicate_check(result, 'r2_lag_r2', self._to_numeric(row[6]), 'phaseTable')
                    else:
                        self.add_with_duplicate_check(result, f'r2_{base}_runon_pct', self._to_numeric(row[1]), 'phaseTable')
                        self.add_with_duplicate_check(result, f'r2_{base}_runon_intercept', self._to_numeric(row[2]), 'phaseTable')
                        self.add_with_duplicate_check(result, f'r2_{base}_runon_r2', self._to_numeric(row[3]), 'phaseTable')
                        self.add_with_duplicate_check(result, f'r2_{base}_lag_pct', self._to_numeric(row[4]), 'phaseTable')
                        self.add_with_duplicate_check(result, f'r2_{base}_lag_intercept', self._to_numeric(row[5]), 'phaseTable')
                        self.add_with_duplicate_check(result, f'r2_{base}_lag_r2', self._to_numeric(row[6]), 'phaseTable')
    
    def process_fq_table(self, variables: Dict[str, Any], result: Dict[str, Any]):
        """Process fqTable to extract FASTQ statistics."""
        if 'fqTable' not in variables:
            return
        
        fq_table = variables['fqTable']
        if not isinstance(fq_table, list) or len(fq_table) <= 1:
            return
        
        for row in fq_table[1:]:
            if isinstance(row, list) and len(row) >= 10:
                category = row[0].lower()
                read_type = None
                
                if 'read1' in category:
                    read_type = 'r1'
                elif 'read2' in category:
                    read_type = 'r2'
                elif 'total' in category:
                    read_type = 'total'
                
                if read_type:
                    self.add_with_duplicate_check(result, f'{read_type}_phred_qual', row[1] if row[1] else None, 'fqTable')
                    self.add_with_duplicate_check(result, f'{read_type}_read_num', row[2] if row[2] else None, 'fqTable')
                    self.add_with_duplicate_check(result, f'{read_type}_base_num', row[3] if row[3] else None, 'fqTable')
                    self.add_with_duplicate_check(result, f'{read_type}_n_pct', self._to_numeric(row[4]), 'fqTable')
                    self.add_with_duplicate_check(result, f'{read_type}_gc_pct', self._to_numeric(row[5]), 'fqTable')
                    self.add_with_duplicate_check(result, f'{read_type}_q10_pct', self._to_numeric(row[6]), 'fqTable')
                    self.add_with_duplicate_check(result, f'{read_type}_q20_pct', self._to_numeric(row[7]), 'fqTable')
                    self.add_with_duplicate_check(result, f'{read_type}_q30_pct', self._to_numeric(row[8]), 'fqTable')
                    self.add_with_duplicate_check(result, f'{read_type}_est_err_pct', self._to_numeric(row[9]), 'fqTable')
    
    def process_summary_single_values(self, variables: Dict[str, Any], result: Dict[str, Any]):
        """Process single value objects for summary statistics."""
        # These will be summary values, not arrays
        single_value_vars = {
            'gcDist': 'gc_content',
            'estError': 'est_err_rate', 
            'qual': 'quality_avg'
        }
        
        for var_name, output_key in single_value_vars.items():
            if var_name in variables:
                data = variables[var_name]
                # Check if it's a single-value object (summary) vs array (cycle data)
                if isinstance(data, dict):
                    # Check if values are arrays (cycle data) or single values (summary)
                    first_value = next(iter(data.values())) if data else None
                    if first_value is not None and not isinstance(first_value, list):
                        # It's a summary value
                        if len(data) == 1:
                            data_key = list(data.keys())[0]
                            self.add_with_duplicate_check(result, output_key, data[data_key], var_name)
        
        # Handle qualPortion summary
        if 'qualPortion' in variables:
            data = variables['qualPortion']
            if isinstance(data, dict):
                # Check if it's summary data (single values) vs cycle data (arrays)
                first_value = next(iter(data.values())) if data else None
                if first_value is not None and not isinstance(first_value, list):
                    bin_mapping = {
                        '[0-10)': 'quality_distribution_0_10',
                        '[10-20)': 'quality_distribution_10_20', 
                        '[20-30)': 'quality_distribution_20_30',
                        '[30-40]': 'quality_distribution_30_40',
                        '0-10': 'quality_distribution_0_10',
                        '10-20': 'quality_distribution_10_20',
                        '20-30': 'quality_distribution_20_30', 
                        '30-40': 'quality_distribution_30_40'
                    }
                    
                    for bin_key, output_key in bin_mapping.items():
                        if bin_key in data and output_key not in result:
                            self.add_with_duplicate_check(result, output_key, data[bin_key], 'qualPortion')
        
        # Handle baseTypeDist summary
        if 'baseTypeDist' in variables:
            data = variables['baseTypeDist']
            if isinstance(data, dict):
                first_value = next(iter(data.values())) if data else None
                if first_value is not None and not isinstance(first_value, list):
                    base_mapping = {
                        'A': 'base_distribution_A',
                        'T': 'base_distribution_T',
                        'C': 'base_distribution_C', 
                        'G': 'base_distribution_G',
                        'N': 'base_distribution_N'
                    }
                    
                    for base_key, output_key in base_mapping.items():
                        if base_key in data:
                            self.add_with_duplicate_check(result, output_key, data[base_key], 'baseTypeDist')
    
    def process_special_cases(self, variables: Dict[str, Any], result: Dict[str, Any]):
        """Handle special case variables like barcodeSplitRate."""
        if 'barcodeSplitRate' in variables:
            value = variables['barcodeSplitRate']
            # Keep as string if it contains barcode names, otherwise convert
            if isinstance(value, dict):
                # Convert dict to JSON string for storage
                import json
                self.add_with_duplicate_check(result, 'barcodeSplitRate', json.dumps(value), 'barcodeSplitRate')
            else:
                # Keep as string (could be barcode names or other format)
                self.add_with_duplicate_check(result, 'barcodeSplitRate', str(value) if value else None, 'barcodeSplitRate')
    
    def process_cycle_variable(self, var_name: str, data: Dict[str, Any], result: Dict[str, Any]):
        """Process cycle data variables (arrays of measurements)."""
        if var_name not in self.cycle_variables:
            return
        
        mapping = self.cycle_variables[var_name]
        
        # Check if this is array data (cycle data) vs single values (summary)
        if isinstance(data, dict):
            first_value = next(iter(data.values())) if data else None
            if first_value is None or not isinstance(first_value, list):
                # Not cycle data, skip
                return
        
        if var_name in ['signal', 'background', 'snr', 'rho']:
            bases = ['A', 'T', 'C', 'G']
            for i, base in enumerate(bases):
                if base in data and isinstance(data[base], list):
                    result[mapping[i]] = data[base]
                    
        elif var_name == 'CycQ30':
            if 'Q30' in data and isinstance(data['Q30'], list):
                result['q30_unfiltered'] = data['Q30']
                
        elif var_name == 'movement':
            coords = ['A_X', 'A_Y', 'C_X', 'C_Y', 'G_X', 'G_Y', 'T_X', 'T_Y']
            for i, coord in enumerate(coords):
                if coord in data and isinstance(data[coord], list):
                    result[mapping[i]] = data[coord]
                    
        elif var_name == 'crosstalk':
            pairs = ['A_C', 'A_G', 'A_T', 'C_A', 'C_G', 'C_T',
                    'G_A', 'G_C', 'G_T', 'T_A', 'T_C', 'T_G']
            for i, pair in enumerate(pairs):
                if pair in data and isinstance(data[pair], list):
                    result[mapping[i]] = data[pair]
                    
        elif var_name == 'BicFit':
            if 'BIC' in data and isinstance(data['BIC'], list):
                result['bic'] = data['BIC']
            if 'FIT' in data and isinstance(data['FIT'], list):
                result['fit'] = data['FIT']
                
        elif var_name in ['runon', 'lag']:
            bases = ['A', 'T', 'C', 'G']
            for i, base in enumerate(bases):
                if base in data and isinstance(data[base], list):
                    result[mapping[i]] = data[base]
            if 'AVG' in data and isinstance(data['AVG'], list):
                result[mapping[4]] = data['AVG']
                
        elif var_name.endswith('Diff'):
            # Handle discrepancy variables
            if var_name in ['signalDiff', 'backgroundDiff', 'snrDiff', 'rhoDiff', 'runonDiff', 'lagDiff']:
                bases = ['A', 'T', 'C', 'G']
                for i, base in enumerate(bases):
                    if base in data and isinstance(data[base], list):
                        result[mapping[i]] = data[base]
            elif var_name == 'CycQ30Diff':
                if 'Q30' in data and isinstance(data['Q30'], list):
                    result['q30_unfiltered_dsc'] = data['Q30']
            elif var_name == 'BicFitDiff':
                if 'BIC' in data and isinstance(data['BIC'], list):
                    result['bic_dsc'] = data['BIC']
                if 'FIT' in data and isinstance(data['FIT'], list):
                    result['fit_dsc'] = data['FIT']
        
        # Handle the potentially dual-purpose variables
        elif var_name == 'baseTypeDist':
            bases = ['A', 'T', 'C', 'G', 'N']
            for i, base in enumerate(bases):
                if base in data and isinstance(data[base], list):
                    result[mapping[i]] = data[base]
                    
        elif var_name == 'qualPortion':
            bins = ['[0-10)', '[10-20)', '[20-30)', '[30-40]',
                   '0-10', '10-20', '20-30', '30-40']
            bin_mapping = {
                '[0-10)': 0, '0-10': 0,
                '[10-20)': 1, '10-20': 1,
                '[20-30)': 2, '20-30': 2,
                '[30-40]': 3, '30-40': 3
            }
            
            for bin_key, idx in bin_mapping.items():
                if bin_key in data and isinstance(data[bin_key], list):
                    if idx < len(mapping) and mapping[idx] not in result:
                        result[mapping[idx]] = data[bin_key]
                        
        elif var_name in ['gcDist', 'estError', 'qual']:
            # Single series cycle variables
            if isinstance(data, dict) and len(data) == 1:
                data_key = list(data.keys())[0]
                if isinstance(data[data_key], list):
                    result[mapping[0]] = data[data_key]
    
    def _to_numeric(self, value: str) -> Optional[float]:
        """Convert string to numeric value."""
        if not value or value == 'NULL':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def post_process_types(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Systematically convert string values to appropriate types.
        Handles numbers, dates, and special formats.
        """
        from datetime import datetime
        
        # Define specific integer fields
        INTEGER_FIELDS = {
            'read1', 'read2', 'barcode', 'dual_barcode', 'cycle_number',
            'dnb_number', 'r1_phred_qual', 'r1_read_num', 'r1_base_num',
            'r2_phred_qual', 'r2_read_num', 'r2_base_num',
            'total_phred_qual', 'total_read_num', 'total_base_num'
        }
        
        # Define specific float fields
        FLOAT_FIELDS = {
            'total_reads_m', 'max_offset_x', 'max_offset_y',
            'initial_offset_x', 'initial_offset_y',
            'recover_value_a', 'recover_value_c', 'recover_value_g',
            'recover_value_t', 'recover_value_avg',
            'r1_declining', 'r2_declining'
        }
        
        # Fields that contain JSON arrays as strings
        JSON_ARRAY_FIELDS = {
            'read1_camera_digital_gains', 'read2_camera_digital_gains'
        }
        
        # Date field patterns and their formats
        DATE_FORMATS = {
            'report_time': '%Y-%m-%d',  # Already in ISO format
            'sequence_start_date': '%Y-%m-%d',  # Already in ISO format
            'sequence_start_time': '%H:%M:%S',  # Time only, already good
        }
        
        for key, value in result.items():
            # Skip if None or already processed (lists) or account field
            if value is None or (isinstance(value, list) and key != 'resume_cycles') or key == 'account':
                continue
            
        # Handle JSON array fields (camera gains)
        # First, collect camera gain fields to process
        camera_gain_fields = {}
        for key, value in list(result.items()):  # Note: list() creates a copy to iterate over
            # ... other conversions stay the same ...
            
            # Handle JSON array fields (camera gains)
            if key in JSON_ARRAY_FIELDS and isinstance(value, str):
                try:
                    import json
                    gains = json.loads(value)
                    prefix = key.replace('_camera_digital_gains', '')
                    # Store individual camera gains
                    for i, gain in enumerate(gains, 1):
                        result[f"{prefix}_camera{i}_gain"] = int(gain)
                    # Mark for deletion
                    del result[key]
                    logger.debug(f"Split {key} into {len(gains)} camera gain fields")
                except Exception as e:
                    logger.warning(f"Could not parse {key}: {e}")

            # Handle expiration dates (M/D/YYYY format)
            elif key.startswith('ed_') and isinstance(value, str):
                try:
                    # Parse M/D/YYYY format
                    dt = datetime.strptime(value.split()[0], '%m/%d/%Y')
                    result[key] = dt.strftime('%Y-%m-%d')
                    logger.debug(f"Converted date {key}: {value} -> {result[key]}")
                except:
                    logger.warning(f"Could not parse date for {key}: {value}")
            
            # Handle other date/time fields
            elif key in DATE_FORMATS and isinstance(value, str):
                # These are already in correct format, just validate
                try:
                    if key == 'sequence_start_time':
                        # Validate time format
                        datetime.strptime(value, DATE_FORMATS[key])
                    else:
                        # Validate date format
                        datetime.strptime(value, DATE_FORMATS[key])
                    # Format is good, keep as is
                except:
                    logger.warning(f"Unexpected format for {key}: {value}")
            
            # Convert to integer
            elif key in INTEGER_FIELDS and isinstance(value, str):
                try:
                    result[key] = int(float(value))  # float first to handle "123.0"
                    logger.debug(f"Converted {key} to int: {result[key]}")
                except:
                    logger.warning(f"Could not convert {key} to integer: {value}")
            
            # Convert to float
            elif key in FLOAT_FIELDS and isinstance(value, str):
                try:
                    result[key] = float(value)
                    logger.debug(f"Converted {key} to float: {result[key]}")
                except:
                    logger.warning(f"Could not convert {key} to float: {value}")
            
            # Pattern-based conversions
            elif isinstance(value, str):
                # Percentage fields - store as decimals (0-1 range)
                if key.endswith('_pct'):
                    try:
                        val = float(value)
                        # If value is > 1, assume it's already in percentage form
                        if val > 1:
                            result[key] = val / 100.0
                        else:
                            result[key] = val
                        logger.debug(f"Converted {key} to decimal: {result[key]}")
                    except:
                        logger.warning(f"Could not convert percentage {key}: {value}")
                
                # Fields ending with _num should be integers
                elif key.endswith('_num'):
                    try:
                        result[key] = int(float(value))
                        logger.debug(f"Converted {key} to int: {result[key]}")
                    except:
                        logger.warning(f"Could not convert {key} to integer: {value}")
                
                # Fields ending with _m (millions) should be floats
                elif key.endswith('_m'):
                    try:
                        result[key] = float(value)
                        logger.debug(f"Converted {key} to float: {result[key]}")
                    except:
                        logger.warning(f"Could not convert {key} to float: {value}")
                
                # Fields with offset, intercept, or r2 in name should be floats
                elif any(pattern in key for pattern in ['offset', 'intercept', 'r2', 'value']):
                    try:
                        result[key] = float(value)
                        logger.debug(f"Converted {key} to float: {result[key]}")
                    except:
                        # Not all fields with these patterns are numeric
                        pass
        
        return result
    
    def split_summarydata_and_cycles(self, data: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Split the extracted data into summarydata and cycle data.
        
        Returns:
            Tuple of (summarydata_dict, cycle_data_list)
            - summarydata_dict: All single-value fields
            - cycle_data_list: List of dicts with flow_cell_id, lane, metric_name, values
        """
        summarydata = {}
        cycle_data_list = []
        
        # Get flow_cell_id and lane for cycle data records
        flow_cell_id = data.get('flow_cell_id')
        lane = data.get('lane')
        
        for key, value in data.items():
            if isinstance(value, list) and key != 'resume_cycles':
                # It's cycle data - add to cycle_data_list
                cycle_data_list.append({
                    'flow_cell_id': flow_cell_id,
                    'lane': lane,
                    'metric_name': key,
                    'values': value
                })
            else:
                # It's summarydata
                summarydata[key] = value
        
        return summarydata, cycle_data_list
    
    def extract_all_data(self, html_file_path: str) -> Dict[str, Any]:
        """
        Main extraction method to process HTML file and extract all data.
        
        Args:
            html_file_path: Path to HTML file
            
        Returns:
            Dictionary with account (if provided), flow_cell_id, summarydata, and cycle data
        """
        # Read HTML file
        with open(html_file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Extract lane from filename
        lane = self.extract_lane_from_filename(html_file_path)
        
        # Extract JavaScript content (between VALUES markers)
        script_content = self.extract_script_content(html_content)
        if not script_content:
            logger.error("Failed to extract JavaScript content")
            return {}
        
        # Extract variables
        variables = self.extract_variables(script_content)
        
        # Initialize result with account if provided
        result = {}
        if self.account:
            result['account'] = self.account
            logger.info(f"Added account field: {self.account}")
        
        # Process summarydata/summary statistics first
        logger.info("Processing summarydata and summary statistics...")
        self.process_simple_variables(variables, result)
        self.process_table_variables(variables, result)
        self.process_qc_table(variables, result)
        self.process_phase_table(variables, result)
        self.process_fq_table(variables, result)
        self.process_summary_single_values(variables, result)
        self.process_special_cases(variables, result)
        
        # Add lane after flow_cell_id
        if 'flow_cell_id' in result:
            # Create new ordered dict with lane after flow_cell_id
            new_result = {}
            for k, v in result.items():
                new_result[k] = v
                if k == 'flow_cell_id':
                    new_result['lane'] = lane
            result = new_result
        
        # Process cartridge IDs
        # Extract the non-full and full IDs
        seq_non_full = result.get('_temp_sequencing_cartridge_id')
        seq_full = result.get('full_sequencing_cartridge_id')
        clean_non_full = result.get('_temp_cleaning_cartridge_id')
        clean_full = result.get('full_cleaning_cartridge_id')
        
        # Parse sequencing cartridge
        if seq_non_full and seq_full:
            parsed_seq = self.parse_cartridge_ids_smart(seq_non_full, seq_full, 'sequencing_cartridge')
            # Insert parsed fields before full ID
            if 'full_sequencing_cartridge_id' in result:
                # Create new ordered dict with parsed fields before full ID
                new_result = {}
                for k, v in result.items():
                    if k == 'full_sequencing_cartridge_id':
                        # Insert parsed fields first
                        for parsed_k, parsed_v in parsed_seq.items():
                            new_result[parsed_k] = parsed_v
                    new_result[k] = v
                result = new_result
            else:
                # Just add them
                result.update(parsed_seq)
        
        # Parse cleaning cartridge
        if clean_non_full and clean_full:
            parsed_clean = self.parse_cartridge_ids_smart(clean_non_full, clean_full, 'cleaning_cartridge')
            # Insert parsed fields before full ID
            if 'full_cleaning_cartridge_id' in result:
                # Create new ordered dict with parsed fields before full ID
                new_result = {}
                for k, v in result.items():
                    if k == 'full_cleaning_cartridge_id':
                        # Insert parsed fields first
                        for parsed_k, parsed_v in parsed_clean.items():
                            new_result[parsed_k] = parsed_v
                    new_result[k] = v
                result = new_result
            else:
                # Just add them
                result.update(parsed_clean)
        
        # Remove temporary fields
        if '_temp_sequencing_cartridge_id' in result:
            del result['_temp_sequencing_cartridge_id']
        if '_temp_cleaning_cartridge_id' in result:
            del result['_temp_cleaning_cartridge_id']
        
        # Process cycle data
        logger.info("Processing cycle data arrays...")
        for var_name in self.cycle_variables:
            if var_name in variables:
                self.process_cycle_variable(var_name, variables[var_name], result)
                logger.debug(f"Processed cycle variable: {var_name}")
        
        # Apply post-processing to convert types
        logger.info("Applying type conversions...")
        result = self.post_process_types(result)
        
        # Count different types of data
        summarydata_count = len([k for k, v in result.items() 
                             if k not in ['flow_cell_id', 'account'] and not isinstance(v, list)])
        array_count = len([k for k, v in result.items() 
                          if isinstance(v, list)])
        
        logger.info(f"Extraction complete: {summarydata_count} summarydata items, {array_count} arrays")
        
        return result
    
    def save_to_parquet(self, data: Dict[str, Any], output_base_path: str):
        """
        Save extracted data to two Parquet files.
        
        Args:
            data: Extracted data dictionary
            output_base_path: Base path for output (without extension)
                             Will create _summarydata.parquet and _cycle_data.parquet
        """
        # Split data into summarydata and cycles
        summarydata, cycle_data_list = self.split_summarydata_and_cycles(data)
        
        # Create output paths
        output_base = Path(output_base_path)
        summarydata_path = output_base.parent / f"{output_base.stem}_summarydata.parquet"
        cycle_data_path = output_base.parent / f"{output_base.stem}_cycle_data.parquet"
        
        # Save summarydata
        if summarydata:
            # Convert summarydata dict to DataFrame (single row)
            summarydata_df = pd.DataFrame([summarydata])
            summarydata_df.to_parquet(summarydata_path, engine='pyarrow', compression='snappy')
            logger.info(f"Saved summarydata to {summarydata_path} ({len(summarydata)} fields)")
        
        # Save cycle data
        if cycle_data_list:
            # Convert cycle data to DataFrame
            cycle_data_df = pd.DataFrame(cycle_data_list)
            cycle_data_df.to_parquet(cycle_data_path, engine='pyarrow', compression='snappy')
            logger.info(f"Saved cycle data to {cycle_data_path} ({len(cycle_data_list)} arrays)")
    
    def validate_extraction(self, data: Dict[str, Any]) -> bool:
        """Validate extracted data for completeness and correctness."""
        is_valid = True
        
        # Check for flow_cell_id
        if not data.get('flow_cell_id'):
            logger.error("Missing flow_cell_id")
            is_valid = False
        
        # Count different types of data
        summarydata_count = 0
        array_count = 0
        array_lengths = {}
        
        for key, value in data.items():
            if key in ['flow_cell_id', 'account']:
                continue
            elif isinstance(value, list) and key != 'resume_cycles':  # resume_cycles is summarydata wrapped in list
                array_count += 1
                length = len(value)
                if length not in array_lengths:
                    array_lengths[length] = []
                array_lengths[length].append(key)
            else:
                summarydata_count += 1
        
        if summarydata_count == 0 and array_count == 0:
            logger.error("No data extracted")
            is_valid = False
        else:
            logger.info(f"Found {summarydata_count} summarydata items and {array_count} data arrays")
        
        # Log array length distribution
        for length, keys in sorted(array_lengths.items()):
            logger.info(f"Arrays with length {length}: {len(keys)} arrays")
            if len(keys) <= 3:
                logger.info(f"  Keys: {keys}")
            else:
                logger.info(f"  Sample keys: {keys[:3]}...")
        
        # Check for expected summarydata fields
        expected_summarydata = ['platform', 'machine_id', 'sequence_type']
        missing_expected = [field for field in expected_summarydata 
                           if field not in data]
        if missing_expected:
            logger.warning(f"Missing expected summarydata fields: {missing_expected}")
        
        return is_valid


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Extract summarydata, summary stats, and cycle data from HTML reports to Parquet'
    )
    parser.add_argument('input_html', help='Path to input HTML file')
    parser.add_argument('output_base', help='Base path for output files (without extension)')
    parser.add_argument('--account', help='Customer account name to include in output')
    parser.add_argument('--validate', action='store_true', 
                       help='Validate extraction results')
    parser.add_argument('--debug', action='store_true', 
                       help='Enable debug logging')
    parser.add_argument('--summary-only', action='store_true',
                       help='Extract only summarydata and summary statistics (no cycle data)')
    parser.add_argument('--cycles-only', action='store_true',
                       help='Extract only cycle data arrays (no summarydata)')
    
    args = parser.parse_args()
    
    # Configure logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Check for conflicting options
    if args.summary_only and args.cycles_only:
        logger.error("Cannot use both --summary-only and --cycles-only")
        return 1
    
    # Create extractor instance with optional account
    extractor = CombinedDataExtractor(account=args.account)
    
    # Extract data
    logger.info(f"Processing file: {args.input_html}")
    if args.account:
        logger.info(f"Account: {args.account}")
    
    if args.summary_only or args.cycles_only:
        # Partial extraction - would need to modify extract_all_data
        # For now, extract everything and filter
        extracted_data = extractor.extract_all_data(args.input_html)
        
        if args.summary_only:
            # Filter out arrays (cycle data) except resume_cycles
            filtered_data = {k: v for k, v in extracted_data.items() 
                           if not isinstance(v, list) or k in ['account', 'barcodeSplitRate', 'resume_cycles']}
            extracted_data = filtered_data
            logger.info("Filtered to summarydata and summary statistics only")
            
        elif args.cycles_only:
            # Keep only arrays and essential identifiers
            filtered_data = {k: extracted_data.get(k) for k in ['account', 'flow_cell_id', 'lane'] if k in extracted_data}
            filtered_data.update({k: v for k, v in extracted_data.items() 
                                 if isinstance(v, list) and k != 'resume_cycles'})
            extracted_data = filtered_data
            logger.info("Filtered to cycle data arrays only")
    else:
        extracted_data = extractor.extract_all_data(args.input_html)
    
    # Validate if requested
    if args.validate:
        if extractor.validate_extraction(extracted_data):
            logger.info("Validation passed")
        else:
            logger.error("Validation failed")
            return 1
    
    # Save to Parquet
    extractor.save_to_parquet(extracted_data, args.output_base)
    
    # Print summary
    summarydata_count = len([k for k, v in extracted_data.items() 
                         if not isinstance(v, list) or k == 'resume_cycles'])
    array_count = len([k for k, v in extracted_data.items() 
                      if isinstance(v, list) and k != 'resume_cycles'])
    
    print(f"\nExtraction Summary:")
    if extracted_data.get('account'):
        print(f"  Account: {extracted_data.get('account')}")
    print(f"  Flowcell ID: {extracted_data.get('flow_cell_id', 'Not found')}")
    print(f"  Lane: {extracted_data.get('lane', 'Not found')}")
    print(f"  summarydata items: {summarydata_count}")
    print(f"  Cycle data arrays: {array_count}")
    print(f"  Output files:")
    print(f"    - {Path(args.output_base).stem}_summarydata.parquet")
    print(f"    - {Path(args.output_base).stem}_cycle_data.parquet")
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())