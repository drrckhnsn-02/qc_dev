#!/usr/bin/env python3
"""
ACHM (All Cycle Heatmap) Data Extractor V2
Extracts cycle-based heatmap data from allCycleHeatmap.html files
Outputs to HDF5 files with proper chunking support
Fixed version handling both commented and uncommented option arrays
"""

import re
import h5py
import numpy as np
import logging
import json
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import argparse
from datetime import datetime
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ACHMExtractorV2:
    """Extract heatmap data from allCycleHeatmap.html files - Version 2."""
    
    def __init__(self, account: Optional[str] = None, output_file: str = None):
        """Initialize the extractor.
        
        Args:
            account: Optional customer account name
            output_file: HDF5 output file path
        """
        self.account = account
        self.output_file = output_file
        
        # Metric name mapping (original -> standardized lowercase with underscores)
        self.metric_mapping = {
            'LoadedDNB': 'loaded_dnb',
            'Background_A': 'background_a',
            'Background_C': 'background_c',
            'Background_G': 'background_g',
            'Background_T': 'background_t',
            'RHO_A': 'rho_a',
            'RHO_C': 'rho_c',
            'RHO_G': 'rho_g',
            'RHO_T': 'rho_t',
            'Q30': 'q30',
            'Lag_A': 'lag_a',
            'Lag_C': 'lag_c',
            'Lag_G': 'lag_g',
            'Lag_T': 'lag_t',
            'Runon_A': 'runon_a',
            'Runon_C': 'runon_c',
            'Runon_G': 'runon_g',
            'Runon_T': 'runon_t',
            'Offset_A_X': 'offset_a_x',
            'Offset_A_Y': 'offset_a_y',
            'Offset_C_X': 'offset_c_x',
            'Offset_C_Y': 'offset_c_y',
            'Offset_G_X': 'offset_g_x',
            'Offset_G_Y': 'offset_g_y',
            'Offset_T_X': 'offset_t_x',
            'Offset_T_Y': 'offset_t_y',
            'Signal_A': 'signal_a',
            'Signal_C': 'signal_c',
            'Signal_G': 'signal_g',
            'Signal_T': 'signal_t',
            'SNR_A': 'snr_a',
            'SNR_C': 'snr_c',
            'SNR_G': 'snr_g',
            'SNR_T': 'snr_t',
            'BIC': 'bic',
            'Fit': 'fit',
            'A-T': 'crosstalk_a_t',
            'G-C': 'crosstalk_g_c'
        }
    
    def extract_flow_cell_and_lane(self, file_path: str) -> Tuple[str, str]:
        """Extract flow_cell_id and lane from filename.
        
        Args:
            file_path: Path to the HTML file
            
        Returns:
            Tuple of (flow_cell_id, lane)
        """
        filename = Path(file_path).name
        
        # Pattern: [letters][digits]_L[digits].allCycleHeatmap.html
        patterns = [
            r'([A-Z]+\d+)_L(\d+)\.allCycleHeatmap\.html',
            r'([^_]+)_L(\d+)\..*\.html'
        ]
        
        for pattern in patterns:
            match = re.match(pattern, filename, re.IGNORECASE)
            if match:
                flow_cell_id = match.group(1)
                lane = f"L{match.group(2).zfill(2)}"
                logger.debug(f"Extracted flow_cell_id={flow_cell_id}, lane={lane} from {filename}")
                return flow_cell_id, lane
        
        logger.warning(f"Could not extract flow_cell_id and lane from filename: {filename}")
        return "UNKNOWN", "L00"
    
    def extract_script_content(self, html_content: str) -> Optional[str]:
        """Extract JavaScript content between VALUES BEGIN and VALUES END markers."""
        values_pattern = r'// VALUES BEGIN(.*?)// VALUES END'
        values_match = re.search(values_pattern, html_content, re.DOTALL)
        
        if values_match:
            return values_match.group(1)
        else:
            logger.error("Could not find VALUES BEGIN/END markers")
            return None
    
    def parse_option_array(self, script_content: str) -> Tuple[List[str], bool]:
        """Parse the option array to get metric names.
        
        Returns:
            Tuple of (options list, is_full_version)
        """
        # Look for both commented and uncommented versions
        commented_pattern = r'^\s*//var option = (\[.*?\]);'
        uncommented_pattern = r'^\s*var option = (\[.*?\]);'
        
        commented_matches = re.findall(commented_pattern, script_content, re.MULTILINE)
        uncommented_matches = re.findall(uncommented_pattern, script_content, re.MULTILINE)
        
        options_to_try = []
        
        # Add commented versions first (likely to be full)
        for match in commented_matches:
            options_to_try.append((match, True))
        
        # Add uncommented versions
        for match in uncommented_matches:
            options_to_try.append((match, False))
        
        if not options_to_try:
            logger.error("Could not find any option array")
            return [], False
        
        # Try each option array, preferring the longest one
        parsed_options = []
        
        for option_str, is_commented in options_to_try:
            try:
                option_str = option_str.replace("'", '"')
                options = json.loads(option_str)
                parsed_options.append((options, is_commented))
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse option array: {e}")
                continue
        
        if not parsed_options:
            logger.error("Could not parse any option arrays")
            return [], False
        
        # Choose the longest option array
        longest_options = max(parsed_options, key=lambda x: len(x[0]))
        options, is_commented = longest_options
        
        logger.info(f"Using {'commented' if is_commented else 'uncommented'} option array with {len(options)} metrics")
        
        # Validate metric count
        if not self.validate_metric_count(script_content, len(options)):
            logger.warning("Metric count mismatch, trying alternative arrays")
            for other_options, other_commented in parsed_options:
                if len(other_options) != len(options):
                    if self.validate_metric_count(script_content, len(other_options)):
                        logger.info(f"Using alternative array with {len(other_options)} metrics")
                        return other_options, True
            
            logger.warning("No perfect match found, using best guess")
        
        return options, True
    
    def validate_metric_count(self, script_content: str, expected_metrics: int) -> bool:
        """Validate that the number of metrics matches data values per row."""
        fov_pattern = r'var fovData = \{(.*?)\};'
        fov_match = re.search(fov_pattern, script_content, re.DOTALL)
        
        if not fov_match:
            return True  # Can't validate, assume OK
        
        fov_str = fov_match.group(1)
        row_pattern = r'\["[^"]+",\s*([\d\.,\s\-]+)\]'
        row_match = re.search(row_pattern, fov_str)
        
        if not row_match:
            return True  # Can't validate, assume OK
        
        values_str = row_match.group(1)
        values = values_str.split(',')
        
        numeric_count = sum(1 for val in values if val.strip() and val.strip() != 'null')
        
        logger.debug(f"Validation: Expected {expected_metrics}, found {numeric_count} values")
        return numeric_count == expected_metrics
    
    def parse_fov_data(self, script_content: str) -> Dict[str, List[List[Any]]]:
        """Parse the fovData object containing all cycle data."""
        fov_pattern = r'var fovData = \{(.*?)\};'
        fov_match = re.search(fov_pattern, script_content, re.DOTALL)
        
        if not fov_match:
            logger.error("Could not find fovData")
            return {}
        
        fov_str = '{' + fov_match.group(1) + '}'
        
        try:
            # Clean JavaScript to JSON
            fov_str = fov_str.replace("'", '"')
            fov_str = re.sub(r'\[\"([^"]+)\"', r'["\1"', fov_str)
            fov_data = json.loads(fov_str)
            logger.info(f"Successfully parsed fovData with {len(fov_data)} cycles")
            return fov_data
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse fovData as JSON: {e}")
            return self.parse_fov_data_manual(fov_str)
    
    def parse_fov_data_manual(self, fov_str: str) -> Dict[str, List[List[Any]]]:
        """Manually parse fovData if JSON parsing fails."""
        logger.info("Using manual parsing for fovData")
        
        fov_data = {}
        cycle_pattern = r'"(C\d+)":\s*\[(.*?)\](?=,\s*"C\d+":|$)'
        cycles = re.findall(cycle_pattern, fov_str, re.DOTALL)
        
        for cycle_id, cycle_content in cycles:
            rows = []
            row_pattern = r'\[(.*?)\]'
            row_matches = re.findall(row_pattern, cycle_content)
            
            for row_str in row_matches:
                if not row_str.strip():
                    continue
                    
                values = []
                parts = row_str.split(',')
                
                for i, part in enumerate(parts):
                    part = part.strip()
                    if i == 0:  # Row ID
                        part = part.strip('"')
                        values.append(part)
                    else:
                        try:
                            values.append(float(part))
                        except ValueError:
                            values.append(None)
                
                if len(values) > 1:
                    rows.append(values)
            
            if rows:
                fov_data[cycle_id] = rows
        
        logger.info(f"Manually parsed {len(fov_data)} cycles")
        return fov_data
    
    def convert_to_arrays(self, fov_data: Dict, options: List[str]) -> Tuple[np.ndarray, Dict]:
        """Convert parsed data to numpy arrays.
        
        Returns:
            Tuple of (4D data array, metadata dict)
        """
        cycle_keys = sorted(fov_data.keys(), key=lambda x: int(re.findall(r'\d+', x)[0]))
        num_cycles = len(cycle_keys)
        
        if num_cycles == 0:
            raise ValueError("No cycle data found")
        
        # Get grid dimensions
        first_cycle_rows = fov_data[cycle_keys[0]]
        max_row = 0
        max_col = 0
        
        for row_data in first_cycle_rows:
            row_id = row_data[0]
            match = re.match(r'C(\d+)R(\d+)', row_id, re.IGNORECASE)
            if match:
                col_idx = int(match.group(1))
                row_idx = int(match.group(2))
                max_col = max(max_col, col_idx)
                max_row = max(max_row, row_idx)
        
        num_metrics = len(options)
        
        logger.info(f"Data dimensions: {max_row} rows x {max_col} cols x {num_cycles} cycles x {num_metrics} metrics")
        
        # Initialize 4D array (rows, cols, cycles, metrics)
        data_array = np.full((max_row, max_col, num_cycles, num_metrics), np.nan, dtype=np.float32)
        
        # Fill the array
        populated_count = 0
        total_positions = 0
        
        for cycle_idx, cycle_key in enumerate(cycle_keys):
            cycle_data = fov_data[cycle_key]
            
            for row_data in cycle_data:
                total_positions += 1
                row_id = row_data[0]
                values = row_data[1:]
                
                match = re.match(r'C(\d+)R(\d+)', row_id, re.IGNORECASE)
                if match:
                    col_idx = int(match.group(1)) - 1  # 0-based
                    row_idx = int(match.group(2)) - 1
                    
                    if row_idx >= max_row or col_idx >= max_col:
                        continue
                    
                    if len(values) != num_metrics:
                        raise ValueError(f"Metric count mismatch at {row_id}")
                    
                    try:
                        data_array[row_idx, col_idx, cycle_idx, :] = values
                        populated_count += 1
                    except Exception as e:
                        logger.error(f"Failed to store values: {e}")
        
        logger.info(f"Populated {populated_count}/{total_positions} positions ({populated_count/total_positions*100:.1f}%)")
        
        non_nan_count = np.sum(~np.isnan(data_array))
        total_elements = data_array.size
        logger.info(f"Non-NaN elements: {non_nan_count}/{total_elements} ({non_nan_count/total_elements*100:.1f}%)")
        
        if non_nan_count == 0:
            raise ValueError("No data was successfully populated!")
        
        # Create metadata
        metadata = {
            'num_cycles': num_cycles,
            'num_rows': max_row,
            'num_cols': max_col,
            'num_metrics': num_metrics,
            'cycle_names': cycle_keys,
            'populated_positions': populated_count,
            'total_positions': total_positions
        }
        
        return data_array, metadata
    
    def save_to_hdf5(self, data_array: np.ndarray, options: List[str], 
                     metadata: Dict[str, Any], flow_cell_id: str, lane: str) -> int:
        """Save data to HDF5 file.
        
        Returns:
            Size of data added in bytes
        """
        start_time = time.time()
        fc_key = f"{flow_cell_id}_{lane}"
        
        # Get file size before writing
        file_size_before = Path(self.output_file).stat().st_size if Path(self.output_file).exists() else 0
        
        with h5py.File(self.output_file, 'a') as f:
            # Create/update global metadata
            if 'metadata' not in f:
                global_meta = f.create_group('metadata')
                global_meta.attrs['created'] = datetime.now().isoformat()
                global_meta.attrs['extractor_version'] = '2.0'
                global_meta.create_dataset('flow_cells', shape=(0,), maxshape=(None,), dtype=h5py.string_dtype())
            else:
                global_meta = f['metadata']
            
            # Update flow cell list
            fc_list = list(global_meta['flow_cells'][:])
            if fc_key.encode() not in fc_list:
                fc_list.append(fc_key.encode())
                del global_meta['flow_cells']
                global_meta.create_dataset('flow_cells', data=fc_list)
            
            # Create flow_cells group if needed
            if 'flow_cells' not in f:
                f.create_group('flow_cells')
            
            fc_group = f['flow_cells']
            
            # Check if already exists
            if fc_key in fc_group:
                logger.warning(f"Flow cell {fc_key} already exists, skipping")
                return 0
            
            # Create group for this flow cell
            fc_entry = fc_group.create_group(fc_key)
            
            # Add metadata
            fc_meta = fc_entry.create_group('metadata')
            fc_meta.attrs['flow_cell_id'] = flow_cell_id
            fc_meta.attrs['lane'] = lane
            if self.account:
                fc_meta.attrs['account'] = self.account
            fc_meta.attrs['num_cycles'] = metadata['num_cycles']
            fc_meta.attrs['num_rows'] = metadata['num_rows']
            fc_meta.attrs['num_cols'] = metadata['num_cols']
            fc_meta.attrs['num_metrics'] = metadata['num_metrics']
            fc_meta.attrs['extraction_date'] = datetime.now().isoformat()
            
            # Store metric names
            fc_meta.create_dataset('metrics', data=[m.encode() for m in options])
            fc_meta.create_dataset('metrics_standardized', 
                                  data=[self.metric_mapping.get(m, m.lower()).encode() for m in options])
            
            # Create data group
            data_group = fc_entry.create_group('data')
            
            # Save each metric as a separate dataset
            for metric_idx, metric_name in enumerate(options):
                std_name = self.metric_mapping.get(metric_name, metric_name.lower())
                metric_data = data_array[:, :, :, metric_idx]
                
                ds = data_group.create_dataset(
                    std_name,
                    data=metric_data,
                    compression='gzip',
                    compression_opts=6,  # Balanced compression
                    shuffle=True,
                    chunks=True
                )
                ds.attrs['dimensions'] = 'rows × cols × cycles'
            
            f.flush()
        
        # Calculate size added
        file_size_after = Path(self.output_file).stat().st_size
        size_added = file_size_after - file_size_before
        
        save_time = time.time() - start_time
        logger.info(f"Added {fc_key} to {self.output_file}")
        logger.info(f"  Save time: {save_time:.1f} seconds")
        logger.info(f"  Size added: {size_added / (1024**2):.1f} MB")
        
        return size_added
    
    def extract_and_save(self, html_file_path: str) -> Dict[str, Any]:
        """Process HTML file and save to HDF5.
        
        Returns:
            Metadata dictionary with size_added field
        """
        logger.info(f"Processing: {html_file_path}")
        
        # Extract identifiers
        flow_cell_id, lane = self.extract_flow_cell_and_lane(html_file_path)
        
        # Read HTML
        with open(html_file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        file_size_mb = len(html_content) / (1024 * 1024)
        logger.info(f"HTML file size: {file_size_mb:.1f} MB")
        
        # Extract script content
        script_content = self.extract_script_content(html_content)
        if not script_content:
            raise ValueError("Failed to extract data from HTML")
        
        # Parse option array
        options, is_full = self.parse_option_array(script_content)
        if not options:
            raise ValueError("Failed to parse metric names")
        
        # Parse fovData
        logger.info("Parsing fovData...")
        fov_data = self.parse_fov_data(script_content)
        if not fov_data:
            raise ValueError("Failed to parse fov data")
        
        # Convert to arrays
        logger.info("Converting to arrays...")
        data_array, metadata = self.convert_to_arrays(fov_data, options)
        
        # Add identifiers
        metadata['flow_cell_id'] = flow_cell_id
        metadata['lane'] = lane
        if self.account:
            metadata['account'] = self.account
        
        # Save to HDF5
        if not self.output_file:
            raise ValueError("No output file specified")
            
        size_added = self.save_to_hdf5(data_array, options, metadata, flow_cell_id, lane)
        metadata['size_added'] = size_added
        
        return metadata


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Extract heatmap data from allCycleHeatmap.html files into HDF5 (v2)'
    )
    parser.add_argument('input_html', help='Path to input HTML file')
    parser.add_argument('output_h5', help='Path to HDF5 file')
    parser.add_argument('--account', help='Customer account name')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create extractor
    extractor = ACHMExtractorV2(account=args.account, output_file=args.output_h5)
    
    try:
        # Process
        metadata = extractor.extract_and_save(args.input_html)
        
        # Print summary
        print(f"\nExtraction Summary:")
        if args.account:
            print(f"  Account: {args.account}")
        print(f"  Flow Cell: {metadata.get('flow_cell_id')}_{metadata.get('lane')}")
        print(f"  Cycles: {metadata['num_cycles']}")
        print(f"  Grid: {metadata['num_rows']} × {metadata['num_cols']}")
        print(f"  Metrics: {metadata['num_metrics']}")
        print(f"  Output: {args.output_h5}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
