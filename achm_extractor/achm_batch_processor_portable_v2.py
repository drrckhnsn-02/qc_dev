#!/usr/bin/env python3
"""
ACHM Batch Processor - Portable Version 2 (Lean)
Processes allCycleHeatmap.html files from accounts/ directories
Creates HDF5 chunks with size-aware chunking

Version 2 improvements:
- Parallel processing with selectable core usage
- Immediate file movement after successful processing
- Better duplicate/incomplete file handling
- Configuration section for customization
- LEAN: Assumes directory structure exists
"""

import os
import sys
import json
import time
import shutil
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import threading
import importlib.util
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

# Suppress unnecessary logging
import logging
logging.basicConfig(level=logging.WARNING)

# ============================================================
#                     CONFIGURATION SECTION
# ============================================================
# Modify these values to customize directory structure

CONFIG = {
    # Directory names (relative to root)
    'ACCOUNTS_DIR': 'accounts',
    'OUTPUT_BASE_DIR': '_output_files',
    'PROCESSED_BASE_DIR': 'processed_files',
    
    # Subdirectory patterns
    'ACHM_OUTPUT_PATTERN': '{date}_achm',  # {date} will be replaced with YYYY-MM-DD
    'INCOMPLETE_SUBDIR': 'incomplete',
    'DUPLICATES_SUBDIR': 'duplicates',
    
    # Processing settings
    'TARGET_CHUNK_GB': 5.0,
    'COMPRESSION_RATIO': 0.23,
    'HDF5_COMPRESSION_LEVEL': 6,  # 1-9, higher = better compression but slower
    
    # File patterns
    'ACHM_FILE_PATTERN': '*allCycleHeatmap*.html',
    'CHUNK_NAME_PATTERN': 'achm_chunk_{num:03d}.h5',
}

# ============================================================


class ACHMBatchProcessor:
    """Batch processor for ACHM files with parallel processing and immediate file handling."""
    
    def __init__(self, compression_ratio: float = None, target_chunk_gb: float = None, 
                 processing_mode: str = 'performance'):
        """Initialize processor with portable directory structure.
        
        Args:
            compression_ratio: HTML to HDF5 compression ratio
            target_chunk_gb: Target chunk size in GB
            processing_mode: 'quiet', 'balanced', or 'performance'
        """
        self.compression_ratio = compression_ratio or CONFIG['COMPRESSION_RATIO']
        self.target_chunk_size = (target_chunk_gb or CONFIG['TARGET_CHUNK_GB']) * 1024**3
        self.processing_mode = processing_mode
        
        # Determine number of workers based on mode
        total_cores = multiprocessing.cpu_count()
        if processing_mode == 'quiet':
            self.num_workers = min(2, total_cores)
        elif processing_mode == 'performance':
            self.num_workers = max(1, total_cores - 2)
        else:  # balanced
            self.num_workers = max(1, total_cores // 2)
        
        # Determine root directory
        if getattr(sys, 'frozen', False):
            self.root_dir = Path(sys.executable).parent
        else:
            current = Path(__file__).parent
            while current != current.parent:
                if (current / "ACHM_batch_processor.bat").exists():
                    self.root_dir = current
                    break
                current = current.parent
            else:
                self.root_dir = Path(__file__).parent.parent.parent
        
        # Set up directories from config
        self.accounts_dir = self.root_dir / CONFIG['ACCOUNTS_DIR']
        self.output_base_dir = self.root_dir / CONFIG['OUTPUT_BASE_DIR']
        self.processed_base_dir = self.root_dir / CONFIG['PROCESSED_BASE_DIR']
        
        # Date-specific output directory
        self.process_date = datetime.now().strftime("%Y-%m-%d")
        achm_dir_name = CONFIG['ACHM_OUTPUT_PATTERN'].format(date=self.process_date)
        self.achm_output_dir = self.output_base_dir / achm_dir_name
        
        # Scripts directory
        self.scripts_dir = self.root_dir / "pkgs" / "scripts"
        
        # Import the extractor module
        self.extractor_module = None
        
        # Statistics
        self.stats = {
            'processed': 0,
            'skipped': 0,
            'failed': 0,
            'incomplete': 0,
            'duplicates': 0,
            'chunks_created': 0,
            'total_size_gb': 0,
            'total_time_seconds': 0,
            'files_moved': 0
        }
        self.failures = []
        self.incomplete_files = []
        self.duplicate_files = []
        self.processed_flow_cells = set()
        
        # Thread safety
        self.lock = threading.Lock()
        
        # Report lines for summary
        self.report_lines = []
        
        # Current chunk file handle (for parallel writing)
        self.current_chunk_file = None
        self.current_chunk_num = 0
        self.current_chunk_size = 0
    
    def print_progress_bar(self, current: int, total: int, width: int = 40) -> str:
        """Create a simple ASCII progress bar."""
        if total == 0:
            return ""
        percent = current / total
        filled = int(width * percent)
        bar = '#' * filled + '-' * (width - filled)
        return f"[{bar}] {current}/{total}"
    
    def load_extractor(self) -> bool:
        """Dynamically load the ACHM extractor module."""
        try:
            # If running as frozen exe, try to import as bundled module first
            if getattr(sys, 'frozen', False):
                try:
                    # Try direct import of bundled module
                    import achm_extractor_v2
                    self.extractor_module = achm_extractor_v2
                    print("Loaded ACHM extractor as bundled module")
                    return True
                except ImportError:
                    pass
                
                # Try alternate name
                try:
                    import achm_extractor_portable
                    self.extractor_module = achm_extractor_portable
                    print("Loaded ACHM extractor as bundled module")
                    return True
                except ImportError:
                    pass
                
                # If bundled import failed, try file paths (shouldn't be needed)
                possible_paths = [
                    Path(sys._MEIPASS) / "scripts" / "achm_extractor_v2.py",
                    Path(sys._MEIPASS) / "achm_extractor_v2.py"
                ]
                
                for extractor_path in possible_paths:
                    if extractor_path.exists():
                        spec = importlib.util.spec_from_file_location(
                            "achm_extractor", 
                            str(extractor_path)
                        )
                        self.extractor_module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(self.extractor_module)
                        print(f"Loaded ACHM extractor from: {extractor_path.name}")
                        return True
                
                print("ERROR: Could not find ACHM extractor in bundled executable")
                self.report_lines.append("ERROR: Could not find ACHM extractor")
                return False
            
            else:
                # Running as script - look for file in scripts directory
                extractor_path = self.scripts_dir / "achm_extractor_v2.py"
                if not extractor_path.exists():
                    extractor_path = self.scripts_dir / "achm_extractor_portable.py"
                
                if not extractor_path.exists():
                    print(f"ERROR: No ACHM extractor found in {self.scripts_dir}")
                    self.report_lines.append(f"ERROR: No ACHM extractor found in scripts directory")
                    return False
                
                spec = importlib.util.spec_from_file_location(
                    "achm_extractor", 
                    str(extractor_path)
                )
                self.extractor_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(self.extractor_module)
                
                print(f"Loaded extractor: {extractor_path.name}")
                return True
            
        except Exception as e:
            print(f"ERROR: Could not load ACHM extractor: {e}")
            self.report_lines.append(f"ERROR: Could not load ACHM extractor: {e}")
            return False
    
    def validate_existing_output(self) -> bool:
        """Check for existing output files and handle appropriately."""
        # Ensure output directory exists for chunks
        if not self.achm_output_dir.exists():
            self.achm_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Check for existing chunks
        existing_chunks = list(self.achm_output_dir.glob("achm_chunk_*.h5"))
        if existing_chunks:
            print(f"\nWARNING: Found {len(existing_chunks)} existing chunk files")
            response = input("Do you want to (O)verwrite, (A)ppend, or (C)ancel? [O/A/C]: ").strip().upper()
            
            if response == 'C':
                self.report_lines.append("Processing cancelled by user")
                return False
            elif response == 'O':
                print("Overwriting existing chunks...")
                for chunk in existing_chunks:
                    chunk.unlink()
                self.report_lines.append("Overwrote existing chunks")
            else:  # Default to append
                print("Appending new chunks...")
                self._load_existing_flow_cells(existing_chunks)
                self.report_lines.append(f"Appending to existing chunks ({len(self.processed_flow_cells)} flow cells already processed)")
        
        return True
    
    def _load_existing_flow_cells(self, chunk_files: List[Path]):
        """Load list of already processed flow cells from existing chunks."""
        try:
            import h5py
            for chunk_file in chunk_files:
                try:
                    with h5py.File(chunk_file, 'r') as f:
                        if 'flow_cells' in f:
                            for fc_key in f['flow_cells'].keys():
                                self.processed_flow_cells.add(fc_key)
                except Exception as e:
                    print(f"  Warning: Could not read {chunk_file.name}: {e}")
            
            if self.processed_flow_cells:
                print(f"  Loaded {len(self.processed_flow_cells)} existing flow cells")
                
        except ImportError:
            print("ERROR: h5py not installed")
            return
    
    def find_achm_files(self) -> Tuple[List[Path], Dict[Path, str]]:
        """Find all ACHM files and map them to customer names."""
        all_files = []
        customer_map = {}
        
        print("\nScanning for ACHM files...")
        
        for customer_dir in sorted(self.accounts_dir.iterdir()):
            if customer_dir.is_dir():
                customer_name = customer_dir.name
                achm_files = list(customer_dir.glob(CONFIG['ACHM_FILE_PATTERN']))
                
                if achm_files:
                    print(f"  {customer_name}: {len(achm_files)} ACHM files")
                    for f in achm_files:
                        all_files.append(f)
                        customer_map[f] = customer_name
        
        return all_files, customer_map
    
    def get_flow_cell_key(self, html_file: Path) -> str:
        """Extract flow cell key from filename."""
        import re
        filename = html_file.name
        match = re.match(r'([A-Z]+\d+)_L(\d+)', filename, re.IGNORECASE)
        if match:
            return f"{match.group(1)}_L{match.group(2).zfill(2)}"
        return filename.split('.')[0]
    
    def move_file_to_directory(self, file_path: Path, customer_name: str, 
                              subdirectory: Optional[str] = None) -> bool:
        """Move file to processed directory with optional subdirectory.
        
        Args:
            file_path: Path to file to move
            customer_name: Customer name for directory structure
            subdirectory: Optional subdirectory (e.g., 'incomplete', 'duplicates')
        """
        try:
            dest_dir = self.processed_base_dir / customer_name / self.process_date
            if subdirectory:
                dest_dir = dest_dir / subdirectory
            dest_dir.mkdir(parents=True, exist_ok=True)
            
            destination = dest_dir / file_path.name
            shutil.move(str(file_path), str(destination))
            
            with self.lock:
                self.stats['files_moved'] += 1
            
            return True
        except Exception as e:
            self.report_lines.append(f"  Warning: Could not move {file_path.name}: {e}")
            return False
    
    def process_single_file_worker(self, task: Dict) -> Dict:
        """Worker function for parallel processing - extracts data only."""
        html_file = task['html_file']
        customer_name = task['customer_name']
        
        try:
            # Create extractor instance - but don't save to HDF5 yet
            if hasattr(self.extractor_module, 'ACHMExtractorV2'):
                extractor = self.extractor_module.ACHMExtractorV2(
                    account=customer_name,
                    output_file=None  # Don't specify output file yet
                )
            else:
                extractor = self.extractor_module.ACHMExtractor(
                    account=customer_name,
                    output_file=None
                )
            
            # Extract flow cell info first
            flow_cell_id, lane = extractor.extract_flow_cell_and_lane(str(html_file))
            fc_key = f"{flow_cell_id}_{lane}"
            
            # Check for duplicate
            with self.lock:
                if fc_key in self.processed_flow_cells:
                    return {
                        'success': True,
                        'html_file': html_file,
                        'customer_name': customer_name,
                        'fc_key': fc_key,
                        'duplicate': True
                    }
            
            # Read and parse HTML
            start_time = time.time()
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            # Extract script content
            script_content = extractor.extract_script_content(html_content)
            if not script_content:
                raise ValueError("Failed to extract data from HTML")
            
            # Parse data
            options, is_full = extractor.parse_option_array(script_content)
            if not options:
                raise ValueError("Failed to parse metric names")
            
            fov_data = extractor.parse_fov_data(script_content)
            if not fov_data:
                raise ValueError("Failed to parse fov data")
            
            # Convert to arrays
            data_array, metadata = extractor.convert_to_arrays(fov_data, options)
            
            # Add identifiers
            metadata['flow_cell_id'] = flow_cell_id
            metadata['lane'] = lane
            metadata['account'] = customer_name
            process_time = time.time() - start_time
            
            # Check if data is complete
            if not metadata.get('flow_cell_id') or metadata.get('num_cycles', 0) == 0:
                return {
                    'success': False,
                    'html_file': html_file,
                    'customer_name': customer_name,
                    'error': 'Incomplete data',
                    'incomplete': True
                }
            
            return {
                'success': True,
                'html_file': html_file,
                'customer_name': customer_name,
                'fc_key': fc_key,
                'data_array': data_array,
                'options': options,
                'metadata': metadata,
                'process_time': process_time,
                'duplicate': False
            }
            
        except Exception as e:
            return {
                'success': False,
                'html_file': html_file,
                'customer_name': customer_name,
                'error': str(e)[:100]
            }
    
    def process_chunk_parallel(self, chunk_files: List[Path], chunk_num: int,
                             customer_map: Dict[Path, str]) -> Tuple[bool, Dict]:
        """Process a chunk of files using parallel processing."""
        chunk_output = self.achm_output_dir / CONFIG['CHUNK_NAME_PATTERN'].format(num=chunk_num)
        chunk_start = time.time()
        
        print(f"\nProcessing Chunk {chunk_num:03d} (Performance mode: {self.num_workers} workers)")
        print(f"  Files: {len(chunk_files)}")
        print(f"  Output: {chunk_output.name}")
        
        chunk_stats = {
            'chunk_num': chunk_num,
            'file': chunk_output.name,
            'processed': 0,
            'failed': 0,
            'incomplete': 0,
            'duplicates': 0,
            'size_mb': 0,
            'time_seconds': 0
        }
        
        # Prepare tasks
        tasks = []
        for html_file in chunk_files:
            customer_name = customer_map.get(html_file, "unknown")
            tasks.append({
                'html_file': html_file,
                'customer_name': customer_name
            })
        
        # Process files in parallel to extract data
        results = []
        completed = 0
        
        print(f"  Extracting data from {len(chunk_files)} files in parallel...")
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = {executor.submit(self.process_single_file_worker, task): task 
                      for task in tasks}
            
            for future in as_completed(futures):
                completed += 1
                progress = self.print_progress_bar(completed, len(chunk_files))
                print(f"  Extracting... {progress}", end='\r')
                
                result = future.result()
                results.append(result)
        
        # Clear progress line
        print(" " * 70, end='\r')
        print(f"  Extraction complete. Writing to HDF5...")
        
        # Now write results to HDF5 sequentially (to avoid HDF5 threading issues)
        import h5py
        for i, result in enumerate(results, 1):
            if result['success']:
                if result.get('duplicate'):
                    chunk_stats['duplicates'] += 1
                    with self.lock:
                        self.stats['duplicates'] += 1
                        self.duplicate_files.append((result['customer_name'], result['html_file'].name))
                    # Move to duplicates folder immediately
                    self.move_file_to_directory(result['html_file'], result['customer_name'], 
                                               CONFIG['DUPLICATES_SUBDIR'])
                elif 'data_array' in result:
                    # Write to HDF5
                    try:
                        # Create extractor just for saving
                        if hasattr(self.extractor_module, 'ACHMExtractorV2'):
                            extractor = self.extractor_module.ACHMExtractorV2(
                                account=result['customer_name'],
                                output_file=str(chunk_output)
                            )
                        else:
                            extractor = self.extractor_module.ACHMExtractor(
                                account=result['customer_name'],
                                output_file=str(chunk_output)
                            )
                        
                        # Save to HDF5
                        size_added = extractor.save_to_hdf5(
                            result['data_array'],
                            result['options'],
                            result['metadata'],
                            result['metadata']['flow_cell_id'],
                            result['metadata']['lane']
                        )
                        
                        chunk_stats['processed'] += 1
                        chunk_stats['size_mb'] += size_added / (1024**2)
                        
                        with self.lock:
                            self.stats['processed'] += 1
                            self.stats['total_time_seconds'] += result.get('process_time', 0)
                            self.processed_flow_cells.add(result['fc_key'])
                        
                        # Move to processed folder immediately after successful save
                        self.move_file_to_directory(result['html_file'], result['customer_name'])
                        
                        # Update progress
                        print(f"  Writing HDF5... [{i}/{len(results)}]", end='\r')
                        
                    except Exception as e:
                        chunk_stats['failed'] += 1
                        with self.lock:
                            self.stats['failed'] += 1
                            self.failures.append((result['customer_name'], result['html_file'].name, str(e)[:100]))
                else:
                    # Shouldn't happen but handle it
                    chunk_stats['failed'] += 1
                    with self.lock:
                        self.stats['failed'] += 1
                        self.failures.append((result['customer_name'], result['html_file'].name, 'No data extracted'))
            else:
                if result.get('incomplete'):
                    chunk_stats['incomplete'] += 1
                    with self.lock:
                        self.stats['incomplete'] += 1
                        self.incomplete_files.append((result['customer_name'], result['html_file'].name))
                    # Move to incomplete folder
                    self.move_file_to_directory(result['html_file'], result['customer_name'], 
                                               CONFIG['INCOMPLETE_SUBDIR'])
                else:
                    chunk_stats['failed'] += 1
                    with self.lock:
                        self.stats['failed'] += 1
                        self.failures.append((result['customer_name'], result['html_file'].name, 
                                            result.get('error', 'Unknown error')))
        
        # Clear progress line
        print(" " * 70, end='\r')
        
        chunk_stats['time_seconds'] = time.time() - chunk_start
        
        if chunk_output.exists():
            actual_size_mb = chunk_output.stat().st_size / (1024**2)
            chunk_stats['size_mb'] = actual_size_mb
            
            with self.lock:
                self.stats['chunks_created'] += 1
                self.stats['total_size_gb'] += actual_size_mb / 1024
            
            print(f"  Chunk complete: {chunk_stats['processed']} processed, "
                  f"{chunk_stats['duplicates']} duplicates, "
                  f"{chunk_stats['incomplete']} incomplete, "
                  f"{chunk_stats['failed']} failed, {actual_size_mb:.1f} MB")
            print(f"  Processing time: {chunk_stats['time_seconds']:.1f} seconds")
            
            return True, chunk_stats
        
        return False, chunk_stats
    
    def estimate_h5_size(self, html_file: Path) -> int:
        """Estimate HDF5 size based on HTML size and compression ratio."""
        html_size = html_file.stat().st_size
        return int(html_size * self.compression_ratio)
    
    def plan_chunks(self, achm_files: List[Path]) -> List[List[Path]]:
        """Plan distribution of files across chunks."""
        chunks = []
        current_chunk = []
        current_size = 0
        
        # Filter out already processed files
        files_to_process = []
        skipped_count = 0
        
        for f in achm_files:
            fc_key = self.get_flow_cell_key(f)
            if fc_key not in self.processed_flow_cells:
                files_to_process.append(f)
            else:
                skipped_count += 1
                self.stats['skipped'] += 1
        
        if skipped_count > 0:
            print(f"  Skipping {skipped_count} already processed files")
        
        if not files_to_process:
            print("All files already processed!")
            return []
        
        # Sort by size for better packing
        sorted_files = sorted(files_to_process, key=lambda f: f.stat().st_size, reverse=True)
        
        for file_path in sorted_files:
            estimated_size = self.estimate_h5_size(file_path)
            
            # Check if adding this file exceeds limit
            if current_size + estimated_size > self.target_chunk_size and current_chunk:
                chunks.append(current_chunk)
                current_chunk = []
                current_size = 0
            
            current_chunk.append(file_path)
            current_size += estimated_size
        
        # Add final chunk
        if current_chunk:
            chunks.append(current_chunk)
        
        return chunks
    
    def save_report(self, chunk_infos: List[Dict], elapsed_time: float):
        """Save processing report to text file."""
        report_file = self.achm_output_dir / "processing_report.txt"
        
        with open(report_file, 'w') as f:
            f.write("ACHM BATCH PROCESSING REPORT\n")
            f.write("=" * 60 + "\n")
            f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Processing mode: {self.processing_mode} ({self.num_workers} workers)\n")
            f.write(f"Processing time: {elapsed_time:.1f} seconds\n")
            
            if self.stats['processed'] > 0:
                avg_time = self.stats['total_time_seconds'] / self.stats['processed']
                f.write(f"Average time per file: {avg_time:.1f} seconds\n")
            
            f.write("\n")
            f