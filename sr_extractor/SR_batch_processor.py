#!/usr/bin/env python3
"""
Portable version of SR Batch Processor
Modified for PyInstaller compatibility
"""

import os
import sys
import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import shutil
import time
from typing import List, Tuple, Dict, Optional, Any
import logging
import importlib.util

# Suppress INFO and DEBUG logging from all modules
logging.basicConfig(level=logging.WARNING)

def get_base_path():
    """Get base path for frozen or normal execution."""
    if getattr(sys, 'frozen', False):
        # Running as compiled executable
        return Path(sys._MEIPASS)
    else:
        # Running as script
        return Path(__file__).parent

def get_working_dir():
    """Get working directory (where exe is located)."""
    if getattr(sys, 'frozen', False):
        # Get the directory containing the exe
        return Path(sys.executable).parent
    else:
        # Running as script
        return Path.cwd()

class SRBatchProcessor:
    """Batch processor for summaryReport files only."""
    
    def __init__(self):
        # Use working directory for all file operations
        self.portable_dir = get_working_dir()
        self.accounts_dir = self.portable_dir / "accounts"
        
        # For frozen app, scripts are in temp directory
        if getattr(sys, 'frozen', False):
            self.scripts_dir = get_base_path() / "scripts"
        else:
            self.scripts_dir = self.portable_dir / "scripts"
        
        # Get today's date for folders
        self.process_date = datetime.now().strftime("%Y-%m-%d")
        self.process_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Output directory for extracted data
        self.output_dir = self.portable_dir / "_output_files"
        self.sr_output_dir = self.output_dir / f"{self.process_date}_sr"
        
        # Processed files base directory (not date-stamped)
        self.processed_base_dir = self.portable_dir / "processed_files"
        
        # Consolidated output files
        self.summarydata_file = self.sr_output_dir / "summarydata.parquet"
        self.cycle_data_file = self.sr_output_dir / "cycle_data.parquet"
        self.duplicates_summarydata_file = self.sr_output_dir / "~duplicates_summarydata.parquet"
        self.duplicates_cycle_data_file = self.sr_output_dir / "~duplicates_cycle_data.parquet"
        self.report_file = self.sr_output_dir / "processing_report.txt"
        
        # Data accumulators
        self.all_summarydata = []
        self.all_cycle_data = []
        self.duplicate_summarydata = []
        self.duplicate_cycle_data = []
        
        # Track processed flow cells to detect duplicates
        self.processed_flow_cells = set()
        
        # Statistics
        self.stats = {
            'processed': 0,
            'duplicates': 0,
            'failed': 0,
            'files_moved': 0,
            'failed_files_copied': 0
        }
        self.failures = []
        
        # Import the extractor module
        self.extractor_module = None
        
        # Report content accumulator
        self.report_lines = []

    def print_progress_bar(self, current: int, total: int, width: int = 40) -> str:
        """Create a simple ASCII progress bar."""
        if total == 0:
            return ""
        percent = current / total
        filled = int(width * percent)
        bar = '#' * filled + '-' * (width - filled)
        return f"[{bar}] {current}/{total}"

    def load_extractor(self) -> bool:
        """Dynamically load the SR extractor module."""
        try:
            # Try to find the extractor script
            if getattr(sys, 'frozen', False):
                # In frozen app, look in multiple places
                possible_paths = [
                    self.scripts_dir / "SR_data_extractor.py",
                    get_working_dir() / "scripts" / "SR_data_extractor.py",
                    get_base_path() / "SR_data_extractor.py"
                ]
                
                extractor_path = None
                for path in possible_paths:
                    if path.exists():
                        extractor_path = path
                        break
                
                if not extractor_path:
                    # Try to import directly if it was bundled as a module
                    try:
                        import SR_data_extractor
                        self.extractor_module = SR_data_extractor
                        print("Loaded SR extractor as bundled module")
                        return True
                    except ImportError:
                        print(f"ERROR: Could not find SR_data_extractor.py")
                        print(f"Searched in: {[str(p) for p in possible_paths]}")
                        return False
            else:
                extractor_path = self.scripts_dir / "SR_data_extractor.py"
            
            # Load from file
            spec = importlib.util.spec_from_file_location(
                "SR_data_extractor", 
                str(extractor_path)
            )
            self.extractor_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(self.extractor_module)
            print(f"Loaded SR extractor from: {extractor_path}")
            return True
            
        except Exception as e:
            print(f"ERROR: Could not load SR extractor: {e}")
            self.report_lines.append(f"ERROR: Could not load SR extractor: {e}")
            return False

    def validate_directory_structure(self) -> bool:
        """Validate and create required directory structure."""
        missing_items = []
        
        # For frozen exe, don't check scripts directory
        if not getattr(sys, 'frozen', False):
            if not self.scripts_dir.exists():
                missing_items.append("scripts/")
            elif not (self.scripts_dir / "SR_data_extractor.py").exists():
                missing_items.append("scripts/SR_data_extractor.py")
        
        if not self.accounts_dir.exists():
            # Create accounts directory if it doesn't exist
            self.accounts_dir.mkdir(parents=True, exist_ok=True)
            print("Created directory: accounts/")
            print("  Place customer folders with HTML files here")
        
        # Create output directory if it doesn't exist
        if not self.output_dir.exists():
            self.output_dir.mkdir(parents=True, exist_ok=True)
            print("Created directory: _output_files/")
        
        # Create SR output directory for today
        if not self.sr_output_dir.exists():
            self.sr_output_dir.mkdir(parents=True, exist_ok=True)
            print(f"Created directory: _output_files/{self.sr_output_dir.name}/")
        
        # Create processed_files directory if it doesn't exist
        if not self.processed_base_dir.exists():
            self.processed_base_dir.mkdir(parents=True, exist_ok=True)
            print("Created directory: processed_files/")
        
        if missing_items:
            error_msg = f"ERROR: Missing required items: {', '.join(missing_items)}"
            print(error_msg)
            self.report_lines.append(error_msg)
            return False
        
        # Check if consolidated files already exist
        files_exist = []
        if self.summarydata_file.exists():
            files_exist.append("summarydata.parquet")
        if self.cycle_data_file.exists():
            files_exist.append("cycle_data.parquet")
        if self.duplicates_summarydata_file.exists():
            files_exist.append("~duplicates_summarydata.parquet")
        if self.duplicates_cycle_data_file.exists():
            files_exist.append("~duplicates_cycle_data.parquet")
            
        if files_exist:
            print(f"WARNING: Output files already exist in {self.sr_output_dir.name}/")
            print(f"Files: {', '.join(files_exist)}")
            response = input("Do you want to (O)verwrite, (A)ppend, or (C)ancel? [O/A/C]: ").strip().upper()
            if response == 'C':
                self.report_lines.append("Processing cancelled by user")
                return False
            elif response == 'A':
                # Load existing data to append to
                loaded_records = 0
                if self.summarydata_file.exists():
                    existing_summarydata = pd.read_parquet(self.summarydata_file)
                    self.all_summarydata = existing_summarydata.to_dict('records')
                    for record in self.all_summarydata:
                        fc_key = f"{record.get('flow_cell_id')}_{record.get('lane')}"
                        self.processed_flow_cells.add(fc_key)
                    loaded_records += len(self.all_summarydata)
                    
                if self.cycle_data_file.exists():
                    existing_cycle_data = pd.read_parquet(self.cycle_data_file)
                    self.all_cycle_data = existing_cycle_data.to_dict('records')
                    loaded_records += len(self.all_cycle_data)
                
                if self.duplicates_summarydata_file.exists():
                    existing_dup_summarydata = pd.read_parquet(self.duplicates_summarydata_file)
                    self.duplicate_summarydata = existing_dup_summarydata.to_dict('records')
                    loaded_records += len(self.duplicate_summarydata)
                
                if self.duplicates_cycle_data_file.exists():
                    existing_dup_cycle_data = pd.read_parquet(self.duplicates_cycle_data_file)
                    self.duplicate_cycle_data = existing_dup_cycle_data.to_dict('records')
                    loaded_records += len(self.duplicate_cycle_data)
                
                print(f"Loaded {loaded_records} existing records")
                self.report_lines.append(f"Appending to existing files - loaded {loaded_records} records")
            else:
                self.report_lines.append("Overwriting existing files")
        
        return True

    def find_customer_directories(self) -> List[Path]:
        """Find all customer directories in accounts folder."""
        customer_dirs = []
        
        if not self.accounts_dir.exists():
            return customer_dirs
        
        for item in self.accounts_dir.iterdir():
            if item.is_dir():
                customer_dirs.append(item)
        
        customer_dirs.sort()
        return customer_dirs

    def extract_customer_name(self, customer_dir: Path) -> str:
        """Extract customer name from directory name."""
        return customer_dir.name

    def find_sr_files(self, customer_dir: Path) -> List[Path]:
        """Find summaryReport HTML files in customer directory."""
        sr_files = []
        for file in customer_dir.glob("*summaryReport*.html"):
            sr_files.append(file)
        return sorted(sr_files)

    def check_if_processed(self, flow_cell_id: str, lane: str) -> bool:
        """Check if this flow cell/lane has already been processed."""
        fc_key = f"{flow_cell_id}_{lane}"
        return fc_key in self.processed_flow_cells

    def process_sr_file(self, input_file: Path, customer_name: str) -> Tuple[bool, str, bool]:
        """Process a single summaryReport HTML file using the imported extractor."""
        try:
            extractor = self.extractor_module.CombinedDataExtractor(account=customer_name)
            extracted_data = extractor.extract_all_data(str(input_file))
            
            if not extracted_data:
                return False, "No data extracted", False
            
            flow_cell_id = extracted_data.get('flow_cell_id')
            lane = extracted_data.get('lane')
            
            if not flow_cell_id:
                return False, "Missing flow_cell_id", False
            
            is_duplicate = self.check_if_processed(flow_cell_id, lane)
            
            summarydata, cycle_data_list = extractor.split_summarydata_and_cycles(extracted_data)
            
            if is_duplicate:
                self.duplicate_summarydata.append(summarydata)
                self.duplicate_cycle_data.extend(cycle_data_list)
                return True, f"{flow_cell_id}_{lane}", True
            else:
                self.all_summarydata.append(summarydata)
                self.all_cycle_data.extend(cycle_data_list)
                
                fc_key = f"{flow_cell_id}_{lane}"
                self.processed_flow_cells.add(fc_key)
                
                return True, f"{flow_cell_id}_{lane}", False
                
        except Exception as e:
            return False, str(e), False

    def move_to_processed(self, file_path: Path, customer_name: str) -> bool:
        """Move processed file to the processed_files/customer/date/ directory."""
        try:
            customer_processed_dir = self.processed_base_dir / customer_name / self.process_date
            customer_processed_dir.mkdir(parents=True, exist_ok=True)
            
            destination = customer_processed_dir / file_path.name
            shutil.move(str(file_path), str(destination))
            self.stats['files_moved'] += 1
            return True
        except Exception as e:
            self.report_lines.append(f"  Warning: Could not move {file_path.name}: {e}")
            return False

    def copy_failed_file(self, file_path: Path, customer_name: str) -> bool:
        """Copy failed file to the _output_files/[date]_sr/failed_[customer]/ directory."""
        try:
            failed_dir = self.sr_output_dir / f"failed_{customer_name}"
            failed_dir.mkdir(parents=True, exist_ok=True)
            
            destination = failed_dir / file_path.name
            shutil.copy2(str(file_path), str(destination))
            self.stats['failed_files_copied'] += 1
            return True
        except Exception as e:
            self.report_lines.append(f"  Warning: Could not copy failed file {file_path.name}: {e}")
            return False

    def process_customer(self, customer_dir: Path) -> None:
        """Process all SR files for a single customer."""
        customer_name = self.extract_customer_name(customer_dir)
        sr_files = self.find_sr_files(customer_dir)
        
        if not sr_files:
            return
        
        print(f"\nProcessing customer: {customer_name}")
        print(f"  Found {len(sr_files)} files")
        
        files_to_move = []
        failed_files = []
        customer_stats = {'ok': 0, 'dup': 0, 'fail': 0}
        
        # Process with progress bar
        for i, input_file in enumerate(sr_files, 1):
            # Update progress bar
            progress = self.print_progress_bar(i, len(sr_files))
            print(f"  Processing... {progress}", end='\r')
            
            success, message, is_duplicate = self.process_sr_file(input_file, customer_name)
            
            if success:
                if is_duplicate:
                    customer_stats['dup'] += 1
                    self.stats['duplicates'] += 1
                else:
                    customer_stats['ok'] += 1
                    self.stats['processed'] += 1
                files_to_move.append(input_file)
            else:
                customer_stats['fail'] += 1
                self.stats['failed'] += 1
                self.failures.append((customer_name, input_file.name, message))
                failed_files.append(input_file)
        
        # Clear progress bar and show summary
        print(" " * 60, end='\r')  # Clear the line
        print(f"  [OK] {customer_stats['ok']} processed, [DUP] {customer_stats['dup']} duplicates, [FAIL] {customer_stats['fail']} failed")
        
        # Add summary to report
        self.report_lines.append(f"\nCustomer: {customer_name}")
        self.report_lines.append(f"  Files found: {len(sr_files)}")
        self.report_lines.append(f"  Processed: {customer_stats['ok']}")
        self.report_lines.append(f"  Duplicates: {customer_stats['dup']}")
        self.report_lines.append(f"  Failed: {customer_stats['fail']}")
        
        # Handle failed files
        if failed_files:
            for file_path in failed_files:
                self.copy_failed_file(file_path, customer_name)
                self.move_to_processed(file_path, customer_name)
        
        # Move successfully processed files
        for file_path in files_to_move:
            self.move_to_processed(file_path, customer_name)

    def save_consolidated_data(self) -> bool:
        """Save all accumulated data to consolidated Parquet files."""
        print("\nSaving consolidated data...")
        self.report_lines.append("\nData Files Created:")
        
        success = True
        
        try:
            if self.all_summarydata:
                summarydata_df = pd.DataFrame(self.all_summarydata)
                if 'barcodeSplitRate' in summarydata_df.columns:
                    summarydata_df['barcodeSplitRate'] = summarydata_df['barcodeSplitRate'].astype(str)
                summarydata_df.to_parquet(self.summarydata_file, engine='pyarrow', compression='snappy')
                file_size_mb = self.summarydata_file.stat().st_size / (1024 * 1024)
                self.report_lines.append(f"  summarydata.parquet: {len(self.all_summarydata)} records, {file_size_mb:.2f} MB")
            
            if self.all_cycle_data:
                cycle_data_df = pd.DataFrame(self.all_cycle_data)
                cycle_data_df['values'] = cycle_data_df['values'].apply(lambda x: np.array(x, dtype=np.float32))
                cycle_data_df.to_parquet(self.cycle_data_file, engine='pyarrow', compression='snappy')
                file_size_mb = self.cycle_data_file.stat().st_size / (1024 * 1024)
                self.report_lines.append(f"  cycle_data.parquet: {len(self.all_cycle_data)} arrays, {file_size_mb:.2f} MB")
            
            if self.duplicate_summarydata:
                dup_summarydata_df = pd.DataFrame(self.duplicate_summarydata)
                if 'barcodeSplitRate' in dup_summarydata_df.columns:
                    dup_summarydata_df['barcodeSplitRate'] = dup_summarydata_df['barcodeSplitRate'].astype(str)
                dup_summarydata_df.to_parquet(self.duplicates_summarydata_file, engine='pyarrow', compression='snappy')
                file_size_mb = self.duplicates_summarydata_file.stat().st_size / (1024 * 1024)
                self.report_lines.append(f"  ~duplicates_summarydata.parquet: {len(self.duplicate_summarydata)} records, {file_size_mb:.2f} MB")
            
            if self.duplicate_cycle_data:
                dup_cycle_data_df = pd.DataFrame(self.duplicate_cycle_data)
                dup_cycle_data_df['values'] = dup_cycle_data_df['values'].apply(lambda x: np.array(x, dtype=np.float32))
                dup_cycle_data_df.to_parquet(self.duplicates_cycle_data_file, engine='pyarrow', compression='snappy')
                file_size_mb = self.duplicates_cycle_data_file.stat().st_size / (1024 * 1024)
                self.report_lines.append(f"  ~duplicates_cycle_data.parquet: {len(self.duplicate_cycle_data)} arrays, {file_size_mb:.2f} MB")
            
        except Exception as e:
            print(f"ERROR: Failed to save parquet files: {e}")
            self.report_lines.append(f"ERROR: Failed to save parquet files: {e}")
            success = False
        
        return success

    def save_report(self, elapsed_time: float):
        """Save processing report to text file."""
        with open(self.report_file, 'w') as f:
            f.write("SR BATCH PROCESSING REPORT\n")
            f.write("=" * 60 + "\n")
            f.write(f"Date: {self.process_datetime}\n")
            f.write(f"Processing time: {elapsed_time:.1f} seconds\n")
            
            if self.stats['processed'] + self.stats['duplicates'] > 0:
                avg_time = elapsed_time / (self.stats['processed'] + self.stats['duplicates'])
                f.write(f"Average time per file: {avg_time:.1f} seconds\n")
            
            f.write("\n")
            f.write("Summary Statistics:\n")
            f.write("-" * 30 + "\n")
            f.write(f"Successfully processed: {self.stats['processed']}\n")
            f.write(f"Duplicates found: {self.stats['duplicates']}\n")
            f.write(f"Failed: {self.stats['failed']}\n")
            total_handled = self.stats['processed'] + self.stats['duplicates'] + self.stats['failed']
            f.write(f"Total files handled: {total_handled}\n")
            f.write(f"Files moved to processed: {self.stats['files_moved']}\n")
            f.write(f"Failed files copied: {self.stats['failed_files_copied']}\n")
            
            f.write("\n")
            f.write("Processing Details:\n")
            f.write("-" * 30 + "\n")
            for line in self.report_lines:
                f.write(line + "\n")
            
            if self.failures:
                f.write("\n")
                f.write("Failed Files Detail:\n")
                f.write("-" * 30 + "\n")
                for customer, filename, error in self.failures:
                    f.write(f"{customer}/{filename}\n")
                    f.write(f"  Error: {error}\n")

    def run(self) -> int:
        """Main execution method."""
        print("SR Batch Processor (Portable Edition)")
        print("=" * 60)
        print(f"Processing date: {self.process_date}")
        print(f"Working directory: {self.portable_dir}")
        
        if getattr(sys, 'frozen', False):
            print("Running as compiled executable")
        
        self.report_lines.append(f"Started at: {self.process_datetime}")
        
        # Load the extractor module
        if not self.load_extractor():
            print("\nPress Enter to exit...")
            input()
            return 1
        
        # Validate directory structure
        if not self.validate_directory_structure():
            print("\nPress Enter to exit...")
            input()
            return 1
        
        # Find customer directories
        customer_dirs = self.find_customer_directories()
        
        if not customer_dirs:
            print("\nNo customer directories found in accounts/")
            print("Please create subdirectories in accounts/ with HTML files to process")
            print("Example: accounts/Customer1/")
            self.report_lines.append("ERROR: No customer directories found")
            print("\nPress Enter to exit...")
            input()
            return 1
        
        print(f"\nFound {len(customer_dirs)} customer directories")
        
        # Process each customer
        start_time = time.time()
        
        for customer_dir in customer_dirs:
            self.process_customer(customer_dir)
        
        # Save consolidated data
        save_success = self.save_consolidated_data()
        
        if not save_success:
            print("\nERROR: Failed to save data")
            self.stats['files_moved'] = 0
        
        elapsed_time = time.time() - start_time
        
        # Save report
        self.save_report(elapsed_time)
        
        # Print final summary
        print("\n" + "=" * 60)
        print("Processing Complete")
        print(f"  Processed: {self.stats['processed']}")
        print(f"  Duplicates: {self.stats['duplicates']}")
        print(f"  Failed: {self.stats['failed']}")
        print(f"  Time: {elapsed_time:.1f} seconds")
        print(f"\nSee processing_report.txt for full details")
        print(f"Output location: _output_files/{self.sr_output_dir.name}/")
        
        print("\nPress Enter to exit...")
        input()
        
        return 1 if self.stats['failed'] > 0 else 0


def main():
    """Main entry point."""
    try:
        processor = SRBatchProcessor()
        return processor.run()
    except Exception as e:
        print(f"\nERROR: {e}")
        print("\nPress Enter to exit...")
        input()
        return 1


if __name__ == "__main__":
    sys.exit(main())
