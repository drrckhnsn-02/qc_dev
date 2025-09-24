#!/usr/bin/env python3
"""
ACHM Batch Processor - Portable Version
Processes allCycleHeatmap.html files from accounts/ directories
Creates HDF5 chunks with size-aware chunking
Simplified console output for maximum compatibility
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

# Suppress unnecessary logging
import logging
logging.basicConfig(level=logging.WARNING)


class ACHMBatchProcessor:
    """Batch processor for ACHM files with size-aware chunking."""
    
    def __init__(self, compression_ratio: float = 0.23, target_chunk_gb: float = 5.0):
        """Initialize processor with portable directory structure."""
        self.compression_ratio = compression_ratio
        self.target_chunk_size = target_chunk_gb * 1024**3  # Convert to bytes
        
        # Determine root directory (where ACHM_batch_processor.bat lives)
        if getattr(sys, 'frozen', False):
            # Running as compiled (shouldn't happen for ACHM)
            self.root_dir = Path(sys.executable).parent
        else:
            # Running as script from pkgs/scripts/
            # Go up to find root with ACHM_batch_processor.bat
            current = Path(__file__).parent
            while current != current.parent:
                if (current / "ACHM_batch_processor.bat").exists():
                    self.root_dir = current
                    break
                current = current.parent
            else:
                # Fallback to relative path
                self.root_dir = Path(__file__).parent.parent.parent
        
        # Shared directories matching SR structure
        self.accounts_dir = self.root_dir / "accounts"
        self.output_base_dir = self.root_dir / "_output_files"
        self.processed_base_dir = self.root_dir / "processed_files"
        
        # Date-specific output directory
        self.process_date = datetime.now().strftime("%Y-%m-%d")
        self.achm_output_dir = self.output_base_dir / f"{self.process_date}_achm"
        
        # Scripts directory
        self.scripts_dir = self.root_dir / "pkgs" / "scripts"
        
        # Import the extractor module
        self.extractor_module = None
        
        # Statistics
        self.stats = {
            'processed': 0,
            'skipped': 0,
            'failed': 0,
            'chunks_created': 0,
            'total_size_gb': 0,
            'total_time_seconds': 0
        }
        self.failures = []
        self.processed_flow_cells = set()
        
        # Thread safety
        self.lock = threading.Lock()
        
        # Report lines for summary
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
        """Dynamically load the ACHM extractor module."""
        try:
            # Look for extractor in scripts directory
            extractor_path = self.scripts_dir / "achm_extractor_v2.py"
            if not extractor_path.exists():
                # Try alternate name
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
    
    def validate_directory_structure(self) -> bool:
        """Validate and create required directories."""
        # Check/create accounts directory
        if not self.accounts_dir.exists():
            self.accounts_dir.mkdir(parents=True, exist_ok=True)
            print("Created directory: accounts/")
            print("  Place customer folders with ACHM HTML files here")
        
        # Create output directories
        if not self.output_base_dir.exists():
            self.output_base_dir.mkdir(parents=True, exist_ok=True)
            print("Created directory: _output_files/")
        
        if not self.achm_output_dir.exists():
            self.achm_output_dir.mkdir(parents=True, exist_ok=True)
            print(f"Created directory: _output_files/{self.achm_output_dir.name}/")
        
        # Create processed_files directory
        if not self.processed_base_dir.exists():
            self.processed_base_dir.mkdir(parents=True, exist_ok=True)
            print("Created directory: processed_files/")
        
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
                # Look for allCycleHeatmap files
                achm_files = list(customer_dir.glob("*allCycleHeatmap*.html"))
                
                if achm_files:
                    print(f"  {customer_name}: {len(achm_files)} ACHM files")
                    for f in achm_files:
                        all_files.append(f)
                        customer_map[f] = customer_name
        
        return all_files, customer_map
    
    def estimate_h5_size(self, html_file: Path) -> int:
        """Estimate HDF5 size based on HTML size and compression ratio."""
        html_size = html_file.stat().st_size
        return int(html_size * self.compression_ratio)
    
    def get_flow_cell_key(self, html_file: Path) -> str:
        """Extract flow cell key from filename."""
        import re
        filename = html_file.name
        match = re.match(r'([A-Z]+\d+)_L(\d+)', filename, re.IGNORECASE)
        if match:
            return f"{match.group(1)}_L{match.group(2).zfill(2)}"
        return filename.split('.')[0]
    
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
    
    def get_chunk_filename(self, chunk_num: int) -> Path:
        """Get filename for a specific chunk."""
        return self.achm_output_dir / f"achm_chunk_{chunk_num:03d}.h5"
    
    def process_single_file(self, html_file: Path, output_file: Path, customer_name: str) -> Tuple[bool, str, float]:
        """Process a single ACHM file."""
        try:
            # Create extractor instance
            if hasattr(self.extractor_module, 'ACHMExtractorV2'):
                extractor = self.extractor_module.ACHMExtractorV2(
                    account=customer_name,
                    output_file=str(output_file)
                )
            else:
                extractor = self.extractor_module.ACHMExtractor(
                    account=customer_name,
                    output_file=str(output_file)
                )
            
            # Process file
            start_time = time.time()
            metadata = extractor.extract_and_save(str(html_file))
            process_time = time.time() - start_time
            
            # Get size added
            size_mb = metadata.get('size_added', 0) / (1024**2)
            
            fc_key = f"{metadata.get('flow_cell_id')}_{metadata.get('lane')}"
            
            with self.lock:
                self.stats['processed'] += 1
                self.stats['total_time_seconds'] += process_time
                self.processed_flow_cells.add(fc_key)
            
            return True, fc_key, size_mb
            
        except Exception as e:
            error_msg = str(e)[:100]  # Truncate long errors
            return False, error_msg, 0
    
    def process_chunk(self, chunk_files: List[Path], chunk_num: int,
                     customer_map: Dict[Path, str]) -> Tuple[bool, Dict]:
        """Process a chunk of files."""
        chunk_output = self.get_chunk_filename(chunk_num)
        chunk_start = time.time()
        
        print(f"\nProcessing Chunk {chunk_num:03d}")
        print(f"  Files: {len(chunk_files)}")
        print(f"  Output: {chunk_output.name}")
        
        chunk_stats = {
            'chunk_num': chunk_num,
            'file': chunk_output.name,
            'processed': 0,
            'failed': 0,
            'size_mb': 0,
            'time_seconds': 0
        }
        
        for i, html_file in enumerate(chunk_files, 1):
            customer_name = customer_map.get(html_file, "unknown")
            
            # Update progress bar
            progress = self.print_progress_bar(i, len(chunk_files))
            print(f"  Processing... {progress}", end='\r')
            
            success, message, size_mb = self.process_single_file(
                html_file, chunk_output, customer_name
            )
            
            if success:
                chunk_stats['processed'] += 1
                chunk_stats['size_mb'] += size_mb
            else:
                chunk_stats['failed'] += 1
                with self.lock:
                    self.stats['failed'] += 1
                    self.failures.append((customer_name, html_file.name, message))
        
        # Clear progress line
        print(" " * 60, end='\r')
        
        chunk_stats['time_seconds'] = time.time() - chunk_start
        
        if chunk_output.exists():
            actual_size_mb = chunk_output.stat().st_size / (1024**2)
            chunk_stats['size_mb'] = actual_size_mb
            
            with self.lock:
                self.stats['chunks_created'] += 1
                self.stats['total_size_gb'] += actual_size_mb / 1024
            
            print(f"  Chunk complete: {chunk_stats['processed']} processed, "
                  f"{chunk_stats['failed']} failed, {actual_size_mb:.1f} MB")
            
            return True, chunk_stats
        
        return False, chunk_stats
    
    def save_report(self, chunk_infos: List[Dict], elapsed_time: float):
        """Save processing report to text file."""
        report_file = self.achm_output_dir / "processing_report.txt"
        
        with open(report_file, 'w') as f:
            f.write("ACHM BATCH PROCESSING REPORT\n")
            f.write("=" * 60 + "\n")
            f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Processing time: {elapsed_time:.1f} seconds\n")
            
            if self.stats['processed'] > 0:
                avg_time = self.stats['total_time_seconds'] / self.stats['processed']
                f.write(f"Average time per file: {avg_time:.1f} seconds\n")
            
            f.write("\n")
            f.write("Summary Statistics:\n")
            f.write("-" * 30 + "\n")
            f.write(f"Files processed: {self.stats['processed']}\n")
            f.write(f"Files skipped: {self.stats['skipped']}\n")
            f.write(f"Files failed: {self.stats['failed']}\n")
            f.write(f"Chunks created: {self.stats['chunks_created']}\n")
            f.write(f"Total size: {self.stats['total_size_gb']:.2f} GB\n")
            
            if chunk_infos:
                f.write("\n")
                f.write("Chunk Details:\n")
                f.write("-" * 30 + "\n")
                for info in chunk_infos:
                    f.write(f"{info['file']}: {info['processed']} files, "
                           f"{info['size_mb']:.1f} MB, {info['time_seconds']:.1f}s\n")
            
            if self.report_lines:
                f.write("\n")
                f.write("Processing Notes:\n")
                f.write("-" * 30 + "\n")
                for line in self.report_lines:
                    f.write(f"{line}\n")
            
            if self.failures:
                f.write("\n")
                f.write("Failed Files:\n")
                f.write("-" * 30 + "\n")
                for customer, filename, error in self.failures:
                    f.write(f"{customer}/{filename}\n")
                    f.write(f"  Error: {error}\n")
    
    def move_to_processed(self, file_path: Path, customer_name: str) -> bool:
        """Move processed file to processed_files directory."""
        try:
            customer_processed_dir = self.processed_base_dir / customer_name / self.process_date
            customer_processed_dir.mkdir(parents=True, exist_ok=True)
            
            destination = customer_processed_dir / file_path.name
            shutil.move(str(file_path), str(destination))
            return True
        except Exception:
            return False
    
    def run(self, move_files: bool = False) -> int:
        """Main execution method.
        
        Args:
            move_files: If True, move processed files to processed_files/
        """
        print("ACHM Batch Processor (Portable Edition)")
        print("=" * 60)
        print(f"Processing date: {self.process_date}")
        print(f"Working directory: {self.root_dir}")
        print(f"Compression ratio: {self.compression_ratio:.1%}")
        print(f"Target chunk size: {self.target_chunk_size / (1024**3):.1f} GB")
        
        self.report_lines.append(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Load extractor
        if not self.load_extractor():
            print("\nPress Enter to exit...")
            input()
            return 1
        
        # Validate directories
        if not self.validate_directory_structure():
            print("\nPress Enter to exit...")
            input()
            return 1
        
        # Find ACHM files
        all_files, customer_map = self.find_achm_files()
        
        if not all_files:
            print("\nNo ACHM files found in accounts/")
            print("Please place .allCycleHeatmap.html files in accounts/[customer]/ folders")
            self.report_lines.append("ERROR: No ACHM files found")
            print("\nPress Enter to exit...")
            input()
            return 1
        
        print(f"\nFound {len(all_files)} total ACHM files")
        
        # Plan chunks
        chunks = self.plan_chunks(all_files)
        
        if not chunks:
            print("No files to process (all already processed)")
            print("\nPress Enter to exit...")
            input()
            return 0
        
        print(f"\nChunk Plan:")
        print(f"  Files to process: {sum(len(c) for c in chunks)}")
        print(f"  Number of chunks: {len(chunks)}")
        
        # Get starting chunk number
        existing_chunks = list(self.achm_output_dir.glob("achm_chunk_*.h5"))
        start_chunk_num = len(existing_chunks)
        
        # Process chunks
        start_time = time.time()
        chunk_infos = []
        
        for chunk_idx, chunk_files in enumerate(chunks):
            chunk_num = start_chunk_num + chunk_idx
            success, info = self.process_chunk(chunk_files, chunk_num, customer_map)
            chunk_infos.append(info)
            
            # Optionally move files after successful processing
            if move_files and success:
                for html_file in chunk_files:
                    customer_name = customer_map.get(html_file, "unknown")
                    self.move_to_processed(html_file, customer_name)
        
        elapsed_time = time.time() - start_time
        
        # Save report
        self.save_report(chunk_infos, elapsed_time)
        
        # Print summary
        print("\n" + "=" * 60)
        print("Processing Complete")
        print(f"  Processed: {self.stats['processed']}")
        print(f"  Skipped: {self.stats['skipped']}")
        print(f"  Failed: {self.stats['failed']}")
        print(f"  Chunks: {self.stats['chunks_created']}")
        print(f"  Total size: {self.stats['total_size_gb']:.2f} GB")
        print(f"  Time: {elapsed_time:.1f} seconds")
        print(f"\nSee processing_report.txt for full details")
        print(f"Output location: _output_files/{self.achm_output_dir.name}/")
        
        print("\nPress Enter to exit...")
        input()
        
        return 0 if self.stats['failed'] == 0 else 1


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='ACHM Batch Processor - Process allCycleHeatmap files'
    )
    parser.add_argument('--chunk-size', type=float, default=5.0,
                       help='Target chunk size in GB (default: 5.0)')
    parser.add_argument('--move-files', action='store_true',
                       help='Move processed files to processed_files/')
    
    args = parser.parse_args()
    
    processor = ACHMBatchProcessor(
        target_chunk_gb=args.chunk_size
    )
    
    return processor.run(move_files=args.move_files)


if __name__ == "__main__":
    sys.exit(main())