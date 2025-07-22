#!/usr/bin/env python3
"""
JPX Master Data Processor - Orchestrator Script
Coordinates downloading and parsing of JPX ETF/REIT monthly data

Author: AfricaAI Data Team
Version: 1.0
Date: 2025-07-18
"""

import os
import sys
import json
import time
import logging
import subprocess
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import configparser
from dataclasses import dataclass
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('jpx_master_processor.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ProcessingConfig:
    """Configuration for JPX data processing"""
    target_months: List[str]  # Format: ['2025-06', '2025-07']
    download_retries: int = 3
    parse_retries: int = 2
    base_output_dir: str = "JPX_Processing_Output"
    keep_raw_files: bool = True
    validation_enabled: bool = False
    
    @classmethod
    def from_config_file(cls, config_path: str = "jpx_config.ini"):
        """Load configuration from INI file"""
        config = configparser.ConfigParser()
        
        # Create default config if it doesn't exist
        if not Path(config_path).exists():
            cls._create_default_config(config_path)
        
        config.read(config_path)
        
        # Parse target months
        months_str = config.get('PROCESSING', 'target_months', fallback='')
        target_months = [m.strip() for m in months_str.split(',') if m.strip()]
        
        return cls(
            target_months=target_months,
            download_retries=config.getint('PROCESSING', 'download_retries', fallback=3),
            parse_retries=config.getint('PROCESSING', 'parse_retries', fallback=2),
            base_output_dir=config.get('PATHS', 'base_output_dir', fallback='JPX_Processing_Output'),
            keep_raw_files=config.getboolean('PROCESSING', 'keep_raw_files', fallback=True),
            validation_enabled=config.getboolean('PROCESSING', 'validation_enabled', fallback=False)
        )
    
    @staticmethod
    def _create_default_config(config_path: str):
        """Create a default configuration file"""
        config = configparser.ConfigParser()
        
        config['PROCESSING'] = {
            'target_months': '2025-06,2025-07',  # Example months
            'download_retries': '3',
            'parse_retries': '2',
            'keep_raw_files': 'true',
            'validation_enabled': 'false'
        }
        
        config['PATHS'] = {
            'base_output_dir': 'JPX_Processing_Output',
            'download_script': 'final.py',
            'parser_script': 'jtmd_scraper5.py'
        }
        
        config['SCRIPTS'] = {
            'download_timeout': '1800',  # 30 minutes
            'parse_timeout': '600'       # 10 minutes
        }
        
        with open(config_path, 'w') as configfile:
            config.write(configfile)
        
        logger.info(f"Created default configuration file: {config_path}")
        logger.info("Please edit the configuration file to set your target months")

class JPXMasterProcessor:
    """
    Master orchestrator for JPX data processing pipeline
    Coordinates downloading and parsing with robust error handling
    """
    
    def __init__(self, config_path: str = "jpx_config.ini"):
        """Initialize the master processor"""
        self.config = ProcessingConfig.from_config_file(config_path)
        self.base_dir = Path(self.config.base_output_dir)
        self.base_dir.mkdir(exist_ok=True)
        
        # Script paths
        self.download_script = Path("final.py")
        self.parser_script = Path("jtmd_scraper5.py")
        
        # Processing state
        self.processing_state = {}
        self.state_file = self.base_dir / "processing_state.json"
        
        # Load existing state
        self._load_processing_state()
        
        logger.info("="*60)
        logger.info("JPX Master Data Processor Initialized")
        logger.info("="*60)
        logger.info(f"Target months: {self.config.target_months}")
        logger.info(f"Output directory: {self.base_dir}")
        logger.info(f"Download retries: {self.config.download_retries}")
        logger.info(f"Parse retries: {self.config.parse_retries}")
        logger.info("="*60)
    
    def _load_processing_state(self):
        """Load processing state from file"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    self.processing_state = json.load(f)
                logger.info(f"Loaded processing state: {len(self.processing_state)} records")
            except Exception as e:
                logger.warning(f"Could not load processing state: {e}")
                self.processing_state = {}
        else:
            self.processing_state = {}
    
    def _save_processing_state(self):
        """Save current processing state to file"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.processing_state, f, indent=2)
            logger.debug("Processing state saved")
        except Exception as e:
            logger.error(f"Could not save processing state: {e}")
    
    def _update_month_state(self, month: str, phase: str, status: str, details: str = ""):
        """Update processing state for a specific month and phase"""
        if month not in self.processing_state:
            self.processing_state[month] = {}
        
        self.processing_state[month][phase] = {
            'status': status,
            'timestamp': datetime.now().isoformat(),
            'details': details
        }
        self._save_processing_state()
    
    def _run_subprocess_with_retry(self, command: List[str], max_retries: int, 
                                  timeout: int = 1800, description: str = "") -> Tuple[bool, str]:
        """
        Run subprocess with retry mechanism
        
        Args:
            command: Command to run
            max_retries: Maximum number of retry attempts
            timeout: Timeout in seconds
            description: Description for logging
            
        Returns:
            Tuple of (success: bool, output: str)
        """
        for attempt in range(max_retries):
            try:
                logger.info(f"🔄 {description} - Attempt {attempt + 1}/{max_retries}")
                logger.debug(f"Command: {' '.join(command)}")
                
                # Run the subprocess
                result = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                    cwd=Path.cwd()
                )
                
                if result.returncode == 0:
                    logger.info(f"✅ {description} - Success on attempt {attempt + 1}")
                    return True, result.stdout
                else:
                    error_msg = f"Exit code {result.returncode}: {result.stderr}"
                    logger.warning(f"⚠️ {description} - Failed attempt {attempt + 1}: {error_msg}")
                    
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 30  # Progressive backoff
                        logger.info(f"⏳ Waiting {wait_time} seconds before retry...")
                        time.sleep(wait_time)
                    
            except subprocess.TimeoutExpired:
                error_msg = f"Process timed out after {timeout} seconds"
                logger.error(f"⏰ {description} - Timeout on attempt {attempt + 1}")
                
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 60  # Longer wait after timeout
                    logger.info(f"⏳ Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                    
            except Exception as e:
                error_msg = f"Unexpected error: {str(e)}"
                logger.error(f"❌ {description} - Error on attempt {attempt + 1}: {error_msg}")
                
                if attempt < max_retries - 1:
                    time.sleep(30)
        
        # All attempts failed
        final_error = f"All {max_retries} attempts failed"
        logger.error(f"❌ {description} - {final_error}")
        return False, final_error
    
    def _verify_scripts_exist(self) -> bool:
        """Verify that required scripts exist"""
        missing_scripts = []
        
        if not self.download_script.exists():
            missing_scripts.append(str(self.download_script))
        
        if not self.parser_script.exists():
            missing_scripts.append(str(self.parser_script))
        
        if missing_scripts:
            logger.error(f"❌ Missing required scripts: {missing_scripts}")
            return False
        
        logger.info("✅ All required scripts found")
        return True
    
    def download_data_for_month(self, target_month: str) -> bool:
        """
        Download data for a specific month using the download script
        
        Args:
            target_month: Month in format YYYY-MM
            
        Returns:
            bool: Success status
        """
        logger.info(f"📥 Starting download for month: {target_month}")
        
        # Update state
        self._update_month_state(target_month, "download", "started", "Beginning download process")
        
        try:
            # Prepare download command
            download_command = [sys.executable, str(self.download_script)]
            
            # Run download with retries
            success, output = self._run_subprocess_with_retry(
                command=download_command,
                max_retries=self.config.download_retries,
                timeout=1800,  # 30 minutes
                description=f"Data download for {target_month}"
            )
            
            if success:
                self._update_month_state(target_month, "download", "completed", "Download successful")
                logger.info(f"✅ Download completed for {target_month}")
                return True
            else:
                self._update_month_state(target_month, "download", "failed", output)
                logger.error(f"❌ Download failed for {target_month}: {output}")
                return False
                
        except Exception as e:
            error_msg = f"Download exception: {str(e)}"
            self._update_month_state(target_month, "download", "error", error_msg)
            logger.error(f"❌ Download error for {target_month}: {error_msg}")
            return False
    
    def _prepare_parser_input(self, target_month: str) -> Optional[Path]:
        """
        Prepare input folder for parser by organizing downloaded files
        
        Args:
            target_month: Month in format YYYY-MM (e.g., "2025-06")
            
        Returns:
            Path to parser input folder or None if preparation failed
        """
        logger.info(f"📁 Preparing parser input for {target_month}")
        
        try:
            # Parse the target month
            year, month_str = target_month.split('-')
            month_int = int(month_str)  # Convert to integer for formatting
            
            # Look for downloaded files in the JPX_Monthly_Reports structure
            download_base = Path("JPX_Monthly_Reports")
            
            # Create parser input directory
            parser_input_dir = self.base_dir / target_month / "parser_input"
            parser_input_dir.mkdir(parents=True, exist_ok=True)
            
            # Find files for the target month
            files_found = []
            files_copied = set()  # Track to avoid duplicates
            
            logger.info(f"🔍 Searching for files matching month {target_month} (year={year}, month={month_int:02d})")
            
            if not download_base.exists():
                logger.error(f"📂 Download base directory does not exist: {download_base}")
                return None
            
            # Log the complete directory structure for debugging
            logger.info("📂 Complete download directory structure:")
            for item in download_base.rglob("*"):
                rel_path = item.relative_to(download_base)
                if item.is_file():
                    logger.info(f"  📄 {rel_path}")
                elif item.is_dir():
                    logger.info(f"  📁 {rel_path}/")
            
            # Search strategy 1: Look through year folders for month subfolders
            year_patterns = [year, f"{year}*"]  # e.g., "2025" or "2025*"
            
            for year_pattern in year_patterns:
                for year_folder in download_base.glob(year_pattern):
                    if year_folder.is_dir():
                        logger.debug(f"🔍 Searching in year folder: {year_folder}")
                        
                        # Look for month folders with multiple patterns
                        month_patterns = [
                            f"{month_int:02d}_*",         # e.g., "06_June"
                            f"*_{month_int:02d}_*",       # e.g., "something_06_something"
                            f"*{month_int:02d}*",         # fallback pattern
                            f"*{month_str}*",             # e.g., "*06*"
                            "*"                           # Search all subfolders as fallback
                        ]
                        
                        for pattern in month_patterns:
                            logger.debug(f"  🔍 Trying pattern: {pattern}")
                            for month_folder in year_folder.glob(pattern):
                                if month_folder.is_dir():
                                    logger.debug(f"    📁 Checking folder: {month_folder}")
                                    # Look for Excel files in this folder
                                    for excel_file in month_folder.glob("*.xls*"):
                                        # Check if this file matches our target month
                                        filename = excel_file.name.lower()
                                        month_pattern = f"m{year[2:]}{month_int:02d}"  # e.g., "m2506"
                                        
                                        if month_pattern in filename and excel_file.name not in files_copied:
                                            dest_file = parser_input_dir / excel_file.name
                                            shutil.copy2(excel_file, dest_file)
                                            files_found.append(excel_file.name)
                                            files_copied.add(excel_file.name)
                                            logger.info(f"    ✅ Copied: {excel_file.name} from {month_folder}")
            
            # Search strategy 2: Look directly in year folders for Excel files
            for year_pattern in year_patterns:
                for year_folder in download_base.glob(year_pattern):
                    if year_folder.is_dir():
                        logger.debug(f"🔍 Searching directly in year folder: {year_folder}")
                        for excel_file in year_folder.glob("*.xls*"):
                            filename = excel_file.name.lower()
                            month_pattern = f"m{year[2:]}{month_int:02d}"  # e.g., "m2506"
                            
                            if month_pattern in filename and excel_file.name not in files_copied:
                                dest_file = parser_input_dir / excel_file.name
                                shutil.copy2(excel_file, dest_file)
                                files_found.append(excel_file.name)
                                files_copied.add(excel_file.name)
                                logger.info(f"    ✅ Copied: {excel_file.name} from {year_folder}")
            
            # Search strategy 3: Recursive search for any Excel files matching the pattern
            logger.debug(f"🔍 Recursive search for pattern: m{year[2:]}{month_int:02d}")
            for excel_file in download_base.rglob("*.xls*"):
                filename = excel_file.name.lower()
                month_pattern = f"m{year[2:]}{month_int:02d}"  # e.g., "m2506"
                
                if month_pattern in filename and excel_file.name not in files_copied:
                    dest_file = parser_input_dir / excel_file.name
                    shutil.copy2(excel_file, dest_file)
                    files_found.append(excel_file.name)
                    files_copied.add(excel_file.name)
                    logger.info(f"    ✅ Copied: {excel_file.name} from {excel_file.parent}")
            
            # Remove duplicates and sort
            files_found = sorted(list(set(files_found)))
            
            if files_found:
                logger.info(f"✅ Successfully prepared {len(files_found)} unique files for parsing:")
                for file in files_found:
                    logger.info(f"  📄 {file}")
                return parser_input_dir
            else:
                logger.warning(f"⚠️ No files found for {target_month}")
                logger.warning(f"🔍 Expected pattern: files containing 'm{year[2:]}{month_int:02d}'")
                
                # Show what files ARE available
                available_files = list(download_base.rglob("*.xls*"))
                if available_files:
                    logger.info("📄 Available Excel files found:")
                    for file in available_files:
                        logger.info(f"  📄 {file.relative_to(download_base)}")
                else:
                    logger.warning("📄 No Excel files found in download directory at all")
                
                return None
                
        except Exception as e:
            logger.error(f"❌ Error preparing parser input for {target_month}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def parse_data_for_month(self, target_month: str) -> bool:
        """
        Parse data for a specific month using the parser script
        
        Args:
            target_month: Month in format YYYY-MM
            
        Returns:
            bool: Success status
        """
        logger.info(f"🔍 Starting parsing for month: {target_month}")
        
        # Update state
        self._update_month_state(target_month, "parsing", "started", "Beginning parsing process")
        
        try:
            # Prepare input files for parser
            parser_input_dir = self._prepare_parser_input(target_month)
            if not parser_input_dir:
                error_msg = "No input files prepared for parsing"
                self._update_month_state(target_month, "parsing", "failed", error_msg)
                return False
            
            # Create parser output directory
            parser_output_dir = self.base_dir / target_month / "parser_output"
            parser_output_dir.mkdir(parents=True, exist_ok=True)
            
            # Prepare parser command
            parser_command = [
                sys.executable, str(self.parser_script),
                "--input", str(parser_input_dir),
                "--output", str(parser_output_dir)
            ]
            
            # Run parser with retries
            success, output = self._run_subprocess_with_retry(
                command=parser_command,
                max_retries=self.config.parse_retries,
                timeout=600,  # 10 minutes
                description=f"Data parsing for {target_month}"
            )
            
            if success:
                # Verify output files were created
                csv_files = list(parser_output_dir.glob("JTMD_DATA_*.csv"))
                zip_files = list(parser_output_dir.glob("JTMD_*.zip"))
                
                if csv_files and zip_files:
                    self._update_month_state(target_month, "parsing", "completed", 
                                           f"Parsing successful. Generated: {len(csv_files)} CSV, {len(zip_files)} ZIP")
                    logger.info(f"✅ Parsing completed for {target_month}")
                    logger.info(f"📄 Generated files: {[f.name for f in csv_files + zip_files]}")
                    return True
                else:
                    error_msg = "Parsing completed but expected output files not found"
                    self._update_month_state(target_month, "parsing", "failed", error_msg)
                    logger.error(f"❌ {error_msg} for {target_month}")
                    return False
            else:
                self._update_month_state(target_month, "parsing", "failed", output)
                logger.error(f"❌ Parsing failed for {target_month}: {output}")
                return False
                
        except Exception as e:
            error_msg = f"Parsing exception: {str(e)}"
            self._update_month_state(target_month, "parsing", "error", error_msg)
            logger.error(f"❌ Parsing error for {target_month}: {error_msg}")
            return False
    
    def validate_month_output(self, target_month: str) -> bool:
        """
        Validate output for a specific month
        
        Args:
            target_month: Month in format YYYY-MM
            
        Returns:
            bool: Validation success
        """
        if not self.config.validation_enabled:
            logger.info(f"📋 Validation disabled for {target_month}")
            return True
        
        logger.info(f"🔍 Validating output for {target_month}")
        
        try:
            parser_output_dir = self.base_dir / target_month / "parser_output"
            
            # Check for required files
            csv_files = list(parser_output_dir.glob("JTMD_DATA_*.csv"))
            meta_files = list(parser_output_dir.glob("JTMD_META_*.csv"))
            zip_files = list(parser_output_dir.glob("JTMD_*.zip"))
            
            validation_issues = []
            
            if not csv_files:
                validation_issues.append("No data CSV files found")
            
            if not meta_files:
                validation_issues.append("No metadata CSV files found")
            
            if not zip_files:
                validation_issues.append("No ZIP archive files found")
            
            # Basic CSV validation
            if csv_files:
                import pandas as pd
                csv_file = csv_files[0]
                try:
                    df = pd.read_csv(csv_file, header=None)
                    if df.shape[1] != 85:
                        validation_issues.append(f"CSV has {df.shape[1]} columns, expected 85")
                    if df.shape[0] < 3:
                        validation_issues.append(f"CSV has only {df.shape[0]} rows, expected at least 3")
                except Exception as e:
                    validation_issues.append(f"Could not read CSV file: {str(e)}")
            
            if validation_issues:
                error_msg = f"Validation issues: {', '.join(validation_issues)}"
                self._update_month_state(target_month, "validation", "failed", error_msg)
                logger.warning(f"⚠️ Validation issues for {target_month}: {error_msg}")
                return False
            else:
                self._update_month_state(target_month, "validation", "passed", "All validation checks passed")
                logger.info(f"✅ Validation passed for {target_month}")
                return True
                
        except Exception as e:
            error_msg = f"Validation exception: {str(e)}"
            self._update_month_state(target_month, "validation", "error", error_msg)
            logger.error(f"❌ Validation error for {target_month}: {error_msg}")
            return False
    
    def process_single_month(self, target_month: str) -> bool:
        """
        Process a single month through the complete pipeline
        
        Args:
            target_month: Month in format YYYY-MM
            
        Returns:
            bool: Overall success status
        """
        logger.info("="*60)
        logger.info(f"🚀 PROCESSING MONTH: {target_month}")
        logger.info("="*60)
        
        start_time = time.time()
        
        try:
            # Phase 1: Download
            logger.info("📥 PHASE 1: DOWNLOADING DATA")
            if not self.download_data_for_month(target_month):
                logger.error(f"❌ Download phase failed for {target_month}")
                return False
            
            # Small delay between phases
            time.sleep(5)
            
            # Phase 2: Parse
            logger.info("🔍 PHASE 2: PARSING DATA")
            if not self.parse_data_for_month(target_month):
                logger.error(f"❌ Parsing phase failed for {target_month}")
                return False
            
            # Small delay between phases
            time.sleep(2)
            
            # Phase 3: Validate
            logger.info("📋 PHASE 3: VALIDATING OUTPUT")
            if not self.validate_month_output(target_month):
                logger.warning(f"⚠️ Validation phase had issues for {target_month}")
                # Don't fail the entire process for validation issues
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            # Update final state
            self._update_month_state(target_month, "complete", "success", 
                                   f"Full pipeline completed in {processing_time:.1f} seconds")
            
            logger.info("="*60)
            logger.info(f"✅ MONTH {target_month} PROCESSING COMPLETE!")
            logger.info(f"⏱️ Total processing time: {processing_time:.1f} seconds")
            logger.info("="*60)
            
            return True
            
        except Exception as e:
            error_msg = f"Pipeline exception: {str(e)}"
            self._update_month_state(target_month, "complete", "error", error_msg)
            logger.error(f"❌ Pipeline error for {target_month}: {error_msg}")
            logger.error(traceback.format_exc())
            return False
    
    def process_all_months(self) -> Dict[str, bool]:
        """
        Process all configured months
        
        Returns:
            Dict mapping month to success status
        """
        if not self.config.target_months:
            logger.error("❌ No target months configured!")
            return {}
        
        if not self._verify_scripts_exist():
            logger.error("❌ Required scripts not found!")
            return {}
        
        logger.info("🎯 STARTING MULTI-MONTH PROCESSING")
        logger.info(f"📅 Target months: {self.config.target_months}")
        
        results = {}
        successful_months = []
        failed_months = []
        
        overall_start_time = time.time()
        
        for month in self.config.target_months:
            try:
                logger.info(f"\n🔄 Processing month {len(results) + 1}/{len(self.config.target_months)}: {month}")
                
                success = self.process_single_month(month)
                results[month] = success
                
                if success:
                    successful_months.append(month)
                    logger.info(f"✅ {month} completed successfully")
                else:
                    failed_months.append(month)
                    logger.error(f"❌ {month} processing failed")
                
                # Brief pause between months
                if month != self.config.target_months[-1]:  # Not the last month
                    logger.info("⏳ Brief pause before next month...")
                    time.sleep(10)
                    
            except KeyboardInterrupt:
                logger.warning(f"⚠️ Processing interrupted by user during {month}")
                results[month] = False
                break
            except Exception as e:
                logger.error(f"❌ Unexpected error processing {month}: {str(e)}")
                results[month] = False
                failed_months.append(month)
        
        # Calculate overall processing time
        overall_time = time.time() - overall_start_time
        
        # Final summary
        logger.info("\n" + "="*60)
        logger.info("📊 FINAL PROCESSING SUMMARY")
        logger.info("="*60)
        logger.info(f"✅ Successful months: {len(successful_months)}/{len(self.config.target_months)}")
        logger.info(f"❌ Failed months: {len(failed_months)}/{len(self.config.target_months)}")
        logger.info(f"⏱️ Total processing time: {overall_time:.1f} seconds")
        
        if successful_months:
            logger.info(f"✅ Success: {', '.join(successful_months)}")
        
        if failed_months:
            logger.info(f"❌ Failed: {', '.join(failed_months)}")
        
        logger.info("="*60)
        
        return results
    
    def get_processing_summary(self) -> Dict:
        """Get summary of processing status for all months"""
        summary = {
            'total_months': len(self.config.target_months),
            'months_status': {},
            'overall_status': 'unknown'
        }
        
        completed_count = 0
        
        for month in self.config.target_months:
            if month in self.processing_state:
                month_data = self.processing_state[month]
                if 'complete' in month_data:
                    summary['months_status'][month] = month_data['complete']['status']
                    if month_data['complete']['status'] == 'success':
                        completed_count += 1
                else:
                    summary['months_status'][month] = 'incomplete'
            else:
                summary['months_status'][month] = 'not_started'
        
        # Determine overall status
        if completed_count == len(self.config.target_months):
            summary['overall_status'] = 'all_complete'
        elif completed_count > 0:
            summary['overall_status'] = 'partial_complete'
        else:
            summary['overall_status'] = 'none_complete'
        
        return summary


def main():
    """Main function to run the JPX Master Processor"""
    import argparse
    
    parser = argparse.ArgumentParser(description='JPX Master Data Processor')
    parser.add_argument('--config', '-c', default='jpx_config.ini',
                       help='Configuration file path (default: jpx_config.ini)')
    parser.add_argument('--month', '-m', 
                       help='Process single month (format: YYYY-MM)')
    parser.add_argument('--status', '-s', action='store_true',
                       help='Show processing status summary')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Initialize processor
        processor = JPXMasterProcessor(args.config)
        
        if args.status:
            # Show status summary
            summary = processor.get_processing_summary()
            print("\n" + "="*50)
            print("📊 PROCESSING STATUS SUMMARY")
            print("="*50)
            print(f"Total configured months: {summary['total_months']}")
            print(f"Overall status: {summary['overall_status']}")
            print("\nMonth-by-month status:")
            for month, status in summary['months_status'].items():
                status_icon = "✅" if status == "success" else "❌" if status in ["failed", "error"] else "⏳"
                print(f"  {status_icon} {month}: {status}")
            print("="*50)
            return
        
        if args.month:
            # Process single month
            logger.info(f"🎯 Single month processing: {args.month}")
            # Temporarily override config
            processor.config.target_months = [args.month]
            success = processor.process_single_month(args.month)
            
            if success:
                print(f"\n✅ Month {args.month} processed successfully!")
                exit(0)
            else:
                print(f"\n❌ Month {args.month} processing failed!")
                exit(1)
        else:
            # Process all configured months
            results = processor.process_all_months()
            
            success_count = sum(1 for success in results.values() if success)
            
            if success_count == len(results):
                print("\n🎉 ALL MONTHS PROCESSED SUCCESSFULLY!")
                exit(0)
            elif success_count > 0:
                print(f"\n⚠️ PARTIAL SUCCESS: {success_count}/{len(results)} months processed")
                exit(2)
            else:
                print("\n❌ ALL PROCESSING FAILED!")
                exit(1)
                
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Processing interrupted by user")
        print("\n⚠️ Processing interrupted by user")
        exit(130)
    except Exception as e:
        logger.error(f"❌ Critical error: {str(e)}")
        logger.error(traceback.format_exc())
        print(f"\n❌ Critical error: {str(e)}")
        exit(1)


if __name__ == "__main__":
    main()

