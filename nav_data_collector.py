import pandas as pd
from pathlib import Path
import logging
import tempfile
from typing import Dict, List, Tuple, Optional, Set, Union
from ftp_service import FTPService
from datetime import datetime
import concurrent.futures
from config import AppConfig, FILE_PATTERNS, EMITTERS, FILE_TYPES
import threading
from queue import Queue
import time

logger = logging.getLogger(__name__)


class NAVDataCollector:
    """Handles collecting NAV data from various sources"""

    def __init__(self, config: AppConfig):
        """
        Initialize the NAV data collector

        Args:
            config: Application configuration
        """
        self.config = config
        self.mode = config.mode.lower()
        self.ftp_service = FTPService(
            config.ftp_configs) if config.ftp_configs else None
        self.input_dir = Path(config.input_dir)
        self.template_dir = Path(config.template_dir)
        self.temp_dir = Path(tempfile.gettempdir()) / "nav_processor"
        self.max_workers = config.max_workers

        # Create directories
        if self.mode == "local":
            self._create_directories()

        # Thread-safe queue for Google Drive uploads if needed
        self.upload_queue = Queue() if config.drive_config else None

    def _create_directories(self):
        """Create necessary directories for local mode"""
        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        # Create emitter subdirectories
        if self.ftp_service:
            for emitter in self.ftp_service.config.keys():
                (self.input_dir / emitter).mkdir(exist_ok=True)

    def cleanup(self):
        """Clean up temporary files and directories"""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
            self.temp_dir.mkdir(parents=True, exist_ok=True)
            logger.info("Temporary files cleaned up")

    def cleanup_input_directories(self):
        """Clean up old files from input directories"""
        if self.ftp_service:
            for emitter in self.ftp_service.config.keys():
                self._cleanup_emitter_directory(emitter)

    def _cleanup_emitter_directory(self, emitter: str):
        """Clean up old files from emitter directory"""
        emitter_dir = self.input_dir / emitter
        if emitter_dir.exists():
            for file in emitter_dir.glob('*.csv'):
                file.unlink()
            logger.info(f"Cleaned up old files from {emitter} directory")

    def get_input_file_paths(self) -> List[Path]:
        """
        Get paths to all downloaded CSV files in input directories

        Returns:
            List of Path objects to CSV files
        """
        input_files = []

        # Collect CSV files from each emitter directory
        if self.ftp_service:
            for emitter in self.ftp_service.config.keys():
                emitter_dir = self.input_dir / emitter
                if emitter_dir.exists():
                    for file_path in emitter_dir.glob('*.csv'):
                        if file_path.is_file():
                            input_files.append(file_path)

        logger.debug(f"Found {len(input_files)} input CSV files")
        return input_files

    def _get_input_file_list(self, date_str: str) -> List[Tuple[str, str]]:
        """
        Generate list of input files with their emitters for a given date.

        Args:
            date_str: Date string in format MMDDYYYY

        Returns:
            List of (emitter, filename) tuples
        """
        input_files = []

        for emitter in EMITTERS:
            for file_type in FILE_TYPES:
                pattern = FILE_PATTERNS[file_type]
                filename = pattern.format(date_str=date_str, emitter=emitter)
                input_files.append((emitter, filename))

        return input_files

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize DataFrame

        Args:
            df: Raw DataFrame to clean

        Returns:
            Cleaned DataFrame
        """
        # Remove unnamed columns
        unnamed_cols = [col for col in df.columns if 'Unnamed' in str(col)]
        if unnamed_cols:
            df = df.drop(columns=unnamed_cols)

        # Clean up column names
        df.columns = df.columns.str.strip()

        # Convert date column to datetime
        if 'Valuation Period-End Date' in df.columns:
            df['Valuation Period-End Date'] = pd.to_datetime(
                df['Valuation Period-End Date'], errors='coerce')

        # Clean up ISIN values
        if 'ISIN' in df.columns:
            df['ISIN'] = df['ISIN'].str.strip()

        # Clean up NAV values
        if 'NAV' in df.columns:
            df['NAV'] = pd.to_numeric(
                df['NAV'].astype(str).str.replace(',', ''),
                errors='coerce'
            )

        # Standardize frequency values to uppercase
        if 'Frequency' in df.columns:
            df['Frequency'] = df['Frequency'].str.upper()

        return df

    def _process_ftp_file(self, emitter: str, filename: str) -> Optional[pd.DataFrame]:
        """
        Process a single FTP file download

        Args:
            emitter: The emitter name
            filename: The filename to download

        Returns:
            DataFrame if successful, None otherwise
        """
        if not self.ftp_service:
            logger.warning("FTP service not configured")
            return None

        temp_file = self.temp_dir / f"{emitter}_{filename}"

        try:
            # Download the file
            df = self.ftp_service.download_file(emitter, filename, temp_file)

            if df is not None:
                # Clean up the DataFrame
                df = self._clean_dataframe(df)

                # Save to input directory
                input_path = self.input_dir / emitter / filename
                input_path.parent.mkdir(exist_ok=True)
                df.to_csv(input_path, index=False)

                # Add file type based on filename
                if 'Wrappers Hybrid' in filename:
                    df['file_type'] = 'hybrid'
                elif 'Loan' in filename:
                    df['file_type'] = 'loan'
                else:
                    df['file_type'] = 'standard'

                # Only log non-empty dataframes
                if not df.empty:
                    logger.debug(f"Processed {emitter} file: {filename}")

                return df if not df.empty else None

        except Exception as e:
            # Only log non-404 errors
            if "550" not in str(e):
                logger.error(
                    f"Error processing {filename} from {emitter}: {str(e)}")
            return None

        finally:
            # Clean up temp file
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except Exception:
                    pass

    def _read_exclude_isins(self) -> Set[str]:
        """
        Read and return set of ISINs to exclude

        Returns:
            Set of ISINs to exclude
        """
        exclude_isins = set()
        exclude_isins_path = self.template_dir / "Exclude ISINs.csv"

        if exclude_isins_path.exists():
            try:
                exclude_isins = set(pd.read_csv(
                    exclude_isins_path, header=None)[0].str.strip())
                logger.info(f"Loaded {len(exclude_isins)} ISINs to exclude")
            except Exception as e:
                logger.error(f"Error reading exclude ISINs: {str(e)}")

        return exclude_isins

    def collect_nav_data(self, date_str: str, target_isins: Optional[Set[str]] = None,
                         exclude_isins: Optional[Set[str]] = None,
                         file_type: Optional[str] = None) -> List[Tuple[str, pd.DataFrame]]:
        """
        Collect NAV data from all sources

        Args:
            date_str: Date string in format MMDDYYYY
            target_isins: Optional set of target ISINs to filter for
            exclude_isins: Optional set of ISINs to exclude
            file_type: Optional file type to filter for ('hybrid', 'loan'). None to include all.

        Returns:
            List of (emitter, dataframe) tuples
        """
        nav_dfs = []
        missing_files = []
        input_files = self._get_input_file_list(date_str)

        # Convert date_str to datetime for filtering
        # date_str is in format MMDDYYYY
        try:
            target_date = datetime.strptime(date_str, '%m%d%Y').date()
            logger.info(f"Filtering for NAV data on {target_date}")
        except ValueError:
            logger.error(f"Invalid date format: {date_str}, expected MMDDYYYY")
            raise ValueError(
                f"Invalid date format: {date_str}, expected MMDDYYYY")

        # Process files concurrently with a smaller number of workers
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(3, self.max_workers)) as executor:
            future_to_file = {
                executor.submit(self._process_ftp_file, emitter, filename): (emitter, filename)
                for emitter, filename in input_files
            }

            for future in concurrent.futures.as_completed(future_to_file):
                emitter, filename = future_to_file[future]
                try:
                    df = future.result()
                    if df is None:
                        missing_files.append(filename)
                        continue

                    # Drop rows with missing required values
                    required_cols = ['ISIN', 'NAV',
                                     'Valuation Period-End Date']
                    df = df.dropna(subset=required_cols)

                    # Apply date filter - keep only rows for the specified date
                    df['Date'] = df['Valuation Period-End Date'].dt.date
                    df = df[df['Date'] == target_date]

                    # Remove the temporary date column
                    df = df.drop(columns=['Date'])

                    # Apply ISIN filters
                    if target_isins:
                        df = df[df['ISIN'].isin(target_isins)]
                    if exclude_isins:
                        df = df[~df['ISIN'].isin(exclude_isins)]

                    # Apply file_type filter if specified
                    if file_type and 'file_type' in df.columns:
                        df = df[df['file_type'] == file_type]

                    if not df.empty:
                        nav_dfs.append((emitter, df))
                        logger.info(
                            f"Processed {emitter} file: {filename} with {len(df)} entries for {target_date}")
                    else:
                        logger.info(
                            f"No entries found for {target_date} in {emitter} file: {filename}")

                except Exception as e:
                    logger.error(f"Error processing {filename}: {str(e)}")

        if missing_files:
            logger.warning(
                f"Some files were not found: {len(missing_files)} files")

        return nav_dfs
