import logging
from typing import List, Dict, Tuple, Optional, Set, Union
from pathlib import Path
from datetime import datetime
from config import AppConfig, DEFAULT_FTP_CONFIGS
from nav_data_collector import NAVDataCollector
from template_manager import TemplateManager
from nav_distributor import NAVDistributor
from db_manager import DBManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NAVProcessor:
    """Orchestrates the NAV processing workflow"""

    def __init__(self, config: AppConfig = None, mode: str = "local",
                 ftp_configs: Dict = None, smtp_config: Dict = None,
                 drive_config: Dict = None, db_connection_string: str = 'sqlite:///nav_data.db',
                 max_workers: int = 5):
        """
        Initialize the NAV processor

        Args:
            config: Application configuration
            mode: Operation mode - either "local" or "remote"
            ftp_configs: Dictionary of FTP configurations for each emitter
            smtp_config: SMTP configuration for email sending
            drive_config: Google Drive configuration for file syncing
            db_connection_string: Database connection string
            max_workers: Maximum number of concurrent workers for file operations
        """
        # Create config if not provided
        if config is None:
            config = AppConfig(
                mode=mode,
                ftp_configs=ftp_configs,
                smtp_config=smtp_config,
                drive_config=drive_config,
                db_connection_string=db_connection_string,
                max_workers=max_workers
            )

        self.config = config

        # Initialize specialized components
        self.collector = NAVDataCollector(config)
        self.template_manager = TemplateManager(config)
        self.distributor = NAVDistributor(config)
        self.db_manager = DBManager(config)

        # Configure logging level
        if config.log_level:
            self._configure_logging(config.log_level)

    def _configure_logging(self, log_level: str):
        """Configure logging based on the specified level"""
        level = getattr(logging, log_level.upper(), logging.INFO)
        logger.setLevel(level)

        # Set more restrictive logging for noisy libraries if not in debug mode
        if level > logging.DEBUG:
            logging.getLogger('ftplib').setLevel(logging.WARNING)
            logging.getLogger('googleapiclient').setLevel(logging.WARNING)
            logging.getLogger('google_auth_httplib2').setLevel(logging.WARNING)
            logging.getLogger('googleapiclient.discovery').setLevel(
                logging.WARNING)
            logging.getLogger('urllib3').setLevel(logging.WARNING)

    def set_debug_logging(self, enable=True):
        """Enable or disable detailed debug logging"""
        if enable:
            # Set verbose logging
            logger.setLevel(logging.DEBUG)
            logging.getLogger('googleapiclient').setLevel(logging.DEBUG)
            logging.getLogger('google_auth_httplib2').setLevel(logging.DEBUG)
            logging.getLogger('ftplib').setLevel(logging.DEBUG)
        else:
            # Restore normal logging
            logger.setLevel(logging.INFO)
            logging.getLogger('googleapiclient').setLevel(logging.WARNING)
            logging.getLogger('google_auth_httplib2').setLevel(logging.WARNING)
            logging.getLogger('ftplib').setLevel(logging.WARNING)

    def cleanup(self):
        """Clean up temporary files and directories"""
        self.collector.cleanup()

    def process_navs(self, date_str: str, send_email: bool = False, to_emails: List[str] = None,
                     isin_filter: Union[str, List[str], None] = None,
                     distribution_type: str = 'morningstar',
                     template_types: List[str] = ['morningstar', 'six'],
                     file_type: Optional[str] = None):
        """
        Process NAV files and update templates

        Args:
            date_str: Date string in format MMDDYYYY
            send_email: Whether to send email report
            to_emails: List of email recipients
            isin_filter: ISIN filter specification. Can be:
                - A predefined frequency ("daily", "weekly", "monthly", "quarterly")
                - A predefined product type ("wrappers_hybrid", "loan")
                - A list of predefined frequencies or product types (e.g., ["daily", "wrappers_hybrid"])
                - A specific ISIN or list of ISINs
                - None to process all ISINs
            distribution_type: Type of distribution
            template_types: List of templates to update ('morningstar', 'six')
            file_type: Optional filter by file type ('hybrid', 'loan'). This filters based on filename patterns.
        """
        try:
            # Clean up directories
            self.template_manager.cleanup_output_directory()
            self.collector.cleanup_input_directories()

            # Map filter types to file types when needed
            derived_file_type = file_type
            if not derived_file_type:
                # Check if we need to derive file_type from isin_filter
                if isinstance(isin_filter, str):
                    if isin_filter.lower() == 'wrappers_hybrid':
                        derived_file_type = 'hybrid'
                    elif isin_filter.lower() == 'loan':
                        derived_file_type = 'loan'
                elif isinstance(isin_filter, list):
                    # If only one filter type is selected and it's a product type, use that for file_type
                    if len(isin_filter) == 1:
                        if isin_filter[0].lower() == 'wrappers_hybrid':
                            derived_file_type = 'hybrid'
                        elif isin_filter[0].lower() == 'loan':
                            derived_file_type = 'loan'

            # Process ISIN filters (for frequencies, we still need these)
            target_isins = None
            if isin_filter:
                # Filter out product types that we're handling via file_type
                if isinstance(isin_filter, list):
                    freq_filters = [f for f in isin_filter if f.lower() not in [
                        'wrappers_hybrid', 'loan']]
                    if freq_filters:
                        target_isins = self.db_manager.get_target_isins(
                            freq_filters)
                elif isinstance(isin_filter, str) and isin_filter.lower() not in ['wrappers_hybrid', 'loan']:
                    target_isins = self.db_manager.get_target_isins(
                        isin_filter)

            if target_isins:
                logger.info(
                    f"Filtering for ISINs: {len(target_isins)} ISINs selected")

            # Read exclude ISINs
            exclude_isins = self.collector._read_exclude_isins()

            # Collect NAV data - use the derived file_type
            nav_dfs = self.collector.collect_nav_data(
                date_str, target_isins, exclude_isins, derived_file_type)
            if not nav_dfs:
                raise ValueError("No NAV files could be processed")

            # Upload input CSV files to Google Drive if configured
            if self.config.drive_config and self.config.drive_config.input_folder_id:
                input_file_paths = self.collector.get_input_file_paths()
                if input_file_paths:
                    uploaded_count = self.distributor.upload_input_files_to_drive(
                        input_file_paths,
                        self.config.drive_config.input_folder_id
                    )
                    logger.info(
                        f"Uploaded {uploaded_count} input CSV files to Google Drive")

            # Update templates
            output_paths = []

            for template_type in template_types:
                try:
                    if template_type.lower() == 'morningstar':
                        output_path = self.template_manager.update_morningstar_template(
                            nav_dfs, date_str)
                        output_paths.append(output_path)
                    elif template_type.lower() == 'six':
                        # Get series information for SIX template
                        all_isins = set()
                        for _, df in nav_dfs:
                            all_isins.update(df['ISIN'].unique())
                        series_info = self.db_manager.get_series_by_isins(
                            all_isins)

                        output_path = self.template_manager.update_six_template(
                            nav_dfs, date_str, series_info)
                        output_paths.append(output_path)
                    else:
                        logger.warning(
                            f"Unknown template type: {template_type}")
                except Exception as e:
                    logger.error(
                        f"Error updating {template_type} template: {str(e)}")
                    raise

            # Handle email sending if requested
            if send_email and to_emails:
                if isinstance(to_emails, str):
                    to_emails = [to_emails]

                # Send separate emails for each template type
                for template_type in template_types:
                    # Find the corresponding output path for this template type
                    template_path = None
                    for path in output_paths:
                        if (template_type.lower() == 'six' and 'LAM_SFI_Price' in path.name) or \
                           (template_type.lower() == 'morningstar' and 'Flexfunds ETPs' in path.name):
                            template_path = path
                            break

                    if template_path:
                        self.distributor.send_email(
                            output_path=template_path,
                            to_emails=to_emails,
                            distribution_type=template_type.lower(),
                            nav_dfs=nav_dfs
                        )

            # Upload to Drive if configured
            if output_paths:
                self.distributor.upload_to_drive(output_paths, template_types)

            # Save to database
            self.db_manager.save_nav_data(nav_dfs, distribution_type)

            # Clean up
            self.cleanup()

        except Exception as e:
            logger.error(f"Error in NAV processing: {str(e)}")
            self.cleanup()
            raise

    def get_nav_history(self, isin: Optional[str] = None, start_date: Optional[datetime] = None,
                        end_date: Optional[datetime] = None, page: int = 1, per_page: int = 50,
                        series_number: Optional[str] = None):
        """
        Get NAV history for a specific ISIN or series number within a date range with pagination

        Args:
            isin: Optional ISIN to filter by
            start_date: Optional start date for filtering
            end_date: Optional end date for filtering
            page: Page number (1-based)
            per_page: Number of entries per page
            series_number: Optional series number to filter by

        Returns:
            Dict containing entries, total_pages, and total_entries
        """
        return self.db_manager.get_nav_history(
            isin=isin,
            start_date=start_date,
            end_date=end_date,
            page=page,
            per_page=per_page,
            series_number=series_number
        )

    def import_historic_data(self, excel_path: str):
        """
        Import historic NAV data from Excel file

        Args:
            excel_path: Path to the Excel file containing historic NAV data

        Returns:
            Tuple of (added_count, duplicates_count)
        """
        return self.db_manager.import_historic_data(excel_path)


def main():
    """Example usage of the NAV processor"""
    # Initialize processor with database support
    processor = NAVProcessor(
        mode="remote",
        ftp_configs=DEFAULT_FTP_CONFIGS,
        db_connection_string='sqlite:///nav_data.db'
    )

    # Process NAVs and save to database
    date_str = datetime.now().strftime('%m%d%Y')
    processor.process_navs(date_str)

    # Example: Query NAV history for a specific ISIN
    isin = "XS2728487260"  # Example ISIN from daily set
    start_date = datetime.now().replace(day=1)  # First day of current month
    nav_result = processor.get_nav_history(isin, start_date)

    print(f"\nNAV history for {isin} since {start_date.date()}:")
    print(
        f"Found {nav_result['total_entries']} entries across {nav_result['total_pages']} pages")

    for entry in nav_result['entries']:
        print(f"Date: {entry.nav_date}, Value: {entry.nav_value}")


if __name__ == "__main__":
    main()
