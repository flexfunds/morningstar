import pandas as pd
from ftplib import FTP, FTP_TLS
from io import StringIO
import os
from typing import List, Dict
import logging
from pathlib import Path
import xlrd
import xlutils.copy
import ssl
from email_sender import EmailSender
from google_drive_service import GoogleDriveService
import shutil
import tempfile
from contextlib import contextmanager


class NAVProcessor:
    def __init__(self, mode: str = "local", ftp_configs: Dict[str, Dict] = None,
                 smtp_config: Dict = None, drive_config: Dict = None):
        """
        Initialize the NAV processor

        Args:
            mode (str): Operation mode - either "local" or "remote"
            ftp_configs (Dict[str, Dict]): Dictionary of FTP configurations for each emitter containing:
                {
                    "ETPCAP2": {
                        "host": FTP server hostname,
                        "user": FTP username,
                        "password": FTP password,
                        "directory": Remote directory path
                    },
                    "HFMX": { ... },
                    ...
                }
            smtp_config (Dict): SMTP configuration for email sending
            drive_config (Dict): Google Drive configuration for file syncing
        """
        self.mode = mode.lower()
        self.ftp_configs = ftp_configs or {}
        self.smtp_config = smtp_config
        self.email_sender = EmailSender(smtp_config) if smtp_config else None
        self.drive_service = GoogleDriveService(
            drive_config['credentials_path']) if drive_config else None
        self.drive_config = drive_config

        # Configure logging
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger('ftplib').setLevel(logging.DEBUG)
        self.logger = logging.getLogger(__name__)

        # Define file structure with existing paths
        self.input_dir = Path("input")
        self.output_dir = Path("output")
        self.template_dir = self.input_dir / "template"
        self.temp_dir = Path(tempfile.gettempdir()) / "nav_processor"

        # Create directories if they don't exist
        if self.mode == "local":
            self._create_directories()

    def _create_directories(self):
        """Create necessary directories for local mode"""
        for directory in [self.input_dir, self.output_dir, self.temp_dir]:
            directory.mkdir(parents=True, exist_ok=True)

        # Create emitter subdirectories
        for emitter in self.ftp_configs.keys():
            (self.input_dir / emitter).mkdir(exist_ok=True)

    @contextmanager
    def _temp_file_handler(self, filename: str):
        """Context manager for handling temporary files"""
        # Create temp directory if it doesn't exist
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        temp_path = self.temp_dir / filename
        try:
            yield temp_path
        finally:
            if temp_path.exists():
                temp_path.unlink()

    def _read_csv_local(self, filename: str) -> pd.DataFrame:
        """Read CSV file from local directory"""
        file_path = self.input_dir / filename
        return pd.read_csv(file_path)

    def _cleanup_emitter_directory(self, emitter: str):
        """Clean up old files from emitter directory"""
        emitter_dir = self.input_dir / emitter
        if emitter_dir.exists():
            for file in emitter_dir.glob('*.csv'):
                file.unlink()
            self.logger.info(f"Cleaned up old files from {emitter} directory")

    def _read_csv_remote(self, filename: str, emitter: str) -> pd.DataFrame:
        """Read CSV file from FTP server and optionally upload to Drive"""
        with self._temp_file_handler(filename) as temp_file_path:
            ftp_config = self.ftp_configs.get(emitter)
            if not ftp_config:
                raise ValueError(
                    f"No FTP configuration found for emitter {emitter}")

            # Create FTP_TLS instance with custom context
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            context.options |= ssl.OP_NO_TICKET

            with FTP_TLS(context=context) as ftp:
                ftp.encoding = 'utf-8'

                try:
                    # Connect and authenticate
                    ftp.connect(host=ftp_config['host'], port=21)
                    ftp.auth()
                    ftp.login(ftp_config['user'], ftp_config['password'])

                    # Enable TLS for data channel
                    ftp.prot_p()
                    ftp.set_pasv(True)

                    # Change to directory if specified
                    if ftp_config.get('directory'):
                        ftp.cwd(ftp_config['directory'])

                    # Download the file
                    with open(temp_file_path, 'wb') as temp_file:
                        ftp.retrbinary(f'RETR {filename}', temp_file.write)

                    # Clean up old files and save to emitter directory
                    emitter_dir = self.input_dir / emitter
                    emitter_dir.mkdir(exist_ok=True)
                    self._cleanup_emitter_directory(
                        emitter)  # Clean up before saving
                    input_path = emitter_dir / filename
                    shutil.copy2(temp_file_path, input_path)

                    # Upload to Google Drive if configured
                    if self.drive_service and self.drive_config.get('input_folder_id'):
                        try:
                            self.drive_service.upload_file(
                                temp_file_path,
                                self.drive_config['input_folder_id']
                            )
                            self.logger.info(f"Successfully uploaded {
                                             filename} to Google Drive")
                        except Exception as e:
                            self.logger.error(f"Failed to upload {
                                              filename} to Google Drive: {str(e)}")

                    # Read the CSV file
                    try:
                        return pd.read_csv(temp_file_path)
                    except UnicodeDecodeError:
                        return pd.read_csv(temp_file_path, encoding='latin-1')

                except Exception as e:
                    self.logger.error(f"FTP Error for {emitter}: {str(e)}")
                    raise

    def cleanup(self):
        """Clean up temporary files"""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
            self.temp_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info("Temporary files cleaned up")

    def _cleanup_output_directory(self):
        """Clean up old files from output directory"""
        if self.output_dir.exists():
            for file in self.output_dir.glob('*.xls'):
                file.unlink()
            self.logger.info("Cleaned up old files from output directory")

    def process_navs(self, date_str: str, send_email: bool = False, to_emails: List[str] = None):
        """Process NAV files and update the template"""
        try:
            # Clean up output directory before processing
            self._cleanup_output_directory()

            # Read the exclude ISINs file
            exclude_isins_path = self.template_dir / "Exclude ISINs.csv"
            exclude_isins = set()
            if exclude_isins_path.exists():
                exclude_isins = set(pd.read_csv(
                    exclude_isins_path, header=None)[0].str.strip())
                self.logger.info(
                    f"Loaded {len(exclude_isins)} ISINs to exclude")

            # Read template workbook locally regardless of mode
            wb = xlrd.open_workbook(
                self.template_dir / "Morningstar Performance Template.xls", formatting_info=True)
            template_sheet = wb.sheet_by_name('NAVs')
            wb_output = xlutils.copy.copy(wb)
            sheet_output = wb_output.get_sheet('NAVs')

            # Define input files with their corresponding emitters
            input_files = [
                ('ETPCAP2', f'CAS_Flexfunds_NAV_{date_str} ETPCAP2.csv'),
                ('HFMX', f'CAS_Flexfunds_NAV_{date_str} HFMX.csv'),
                ('IACAP', f'CAS_Flexfunds_NAV_{date_str} IACAP.csv'),
                ('CIX', f'CAS_Flexfunds_NAV_{date_str} CIX.csv'),
                ('DCXPD', f'CAS_Flexfunds_NAV_{date_str} DCXPD.csv'),
                ('ETPCAP2', f'CAS_Flexfunds_NAV_{
                 date_str} Wrappers Hybrid ETPCAP2.csv'),
                ('HFMX', f'CAS_Flexfunds_NAV_{
                 date_str} Wrappers Hybrid HFMX.csv'),
                ('IACAP', f'CAS_Flexfunds_NAV_{
                 date_str} Wrappers Hybrid IACAP.csv'),
                ('CIX', f'CAS_Flexfunds_NAV_{
                 date_str} Wrappers Hybrid CIX.csv'),
                ('DCXPD', f'CAS_Flexfunds_NAV_{
                 date_str} Wrappers Hybrid DCXPD.csv'),
                ('ETPCAP2', f'CAS_Flexfunds_NAV_{
                 date_str} Loan ETPCAP2.csv'),
                ('HFMX', f'CAS_Flexfunds_NAV_{date_str} Loan HFMX.csv'),
                ('IACAP', f'CAS_Flexfunds_NAV_{date_str} Loan IACAP.csv'),
                ('CIX', f'CAS_Flexfunds_NAV_{date_str} Loan CIX.csv'),
                ('DCXPD', f'CAS_Flexfunds_NAV_{date_str} Loan DCXPD.csv')
            ]

            # Read and process NAV files
            nav_dfs = []
            missing_files = []
            for emitter, file in input_files:
                try:
                    if self.mode == "local":
                        if not (self.input_dir / emitter / file).exists():
                            missing_files.append(file)
                            continue
                        df = self._read_csv_local(file)
                    else:
                        try:
                            df = self._read_csv_remote(file, emitter)
                        except Exception as e:
                            if "550" in str(e):  # FTP error code for file not found
                                missing_files.append(file)
                                continue
                            raise

                    # Filter out excluded ISINs
                    if not df.empty:
                        original_count = len(df)
                        df = df[~df['ISIN'].isin(exclude_isins)]
                        filtered_count = len(df)
                        if original_count != filtered_count:
                            self.logger.info(
                                f"Filtered out {original_count - filtered_count} excluded ISINs from {file}")

                    nav_dfs.append(
                        df.dropna(how='all', axis=0).dropna(how='all', axis=1))
                except Exception as e:
                    self.logger.error(
                        f"Error processing file {file} from {emitter}: {str(e)}")
                    raise

            if not nav_dfs:
                raise ValueError(
                    f"No NAV files could be processed. Missing files: {missing_files}")

            if missing_files:
                self.logger.warning(
                    f"Some files were not found: {missing_files}")

            # Combine NAV dataframes
            nav_df = pd.concat(nav_dfs, ignore_index=True)

            # Get column indices from template (row 7 contains headers)
            header_row = 7
            col_indices = {}
            for col_idx in range(template_sheet.ncols):
                header_value = template_sheet.cell_value(header_row, col_idx)
                col_indices[header_value] = col_idx

            # Update template with NAV data
            mapping = {
                'Unique Identifier': 'ISIN',
                'NAV/Daily dividend Date': 'Valuation Period-End Date',
                'NAV': 'NAV'
            }

            # Update the data starting from row 8
            for i, row in nav_df.iterrows():
                row_idx = i + 8  # Start from row 8
                for template_col, nav_col in mapping.items():
                    if template_col in col_indices:
                        col_idx = col_indices[template_col]
                        value = row[nav_col]
                        sheet_output.write(row_idx, col_idx, value)

            # Save updated template
            date_obj = pd.to_datetime(date_str, format='%m%d%Y')
            formatted_date = date_obj.strftime('%m.%d.%Y')
            output_path = self.output_dir / \
                f'Flexfunds ETPs - NAVs {formatted_date}.xls'
            wb_output.save(str(output_path))

            self.logger.info(
                f"Successfully processed NAV files and saved output to {output_path}")

            # After saving the output file
            self.logger.info(
                f"Email settings - send_email: {send_email}, to_emails: {to_emails}")
            if send_email and self.email_sender:
                self.logger.info("Starting email sending process...")
                subject = f"FlexFunds Calculation Agent ETPs - Morningstar NAV Update"
                body = f"""Hi team,\n\nI hope you are well.\n\nPlease find attached the updated NAV for the pricing distribution process of the different notes for which we function as the calculation agent.\n\nMany thanks,"""

                # Ensure to_emails is a list
                if isinstance(to_emails, str):
                    to_emails = [to_emails]
                    self.logger.info(
                        f"Converted single email to list: {to_emails}")
                elif not to_emails:
                    self.logger.error("No email recipients provided")
                    return False
                else:
                    self.logger.info(f"Using email list: {to_emails}")

                self.logger.info("Attempting to send email...")
                email_sent = self.email_sender.send_report(
                    to_emails=to_emails,
                    subject=subject,
                    body=body,
                    attachment_path=output_path
                )

                if email_sent:
                    self.logger.info("NAV report sent via email successfully")
                else:
                    self.logger.error("Failed to send NAV report via email")
                    return False
            else:
                self.logger.warning(
                    "Email sending skipped - send_email is False or email_sender not configured")

            # Upload output to Drive if configured
            if self.drive_service and self.drive_config.get('output_folder_id'):
                self.drive_service.upload_file(
                    output_path,
                    self.drive_config['output_folder_id']
                )

            # Clean up temporary files after processing
            self.cleanup()

        except Exception as e:
            self.logger.error(f"Error in NAV processing: {str(e)}")
            self.cleanup()  # Clean up even if there's an error
            raise


def main():
    # Example usage for remote mode with multiple FTP configs
    ftp_configs = {
        "ETPCAP2": {
            "host": "127.0.0.1",
            "user": "nav_auto",
            "password": "hola",
            "directory": "/1"
        },
        "HFMX": {
            "host": "teo.superhosting.bg",
            "user": "data@hfmxdacseries.com",
            "password": "BF0*5bZIRZK^",
            "directory": "/"
        },
        "IACAP": {
            "host": "omar.superhosting.bg",
            "user": "data@iacapitalplc.com",
            "password": "BF0*5bZIRZK^",
            "directory": "/"
        },
        "CIX": {
            "host": "mini.superhosting.bg",
            "user": "data@cixdac.com",
            "password": "BF0*5bZIRZK^",
            "directory": "/"
        },
        "DCX": {
            "host": "mini.superhosting.bg",
            "user": "data@dcxpd.com",
            "password": "9RF#c[tCq}rT",
            "directory": "/"
        }
    }

    processor_remote = NAVProcessor(mode="remote", ftp_configs=ftp_configs)
    processor_remote.process_navs("12202024")


if __name__ == "__main__":
    main()
