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


class NAVProcessor:
    def __init__(self, mode: str = "local", ftp_config: Dict = None, smtp_config: Dict = None):
        """
        Initialize the NAV processor

        Args:
            mode (str): Operation mode - either "local" or "remote"
            ftp_config (Dict): FTP configuration for remote mode containing:
                - host: FTP server hostname
                - user: FTP username
                - password: FTP password
                - directory: Remote directory path
            smtp_config (Dict): SMTP configuration for email sending
        """
        self.mode = mode.lower()
        self.ftp_config = ftp_config
        self.smtp_config = smtp_config
        self.email_sender = EmailSender(smtp_config) if smtp_config else None

        # Configure logging
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger('ftplib').setLevel(logging.DEBUG)
        self.logger = logging.getLogger(__name__)

        # Define file structure
        self.input_dir = Path("input")
        self.output_dir = Path("output")
        self.template_path = self.input_dir / "template" / \
            "Morningstar Performance Template.xls"

        # Create directories if they don't exist
        if self.mode == "local":
            self._create_directories()

    def _create_directories(self):
        """Create necessary directories for local mode"""
        for directory in [self.input_dir, self.output_dir, self.input_dir / "template"]:
            directory.mkdir(parents=True, exist_ok=True)

    def _read_csv_local(self, filename: str) -> pd.DataFrame:
        """Read CSV file from local directory"""
        file_path = self.input_dir / filename
        return pd.read_csv(file_path)

    def _read_csv_remote(self, filename: str) -> pd.DataFrame:
        """Read CSV file from FTP server"""
        data = StringIO()

        # Create FTP_TLS instance with custom context
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        # Enable session reuse
        context.options |= ssl.OP_NO_TICKET

        ftp = FTP_TLS(context=context)
        ftp.encoding = 'utf-8'

        try:
            # Connect and authenticate
            ftp.connect(host=self.ftp_config['host'], port=21)
            ftp.auth()  # Explicitly authenticate TLS
            ftp.login(self.ftp_config['user'], self.ftp_config['password'])

            # Enable TLS for data channel
            ftp.prot_p()
            ftp.set_pasv(True)

            # Change to directory
            ftp.cwd(self.ftp_config['directory'])

            # Get file data using a single connection
            def handle_binary(bin_data):
                try:
                    data.write(bin_data.decode('utf-8'))
                except UnicodeDecodeError:
                    # Handle potential binary data
                    data.write(bin_data.decode('latin-1'))

            # Use lower level FTP command with explicit buffer size
            ftp.retrbinary(f'RETR {filename}', handle_binary, blocksize=32768)

            # Process data
            data.seek(0)
            return pd.read_csv(data)
        except Exception as e:
            self.logger.error(f"FTP Error: {str(e)}")
            raise
        finally:
            try:
                ftp.quit()
            except:
                ftp.close()

    def process_navs(self, date_str: str, send_email: bool = False, to_emails: List[str] = None):
        """
        Process NAV files and update the template

        Args:
            date_str (str): Date string in the format 'MMDDYYYY' for file naming
            send_email (bool): Whether to send the output file via email
            to_emails (List[str]): List of email recipients
        """
        try:
            # Read template workbook locally regardless of mode
            wb = xlrd.open_workbook(self.template_path, formatting_info=True)
            template_sheet = wb.sheet_by_name('NAVs')
            wb_output = xlutils.copy.copy(wb)
            sheet_output = wb_output.get_sheet('NAVs')

            # Define input files
            input_files = [
                f'CAS_Flexfunds_NAV_{date_str} ETPCAP2.csv',
                f'CAS_Flexfunds_NAV_{date_str} HFMX.csv',
                f'CAS_Flexfunds_NAV_{date_str} IACAP.csv'
            ]

            # Read and process NAV files
            nav_dfs = []
            for file in input_files:
                try:
                    if self.mode == "local":
                        df = self._read_csv_local(file)
                    else:
                        df = self._read_csv_remote(file)

                    nav_dfs.append(
                        df.dropna(how='all', axis=0).dropna(how='all', axis=1))
                except Exception as e:
                    self.logger.error(
                        f"Error processing file {file}: {str(e)}")
                    raise

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
            output_path = self.output_dir / \
                f'Morningstar_Performance_Template_{date_str}.xls'
            wb_output.save(str(output_path))

            self.logger.info(
                f"Successfully processed NAV files and saved output to {output_path}")

            # After saving the output file
            if send_email and self.email_sender and to_emails:
                subject = f"NAV Report - {date_str}"
                body = f"Please find attached the NAV report for {date_str}."

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

        except Exception as e:
            self.logger.error(f"Error in NAV processing: {str(e)}")
            raise


def main():
    # # Example usage for local mode
    # processor_local = NAVProcessor(mode="local")
    # processor_local.process_navs("12202024")

    # Example usage for remote mode
    ftp_config = {
        "host": "127.0.0.1",
        "user": "nav_auto",
        "password": "hola",
        "directory": "/1"
    }
    processor_remote = NAVProcessor(mode="remote", ftp_config=ftp_config)
    processor_remote.process_navs("12202024")


if __name__ == "__main__":
    main()
