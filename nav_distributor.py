import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import pandas as pd
from email_sender import EmailSender
from google_drive_service import GoogleDriveService
from config import AppConfig

logger = logging.getLogger(__name__)


class NAVDistributor:
    """Handles distribution of processed NAV reports"""

    def __init__(self, config: AppConfig):
        """
        Initialize the NAV distributor

        Args:
            config: Application configuration
        """
        self.config = config
        self.email_sender = EmailSender(
            config.smtp_config) if config.smtp_config else None
        self.drive_service = GoogleDriveService(
            config.drive_config.credentials_path) if config.drive_config else None
        self.drive_config = config.drive_config
        self.output_dir = Path(config.output_dir)

    def _get_email_template(self, distribution_type: str,
                            nav_dfs: List[Tuple[str, pd.DataFrame]]) -> Tuple[str, str]:
        """
        Get email template based on distribution type

        Args:
            distribution_type: Type of distribution ('six' or 'morningstar')
            nav_dfs: List of (emitter, dataframe) tuples

        Returns:
            Tuple of (subject, body)
        """
        if distribution_type.lower() == 'six':
            # Count series by emitter
            emitter_isins = {}  # Dictionary to store sets of ISINs per emitter
            for emitter, df in nav_dfs:
                if emitter not in emitter_isins:
                    emitter_isins[emitter] = set()
                # Add ISINs to the set for this emitter
                emitter_isins[emitter].update(df['ISIN'].unique())

            # Build the body with emitter counts
            body_lines = [
                "Hi team,",
                "",  # Empty line
                "I hope you are well.",
                "",  # Empty line
                "Attached please find the pricing distribution information for the issuers we function as the calculation agent.",
                ""  # Empty line before counts
            ]

            # Add emitter counts in specific order
            emitters_order = ['IACAP', 'ETPCAP2', 'HFMX', 'CIX', 'DCXPD']
            total_series = 0

            for emitter in emitters_order:
                count = len(emitter_isins.get(emitter, set()))
                total_series += count
                body_lines.append(f"{emitter}: {count}")

            body_lines.extend([
                "",  # Empty line
                f"Total series: {total_series}",
                "",  # Empty line
                "Many thanks,"
            ])

            return ("Pricing distribution - IA Capital, ETPCAP2, HFMX, CIX, and DCXPD",
                    "\n".join(body_lines))
        else:
            # Morningstar template
            return ("FlexFunds Calculation Agent ETPs - Morningstar NAV Update",
                    """Hi team,

I hope you are well.

Please find attached the updated NAV for the pricing distribution process of the different notes for which we function as the calculation agent.

Many thanks,""")

    def send_email(self, output_path: Path, to_emails: List[str],
                   distribution_type: str, nav_dfs: List[Tuple[str, pd.DataFrame]]) -> bool:
        """
        Send email with NAV data

        Args:
            output_path: Path to the file to attach
            to_emails: List of email recipients
            distribution_type: Type of distribution ('six' or 'morningstar')
            nav_dfs: List of (emitter, dataframe) tuples

        Returns:
            True if email was sent successfully, False otherwise
        """
        if not self.email_sender:
            logger.warning("Email sender not configured")
            return False

        # Get email template based on distribution type
        subject, body = self._get_email_template(distribution_type, nav_dfs)

        try:
            email_sent = self.email_sender.send_report(
                to_emails=to_emails,
                subject=subject,
                body=body,
                attachment_path=output_path
            )

            if email_sent:
                logger.info(
                    f"NAV report for {distribution_type} sent via email successfully")
            else:
                logger.error(
                    f"Failed to send NAV report for {distribution_type} via email")

            return email_sent

        except Exception as e:
            logger.error(f"Error sending email: {str(e)}")
            return False

    def upload_input_files_to_drive(self, input_file_paths: List[Path], folder_id: str) -> int:
        """
        Upload input CSV files to Google Drive

        Args:
            input_file_paths: List of paths to input CSV files
            folder_id: Google Drive folder ID to upload to

        Returns:
            Number of files uploaded
        """
        if not self.drive_service:
            logger.warning("Google Drive service not configured")
            return 0

        uploads_count = 0

        for file_path in input_file_paths:
            try:
                self.drive_service.upload_file(file_path, folder_id)
                uploads_count += 1
                # Don't log every file to avoid verbose output
            except Exception as e:
                logger.error(
                    f"Error uploading input file {file_path.name} to Google Drive: {str(e)}")

        if uploads_count > 0:
            logger.info(
                f"Uploaded {uploads_count} input CSV files to Google Drive")

        return uploads_count

    def upload_to_drive(self, output_paths: List[Path], template_types: List[str]) -> int:
        """
        Upload files to Google Drive

        Args:
            output_paths: List of paths to files to upload
            template_types: List of template types

        Returns:
            Number of files uploaded
        """
        if not self.drive_service or not self.drive_config:
            logger.warning("Google Drive service not configured")
            return 0

        uploads_count = 0

        for output_path in output_paths:
            # Upload to appropriate folder based on template type
            for template_type in template_types:
                if template_type.lower() == 'morningstar' and self.drive_config.morningstar_output_folder_id:
                    if 'Flexfunds ETPs - NAVs' in output_path.name:
                        try:
                            self.drive_service.upload_file(
                                output_path,
                                self.drive_config.morningstar_output_folder_id
                            )
                            uploads_count += 1
                            logger.debug(
                                f"Uploaded {output_path.name} to Morningstar folder")
                        except Exception as e:
                            logger.error(
                                f"Error uploading {output_path.name} to Google Drive: {str(e)}")

                elif template_type.lower() == 'six' and self.drive_config.six_output_folder_id:
                    if 'LAM_SFI_Price' in output_path.name:
                        try:
                            self.drive_service.upload_file(
                                output_path,
                                self.drive_config.six_output_folder_id
                            )
                            uploads_count += 1
                            logger.debug(
                                f"Uploaded {output_path.name} to SIX folder")
                        except Exception as e:
                            logger.error(
                                f"Error uploading {output_path.name} to Google Drive: {str(e)}")

        # Log overall upload results
        if uploads_count > 0:
            logger.info(
                f"Uploaded {uploads_count} template(s) to Google Drive")

        return uploads_count
