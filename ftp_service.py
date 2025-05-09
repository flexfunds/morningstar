import logging
from ftplib import FTP_TLS
import ssl
from pathlib import Path
import pandas as pd
from typing import Optional, Dict

logger = logging.getLogger(__name__)


class FTPService:
    def __init__(self, config: Dict[str, Dict]):
        """
        Initialize FTP service with configurations for multiple emitters

        Args:
            config: Dictionary of FTP configurations for each emitter
        """
        self.config = config

    def _create_ftp_context(self) -> ssl.SSLContext:
        """Create SSL context for FTP connection"""
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        context.options |= ssl.OP_NO_TICKET
        return context

    def download_file(self, emitter: str, filename: str, temp_file: Path) -> Optional[pd.DataFrame]:
        """
        Download and read a CSV file from FTP server

        Args:
            emitter: The emitter identifier
            filename: Name of the file to download
            temp_file: Path to temporary file for download

        Returns:
            Optional[pd.DataFrame]: DataFrame containing file contents or None if file not found
        """
        ftp_config = self.config.get(emitter)
        if not ftp_config:
            raise ValueError(
                f"No FTP configuration found for emitter {emitter}")

        context = self._create_ftp_context()

        with FTP_TLS(context=context) as ftp:
            ftp.encoding = 'utf-8'
            try:
                # Connect and authenticate
                ftp.connect(host=ftp_config.host, port=21)
                ftp.auth()
                ftp.login(ftp_config.user, ftp_config.password)

                # Enable TLS for data channel
                ftp.prot_p()
                ftp.set_pasv(True)

                # Change to directory if specified
                if hasattr(ftp_config, 'directory') and ftp_config.directory:
                    ftp.cwd(ftp_config.directory)

                # Download the file
                with open(temp_file, 'wb') as f:
                    ftp.retrbinary(f'RETR {filename}', f.write)

                # Read the CSV file
                try:
                    return pd.read_csv(temp_file)
                except UnicodeDecodeError:
                    return pd.read_csv(temp_file, encoding='latin-1')

            except Exception as e:
                if "550" in str(e):  # File not found
                    return None
                logger.error(
                    f"Error downloading {filename} from {emitter}: {str(e)}")
                raise

    def cleanup_emitter_directory(self, emitter: str, directory: Path):
        """
        Clean up old files from emitter directory

        Args:
            emitter: The emitter identifier
            directory: Path to emitter directory
        """
        emitter_dir = directory / emitter
        if emitter_dir.exists():
            for file in emitter_dir.glob('*.csv'):
                file.unlink()
            logger.info(f"Cleaned up old files from {emitter} directory")
