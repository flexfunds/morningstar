from nav_processor import NAVProcessor
from google_drive_service import GoogleDriveService
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# FTP Configurations for all emitters - load from environment variables
ftp_configs = {
    "ETPCAP2": {
        "host": os.getenv('ETPCAP2_FTP_HOST'),
        "user": os.getenv('ETPCAP2_FTP_USER'),
        "password": os.getenv('ETPCAP2_FTP_PASSWORD'),
        "directory": "/NAVs_Consolidated"
    },
    "HFMX": {
        "host": os.getenv('HFMX_FTP_HOST'),
        "user": os.getenv('HFMX_FTP_USER'),
        "password": os.getenv('HFMX_FTP_PASSWORD'),
        "directory": "/NAVs_Consolidated"
    },
    "IACAP": {
        "host": os.getenv('IACAP_FTP_HOST'),
        "user": os.getenv('IACAP_FTP_USER'),
        "password": os.getenv('IACAP_FTP_PASSWORD'),
        "directory": "/NAVs_Consolidated"
    },
    "CIX": {
        "host": os.getenv('CIX_FTP_HOST'),
        "user": os.getenv('CIX_FTP_USER'),
        "password": os.getenv('CIX_FTP_PASSWORD'),
        "directory": "/NAVs_Consolidated"
    },
    "DCXPD": {
        "host": os.getenv('DCXPD_FTP_HOST'),
        "user": os.getenv('DCXPD_FTP_USER'),
        "password": os.getenv('DCXPD_FTP_PASSWORD'),
        "directory": "/NAVs_Consolidated"
    }
}

# SMTP Configuration from environment variables
smtp_config = {
    "host": os.getenv('SMTP_HOST', 'smtp.gmail.com'),
    "port": int(os.getenv('SMTP_PORT', '587')),
    "user": os.getenv('SMTP_USER'),
    "password": os.getenv('SMTP_PASSWORD'),
    "use_tls": os.getenv('SMTP_USE_TLS', 'True').lower() == 'true'
}

# Drive configuration from environment variables
drive_config = {
    "credentials_path": os.getenv('GOOGLE_DRIVE_CREDENTIALS_PATH'),
    "output_folder_id": os.getenv('DRIVE_OUTPUT_FOLDER_ID'),
    "input_folder_id": os.getenv('DRIVE_INPUT_FOLDER_ID')
}

# Initialize processor with drive service
processor = NAVProcessor(
    mode="remote",
    ftp_configs=ftp_configs,
    smtp_config=smtp_config,
    drive_config=drive_config
)

# Process NAVs and send email
processor.process_navs(
    date_str="01242025",
    send_email=True,
    to_emails=[os.getenv('REPORT_EMAIL_RECIPIENT')]
)
