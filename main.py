from nav_processor import NAVProcessor
from google_drive_service import GoogleDriveService
import os
from dotenv import load_dotenv
from db_service import DatabaseService

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
    "morningstar_output_folder_id": os.getenv('MORNINGSTAR_OUTPUT_FOLDER_ID'),
    "six_output_folder_id": os.getenv('SIX_OUTPUT_FOLDER_ID'),
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
# Example 1: Process all ISINs (original behavior)
processor.process_navs(
    date_str="03212025",
    send_email=True,
    to_emails=["productoperations@flexfunds.com", "nav@morningstareurope.com"],
    # to_emails=["sebastian.masia@flexfundsetp.com"],
    distribution_type="morningstar",
    isin_filter=["daily", "weekly"],
    template_types=["morningstar"]
)

processor.process_navs(
    date_str="03212025",
    send_email=True,
    to_emails=["data-qc-usa@six-financial-information.com",
               "dc.us@telekurs.com",
               "Darren.Smith@six-group.com",
               "matthew.leventhal@six-group.com",
               "dpsfixedincome.us@six-group.com",
               "notification.us@six-financial-information.com",
               "usdata.us@six-group.com",
               "notification.eurobonds@six-financial-information.com",
               "productoperations@flexfunds.com",
               ],
    # to_emails=["sebastian.masia@flexfundsetp.com"],
    distribution_type="six",
    isin_filter=["daily", "weekly"],
    template_types=["six"]
)
# Example 2: Process only daily ISINs
# processor.process_navs(
#     date_str="02072025",
#     distribution="six"  # This will use the predefined daily set
# )

# processor.import_historic_data(
#     excel_path="C:/Users/a/Documents/FlexFunds/morningstar/input/template/NAVs Historical Prices 02.07.2025.xlsx"
# )

# processor.process_navs(
#     date_str="02122025",
#     send_email=True,
#     to_emails=[os.getenv('REPORT_EMAIL_RECIPIENT')],
#     isin_filter='daily'
# )

# Example 3: Process specific ISINs
# processor.process_navs(
#     date_str="01312025",
#     send_email=True,
#     to_emails=[os.getenv('REPORT_EMAIL_RECIPIENT')],
#     isin_filter=["XS9292383", "XS2292922020"]  # List of specific ISINs
# )
