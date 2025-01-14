from nav_processor import NAVProcessor

# FTP Configuration
ftp_config = {
    "host": "127.0.0.1",
    "user": "nav_auto",
    "password": "hola",
    "directory": "/1"
}

# SMTP Configuration
smtp_config = {
    "host": "smtp.gmail.com",  # Example for Gmail
    "port": 587,
    "user": "sebastian.masia@flexfundsetp.com",
    "password": "fxdc himz rbzc ztvd",  # Use app-specific password for Gmail
    "use_tls": True
}

# Initialize processor with email capability
processor = NAVProcessor(
    mode="local",
    ftp_config=ftp_config,
    smtp_config=smtp_config
)

# Process NAVs and send email
processor.process_navs(
    date_str="12202024",
    send_email=True,
    to_emails=["sebastian.masia@flexfundsetp.com"]
)
