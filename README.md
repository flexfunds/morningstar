# NAV Report Processor

A Python application for processing NAV (Net Asset Value) files and generating reports. The application supports both local and remote (FTP) file processing, with capabilities to email the generated reports.

## Features

- Process NAV files from local directory or FTP server
- Preserve Excel template formatting
- Email report distribution
- Logging and error handling

## Directory Structure

```bash
project_root/
├── email_sender.py ## Logic for sending emails using Gmail
├── input
│   ├── CAS_Flexfunds_NAV_12202024 ETPCAP2.csv
│   ├── CAS_Flexfunds_NAV_12202024 HFMX.csv
│   ├── CAS_Flexfunds_NAV_12202024 IACAP.csv
│   └── template
│       └── Morningstar Performance Template.xls
├── main.py ## Controller for nav_processor.py and email_sender.py
├── nav_processor.py ## Main logic for processing the price distribution
├── output
│   └── Morningstar_Performance_Template_12202024.xls
├── README.md
├── requirements.txt
├── test.ipynb ## File to handle and test implementations for data manipulation
└── test_ftp_connection.py ## Tests connection with FTP server
```

## Installation

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Configure FTP and SMTP settings in `main.py`:

```python
ftp_config = {
"host": "your_ftp_host",
"user": "your_username",
"password": "your_password",
"directory": "your_directory"
}
smtp_config = {
"host": "smtp.gmail.com",
"port": 587,
"user": "your_email@gmail.com",
"password": "your_app_password",
"use_tls": True
}
```

## Usage

Run the processor:

```bash
python main.py
```

## Dependencies

- pandas
- xlrd==1.2.0
- xlutils
- pyOpenSSL
