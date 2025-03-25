# NAV Report Processor

A sophisticated Python application designed to automate the processing of Net Asset Value (NAV) files for financial instruments. This tool streamlines the workflow of retrieving, processing, and distributing NAV reports, making it an essential utility for fund administrators and financial institutions.

## 🌟 Key Features

- **Multi-source File Processing**

  - Local directory file processing
  - Secure FTP server integration for remote file retrieval
  - Support for multiple NAV file formats (CSV)

- **Advanced Data Processing**

  - Automated NAV calculation and validation
  - Template-based report generation
  - Preservation of Excel template formatting
  - Batch processing capabilities

- **Distribution System**

  - Automated email distribution
  - Configurable recipient lists
  - Support for multiple email templates
  - Attachment handling with size validation

- **Security & Reliability**
  - Secure FTP communication
  - SSL/TLS email encryption
  - Comprehensive error handling
  - Detailed logging system

## 📁 Directory Structure

```bash
project_root/
├── email_sender.py        # Email distribution module
├── input/                 # Input directory for NAV files
│   ├── [NAV CSV files]   # NAV data files
│   └── template/         # Report templates
│       └── Morningstar Performance Template.xls
├── main.py               # Application controller
├── nav_processor.py      # NAV processing core logic
├── output/              # Processed reports directory
├── requirements.txt     # Project dependencies
├── test.ipynb          # Development testing notebook
└── test_ftp_connection.py # FTP connectivity testing
```

## 🚀 Getting Started

### Prerequisites

- Python 3.7 or higher
- Git (for cloning the repository)
- Access to SMTP server (for email functionality)
- FTP server credentials (if using remote file processing)

### Installation

1. Clone the repository:

   ```bash
   git clone [repository-url]
   cd nav-report-processor
   ```

2. Create and activate a virtual environment:

   ```bash
   # Windows
   python -m venv venv
   .\venv\Scripts\activate

   # Linux/MacOS
   python -m venv venv
   source venv/bin/activate
   ```

3. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## ⚙️ Configuration

### FTP Configuration

Create or modify the FTP configuration in `main.py`:

```python
ftp_config = {
    "host": "your_ftp_host",
    "user": "your_username",
    "password": "your_password",
    "directory": "your_directory"
}
```

### Email Configuration

Set up the SMTP configuration in `main.py`:

```python
smtp_config = {
    "host": "smtp.gmail.com",
    "port": 587,
    "user": "your_email@gmail.com",
    "password": "your_app_password",  # Gmail App Password
    "use_tls": True
}
```

> **Note**: For Gmail, you'll need to generate an App Password. See [Gmail App Passwords](https://support.google.com/accounts/answer/185833?hl=en)

## 📘 Usage Guide

### Basic Usage

1. Place your NAV files in the `input` directory
2. Run the processor:
   ```bash
   python main.py
   ```
3. Check the `output` directory for processed reports

### Processing Remote Files

The application can fetch files from an FTP server automatically:

```python
# Example configuration in main.py
USE_FTP = True
```

### Customizing Email Distribution

Modify the email settings in `email_sender.py`:

```python
# Example email configuration
email_settings = {
    "recipients": ["recipient1@example.com", "recipient2@example.com"],
    "subject": "NAV Report - {date}",
    "body": "Please find attached the latest NAV report."
}
```

## 📊 Sample Output

[Screenshot Placeholder: Add a screenshot of the generated NAV report Excel file, showing the formatted data and calculations]

## 🔍 Logging and Monitoring

The application maintains detailed logs of all operations:

- Processing status
- Error messages
- FTP connection details
- Email delivery status

Logs are stored in the application's root directory.

## 🛠 Troubleshooting

Common issues and solutions:

1. **FTP Connection Failed**

   - Verify FTP credentials
   - Check network connectivity
   - Ensure correct FTP directory path

2. **Email Sending Failed**

   - Verify SMTP settings
   - Check app password validity
   - Confirm recipient email addresses

3. **Template Processing Errors**
   - Ensure template file exists in input/template
   - Verify template format compatibility
   - Check file permissions

## 📋 Dependencies

Key dependencies include:

- pandas: Data processing and manipulation
- xlrd (v1.2.0): Excel file reading
- xlutils: Excel template manipulation
- pyOpenSSL: Secure communications

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🎯 Future Enhancements

- [ ] Web interface for report management
- [ ] Additional report template support
- [ ] Real-time processing notifications
- [ ] Advanced data validation rules
- [ ] API integration capabilities

## 📞 Support

For support and questions, please contact [support email/contact information]

---

[Screenshot Placeholder: Add a screenshot of the application's main interface or workflow diagram showing the process flow from input to output]
