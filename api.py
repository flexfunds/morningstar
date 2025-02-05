from flask import Flask, request, jsonify
from nav_processor import NAVProcessor
import os
from dotenv import load_dotenv
from datetime import datetime
from functools import wraps
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

app = Flask(__name__)

# Configure timeouts
app.config['TIMEOUT'] = 300  # 5 minutes timeout

# Load environment variables
load_dotenv()

# Initialize configurations
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
    },
}

smtp_config = {
    "host": os.getenv('SMTP_HOST', 'smtp.gmail.com'),
    "port": int(os.getenv('SMTP_PORT', '587')),
    "user": os.getenv('SMTP_USER'),
    "password": os.getenv('SMTP_PASSWORD'),
    "use_tls": os.getenv('SMTP_USE_TLS', 'True').lower() == 'true'
}

drive_config = {
    "credentials_path": os.getenv('GOOGLE_DRIVE_CREDENTIALS_PATH'),
    "output_folder_id": os.getenv('DRIVE_OUTPUT_FOLDER_ID'),
    "input_folder_id": os.getenv('DRIVE_INPUT_FOLDER_ID')
}

# Initialize rate limiter
limiter = Limiter(
    key_func=get_remote_address,
    app=app,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://"
)


def require_api_key(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get('X-API-Key')
        if api_key and api_key == os.getenv('API_KEY'):
            return f(*args, **kwargs)
        return jsonify({'message': 'Invalid API key'}), 401
    return decorated_function


@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'}), 200


@app.route('/process-navs', methods=['POST'])
@limiter.limit("10 per minute")
@require_api_key
def process_navs():
    try:
        # Get parameters from request
        data = request.get_json()
        date_str = data.get('date_str')
        # Handle both single email and list of emails
        emails = data.get('emails', [])
        if isinstance(emails, str):
            emails = [emails]  # Convert single email to list
        elif not isinstance(emails, list):
            return jsonify({
                'status': 'error',
                'message': 'emails must be a string or list of strings'
            }), 400

        # Validate date_str format
        if not date_str:
            date_str = datetime.now().strftime('%m%d%Y')

        # Initialize processor
        processor = NAVProcessor(
            mode="remote",
            ftp_configs=ftp_configs,
            smtp_config=smtp_config,
            drive_config=drive_config
        )

        # Process NAVs
        # Temporary debug print
        print(f"Debug - Sending request with emails: {emails}")
        result = processor.process_navs(
            date_str=date_str,
            send_email=True if emails else False,
            to_emails=emails if emails else None
        )
        
        response = {
            'status': 'success',
            'message': 'NAV processing completed successfully',
            'date_processed': date_str,
            'emails_sent_to': emails if emails else []
        }
        
        # Clean up resources
        processor.cleanup()
        
        return jsonify(response), 200

    except Exception as e:
        # Clean up resources in case of error
        if 'processor' in locals():
            processor.cleanup()
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8080)))
