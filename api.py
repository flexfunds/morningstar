from flask import Flask, request, jsonify
from nav_processor import NAVProcessor
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from functools import wraps
from models import Series, SeriesStatus, FeeStructure, Trade
from series_change_detector import SeriesChangeDetector
import pandas as pd
import math
import traceback
from sqlalchemy import func, create_engine
from sqlalchemy.orm import sessionmaker
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Set, Optional
from config import AppConfig, DEFAULT_FTP_CONFIGS

app = Flask(__name__)

# Configure timeouts
app.config['TIMEOUT'] = 300  # 5 minutes timeout

# Load environment variables
load_dotenv()


def get_reliable_session():
    """
    Create a database session using multiple fallback methods to ensure reliability.
    Returns a session object that must be closed by the caller.
    """
    session = None
    errors = []

    # Method 1: Using db_service.SessionMaker (correct way)
    try:
        session = processor.db_manager.db_service.SessionMaker()
        return session
    except (AttributeError, TypeError) as e:
        errors.append(f"Method 1 failed: {str(e)}")

    # Method 2: Using db_service as direct session provider
    try:
        session = processor.db_manager.db_service.get_session()
        return session
    except (AttributeError, TypeError) as e:
        errors.append(f"Method 2 failed: {str(e)}")

    # Final fallback - create a new SQLAlchemy session from scratch
    try:
        engine = create_engine('sqlite:///nav_data.db')
        Session = sessionmaker(bind=engine)
        session = Session()
        print(
            f"Created fallback session. Previous errors: {', '.join(errors)}")
        return session
    except Exception as e:
        errors.append(f"Fallback method failed: {str(e)}")
        raise Exception(
            f"Could not create database session. Errors: {', '.join(errors)}")


def get_previous_business_day():
    """Get the previous business day date string in MMDDYYYY format"""
    today = pd.Timestamp.now()
    prev_business_day = today

    while True:
        prev_business_day = prev_business_day - pd.Timedelta(days=1)
        if prev_business_day.dayofweek < 5:  # Monday = 0, Friday = 4
            break

    return prev_business_day.strftime('%m%d%Y')


def get_nav_files(date_str):
    """Get all NAV files for a given date"""
    nav_files = []

    # Check each source directory
    for source in ['HFMX', 'IACAP', 'ETPCAP2', 'CIX', 'DCXPD']:
        source_dir = os.path.join('input', source)
        if os.path.exists(source_dir):
            # Get all CSV files in the directory
            for file in os.listdir(source_dir):
                if file.endswith('.csv'):
                    file_path = os.path.join(source_dir, file)
                    nav_files.append(file_path)

    return nav_files


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
    "morningstar_output_folder_id": os.getenv('MORNINGSTAR_OUTPUT_FOLDER_ID'),
    "six_output_folder_id": os.getenv('SIX_OUTPUT_FOLDER_ID'),
    "input_folder_id": os.getenv('DRIVE_INPUT_FOLDER_ID')
}


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


# Create a config dictionary
config_dict = {
    'mode': 'remote',
    'ftp_configs': ftp_configs,
    'smtp_config': smtp_config,
    'drive_config': drive_config,
    'db_connection_string': 'sqlite:///nav_data.db'
}

# Create AppConfig using from_dict method
app_config = AppConfig.from_dict(config_dict)

processor = NAVProcessor(
    config=app_config
)


@app.route('/nav-data', methods=['GET'])
@require_api_key
def get_nav_data():
    """Get paginated NAV data with filtering options"""
    try:
        # Get query parameters
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        isin = request.args.get('isin')
        series_number = request.args.get('series_number')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        # Convert dates if provided
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d')

        # Get NAV history with pagination
        nav_entries = processor.db_manager.get_nav_history(
            isin=isin,
            series_number=series_number,
            start_date=start_date,
            end_date=end_date,
            page=page,
            per_page=per_page
        )

        response = {
            'status': 'success',
            'data': [
                {
                    'isin': entry.isin,
                    'series_number': entry.series_number,
                    'nav_date': entry.nav_date.strftime('%Y-%m-%d'),
                    'nav_value': float(entry.nav_value),
                    'emitter': entry.emitter
                }
                for entry in nav_entries['entries']
            ],
            'pagination': {
                'current_page': page,
                'per_page': per_page,
                'total_pages': nav_entries['total_pages'],
                'total_entries': nav_entries['total_entries']
            }
        }

        return jsonify(response), 200

    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/fetch-remote-navs', methods=['POST'])
@require_api_key
def fetch_remote_navs():
    """Fetch remote NAV data and save to database"""
    try:
        data = request.get_json() or {}

        # Get date_str (required)
        date_str = data.get('date_str')
        if not date_str:
            return jsonify({
                'status': 'error',
                'message': 'Date is required'
            }), 400

        # Get filters
        isin_filters = data.get('isin_filters', [])
        specific_isins = data.get('isins', [])
        series_number = data.get('series_number')
        series_type = data.get('series_type')  # Renamed from file_type

        print(f"Processing NAVs for date: {date_str}")
        print(f"Filter types: {isin_filters}")
        print(f"Specific ISINs: {specific_isins}")
        print(f"Series number: {series_number}")
        print(f"Series type: {series_type}")

        # If series_type is specified, add it to the filter
        if series_type:
            if isinstance(isin_filters, list):
                isin_filters.append(series_type)
            else:
                isin_filters = [series_type]

        # If specific ISINs or series number is provided, use that as target
        target_isins = None
        if specific_isins or series_number:
            session = get_reliable_session()
            try:
                query = session.query(Series.isin)
                if specific_isins:
                    query = query.filter(Series.isin.in_(specific_isins))
                if series_number:
                    query = query.filter(Series.series_number == series_number)
                target_isins = [row[0] for row in query.all()]
                if not target_isins:
                    return jsonify({
                        'status': 'error',
                        'message': f'No series found matching the provided filters'
                    }), 404
            finally:
                session.close()
        else:
            # Get target ISINs for each filter type and combine them
            target_isins = set()
            for filter_type in isin_filters:
                filter_isins = processor.db_manager.get_target_isins(
                    filter_type)
                if filter_isins is not None:  # Only update if filter_isins is not None
                    target_isins.update(filter_isins)
            target_isins = list(target_isins)

        # Process NAVs without sending email or generating templates
        try:
            # Use process_navs with empty template_types to avoid template generation
            # and send_email=False to avoid sending emails
            processor.process_navs(
                date_str=date_str,
                send_email=False,
                isin_filter=isin_filters,  # Pass the full list including series_type
                template_types=[]  # Empty list to avoid template generation
            )

            return jsonify({
                'status': 'success',
                'message': f'Successfully processed NAV files',
                'date_processed': date_str,
                'stats': {
                    'added': 0,  # We don't have actual stats anymore, but frontend expects these fields
                    'duplicates': 0,
                    'invalids': 0
                },
                'filters_applied': {
                    'filter_types': isin_filters,
                    'specific_isins': specific_isins,
                    'series_number': series_number,
                    'series_type': series_type
                }
            }), 200

        except Exception as e:
            # Log the error but don't treat it as a failure
            print(
                f"Note: Some NAV entries were duplicates or invalid: {str(e)}")
            return jsonify({
                'status': 'success',
                'message': 'Processed NAV files with some duplicates/invalid entries',
                'date_processed': date_str,
                'stats': {
                    'added': 0,
                    'duplicates': 0,
                    'invalids': 0
                },
                'filters_applied': {
                    'filter_types': isin_filters,
                    'specific_isins': specific_isins,
                    'series_number': series_number,
                    'series_type': series_type
                }
            }), 200

    except Exception as e:
        error_traceback = traceback.format_exc()
        print(f"Error in fetch_remote_navs: {str(e)}")
        print(f"Traceback: {error_traceback}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'traceback': error_traceback
        }), 500


@app.route('/generate-templates', methods=['POST'])
@require_api_key
def generate_templates():
    """Generate templates, upload to drive and send emails"""
    try:
        data = request.get_json() or {}

        # Get date_str (required)
        date_str = data.get('date_str')
        if not date_str:
            return jsonify({
                'status': 'error',
                'message': 'Date is required'
            }), 400

        # Get emails (default to env var if set)
        emails = data.get('emails', [])
        if not emails and os.getenv('REPORT_EMAIL_RECIPIENT'):
            emails = [email.strip() for email in os.getenv(
                'REPORT_EMAIL_RECIPIENT').split(',')]

        # Get filters
        isin_filters = data.get('isin_filters', [])
        specific_isins = data.get('isins', [])
        series_number = data.get('series_number')
        series_type = data.get('series_type')  # Renamed from file_type

        # Get template types (default to both)
        template_types = data.get('template_types', ['morningstar', 'six'])

        print(f"Generating templates for date: {date_str}")
        print(f"Sending to emails: {emails}")
        print(f"Filter types: {isin_filters}")
        print(f"Specific ISINs: {specific_isins}")
        print(f"Series number: {series_number}")
        print(f"Series type: {series_type}")
        print(f"Template types: {template_types}")

        # If series_type is specified, add it to the filter
        if series_type:
            if isinstance(isin_filters, list):
                isin_filters.append(series_type)
            else:
                isin_filters = [series_type]

        # If specific ISINs or series number is provided, use that as target
        isin_filter_value = None
        if specific_isins or series_number:
            session = get_reliable_session()
            try:
                query = session.query(Series.isin)
                if specific_isins:
                    query = query.filter(Series.isin.in_(specific_isins))
                if series_number:
                    query = query.filter(Series.series_number == series_number)
                target_isins = [row[0] for row in query.all()]
                if not target_isins:
                    return jsonify({
                        'status': 'error',
                        'message': f'No series found matching the provided filters'
                    }), 404
                isin_filter_value = target_isins
            finally:
                session.close()
        else:
            # Pass the isin_filters directly (including series_type)
            isin_filter_value = isin_filters

        # Process NAVs with template generation and email sending
        processor.process_navs(
            date_str=date_str,
            send_email=bool(emails),
            to_emails=emails,
            isin_filter=isin_filter_value,
            template_types=template_types
        )

        return jsonify({
            'status': 'success',
            'message': 'Successfully generated templates and sent emails',
            'date_processed': date_str,
            'emails_sent_to': emails if emails else [],
            'template_types': template_types,
            'filters_applied': {
                'filter_types': isin_filters,
                'specific_isins': specific_isins,
                'series_number': series_number,
                'series_type': series_type
            }
        }), 200

    except Exception as e:
        error_traceback = traceback.format_exc()
        print(f"Error in generate_templates: {str(e)}")
        print(f"Traceback: {error_traceback}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'traceback': error_traceback
        }), 500


@app.route('/series', methods=['GET'])
@require_api_key
def get_series():
    """Get series information with optional filters"""
    try:
        # Get query parameters
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        status = request.args.get('status')  # A, D, or Matured
        region = request.args.get('region')
        isin = request.args.get('isin')
        series_number = request.args.get('series_number')

        # Query series from database
        session = get_reliable_session()
        try:
            query = session.query(Series)

            # Apply filters
            if status:
                query = query.filter(
                    Series.status == SeriesStatus[status.upper()])
            if region:
                query = query.filter(Series.series_region == region)
            if isin:
                query = query.filter(Series.isin == isin)
            if series_number:
                query = query.filter(Series.series_number == series_number)

            # Get total count
            total = query.count()

            # Apply pagination
            series = query.offset((page - 1) * per_page).limit(per_page).all()

            response = {
                'status': 'success',
                'data': [
                    {
                        'isin': s.isin,
                        'series_number': s.series_number,
                        'series_name': s.series_name,
                        'status': s.status.value,
                        'region': s.series_region,
                        'currency': s.currency,
                        'nav_frequency': s.nav_frequency.value,
                        'issuance_date': s.issuance_date.isoformat() if s.issuance_date else None,
                        'maturity_date': s.maturity_date.isoformat() if s.maturity_date else None
                    }
                    for s in series
                ],
                'pagination': {
                    'current_page': page,
                    'per_page': per_page,
                    'total_pages': math.ceil(total / per_page),
                    'total_entries': total
                }
            }
            return jsonify(response), 200
        finally:
            session.close()

    except Exception as e:
        error_traceback = traceback.format_exc()
        print(f"Error in get_series: {str(e)}")
        print(f"Traceback: {error_traceback}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'traceback': error_traceback
        }), 500


@app.route('/series/<identifier>/nav-history', methods=['GET'])
@require_api_key
def get_series_nav_history(identifier):
    """Get NAV history for a specific series by ISIN or series number"""
    try:
        # Get query parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        distribution_type = request.args.get('distribution_type')
        emitter = request.args.get('emitter')
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))

        # Convert dates if provided
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d')

        # Get NAV history
        nav_entries = processor.db_manager.get_nav_history(
            isin=identifier,
            series_number=identifier,
            start_date=start_date,
            end_date=end_date,
            page=page,
            per_page=per_page
        )

        # Format response
        response = {
            'status': 'success',
            'data': [
                {
                    'nav_date': entry.nav_date.strftime('%Y-%m-%d'),
                    'nav_value': float(entry.nav_value),
                    'distribution_type': entry.distribution_type,
                    'emitter': entry.emitter,
                    'isin': entry.isin,
                    'series_number': entry.series_number
                }
                for entry in nav_entries['entries']
            ],
            'pagination': {
                'current_page': page,
                'per_page': per_page,
                'total_pages': nav_entries['total_pages'],
                'total_entries': nav_entries['total_entries']
            }
        }

        return jsonify(response), 200

    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/series/<identifier>/details', methods=['GET'])
@require_api_key
def get_series_details(identifier):
    """Get detailed information about a specific series by ISIN or series number"""
    try:
        session = get_reliable_session()
        try:
            series = session.query(Series).filter(
                (Series.isin == identifier) | (
                    Series.series_number == identifier)
            ).first()

            if not series:
                return jsonify({
                    'status': 'error',
                    'message': f'Series with identifier {identifier} not found'
                }), 404

            def format_date(date):
                """Helper function to format dates"""
                if date is None or pd.isna(date):
                    return None
                return date.strftime('%Y-%m-%d')

            response = {
                'status': 'success',
                'data': {
                    'isin': series.isin,
                    'series_number': series.series_number,
                    'series_name': series.series_name,
                    'status': series.status.value,
                    'issuance_type': series.issuance_type,
                    'product_type': series.product_type,
                    'dates': {
                        'issuance': format_date(series.issuance_date),
                        'maturity': format_date(series.maturity_date),
                        'close': format_date(series.close_date)
                    },
                    'details': {
                        'issuer': series.issuer,
                        'relationship_manager': series.relationship_manager,
                        'region': series.series_region,
                        'portfolio_manager': {
                            'name': series.portfolio_manager,
                            'jurisdiction': series.portfolio_manager_jurisdiction
                        },
                        'borrower': series.borrower,
                        'asset_manager': series.asset_manager
                    },
                    'financial': {
                        'currency': series.currency,
                        'nav_frequency': series.nav_frequency.value,
                        'issuance_principal_amount': series.issuance_principal_amount,
                        'fees_frequency': series.fees_frequency,
                        'payment_method': series.payment_method
                    },
                    'custodians': [
                        {
                            'name': c.custodian_name,
                            'account_number': c.account_number
                        }
                        for c in series.custodians
                    ],
                    'fee_structures': [
                        {
                            'type': f.fee_type,
                            'category': f.fee_type_category.value,
                            'percentage': f.fee_percentage,
                            'fixed_amount': f.fixed_amount,
                            'currency': f.currency,
                            'aum_threshold': f.aum_threshold
                        }
                        for f in series.fee_structures
                    ]
                }
            }

            return jsonify(response), 200
        finally:
            session.close()

    except Exception as e:
        error_traceback = traceback.format_exc()
        print(f"Error in get_series_details: {str(e)}")
        print(f"Traceback: {error_traceback}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'traceback': error_traceback
        }), 500


@app.route('/series/<identifier>/stakeholders', methods=['GET'])
@require_api_key
def get_series_stakeholders(identifier):
    """Get all stakeholders associated with a specific series by ISIN or series number"""
    try:
        session = get_reliable_session()
        try:
            # Try to find series by ISIN or series number
            series = session.query(Series).filter(
                (Series.isin == identifier) | (
                    Series.series_number == identifier)
            ).first()

            if not series:
                return jsonify({
                    'status': 'error',
                    'message': f'Series with identifier {identifier} not found'
                }), 404

            def format_date(date):
                """Helper function to format dates"""
                if date is None or pd.isna(date):
                    return None
                return date.strftime('%Y-%m-%d')

            stakeholders = {
                'status': 'success',
                'data': {
                    'series_info': {
                        'isin': series.isin,
                        'series_number': series.series_number,
                        'series_name': series.series_name,
                        'status': series.status.value,
                        'issuance_date': format_date(series.issuance_date),
                        'maturity_date': format_date(series.maturity_date),
                        'region': series.series_region
                    },
                    'key_stakeholders': {
                        'issuer': {
                            'name': series.issuer,
                            'role': 'Issuer',
                            'type': 'Primary'
                        },
                        'portfolio_manager': {
                            'name': series.portfolio_manager,
                            'jurisdiction': series.portfolio_manager_jurisdiction,
                            'role': 'Portfolio Manager',
                            'type': 'Primary'
                        },
                        'relationship_manager': {
                            'name': series.relationship_manager,
                            'role': 'Relationship Manager',
                            'type': 'Primary'
                        }
                    },
                    'service_providers': {
                        'borrower': {
                            'name': series.borrower,
                            'role': 'Borrower',
                            'type': 'Service Provider'
                        },
                        'asset_manager': {
                            'name': series.asset_manager,
                            'role': 'Asset Manager',
                            'type': 'Service Provider'
                        },
                        'custodians': [
                            {
                                'name': custodian.custodian_name,
                                'account': custodian.account_number,
                                'role': 'Custodian',
                                'type': 'Service Provider'
                            } for custodian in series.custodians
                        ]
                    },
                    'fees': {
                        'frequency': series.fees_frequency,
                        'payment_method': series.payment_method
                    }
                }
            }

            return jsonify(stakeholders), 200
        finally:
            session.close()

    except Exception as e:
        error_traceback = traceback.format_exc()
        print(f"Error in get_series_stakeholders: {str(e)}")
        print(f"Traceback: {error_traceback}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'traceback': error_traceback
        }), 500


@app.route('/series/<identifier>/fee-structures', methods=['GET'])
@require_api_key
def get_series_fee_structures(identifier):
    """Get all fee structures associated with a specific series by ISIN or series number"""
    try:
        session = get_reliable_session()
        try:
            series = session.query(Series).filter(
                (Series.isin == identifier) | (
                    Series.series_number == identifier)
            ).first()

            if not series:
                return jsonify({
                    'status': 'error',
                    'message': f'Series with identifier {identifier} not found'
                }), 404

            fee_structures = {
                'status': 'success',
                'data': {
                    'series_info': {
                        'isin': series.isin,
                        'series_number': series.series_number,
                        'series_name': series.series_name,
                        'fees_frequency': series.fees_frequency,
                        'payment_method': series.payment_method
                    },
                    'fee_structures': [
                        {
                            'type': fee.fee_type,
                            'category': fee.fee_type_category.value,
                            'aum_threshold': fee.aum_threshold,
                            'fee_percentage': fee.fee_percentage,
                            'fixed_amount': fee.fixed_amount,
                            'currency': fee.currency
                        } for fee in series.fee_structures
                    ]
                }
            }

            return jsonify(fee_structures), 200
        finally:
            session.close()

    except Exception as e:
        error_traceback = traceback.format_exc()
        print(f"Error in get_series_fee_structures: {str(e)}")
        print(f"Traceback: {error_traceback}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'traceback': error_traceback
        }), 500


@app.route('/fee-structures/summary', methods=['GET'])
@require_api_key
def get_fee_structures_summary():
    """Get a summary of all fee structures across series"""
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        fee_type = request.args.get('fee_type')
        category = request.args.get('category')
        isin = request.args.get('isin')
        series_number = request.args.get('series_number')

        session = get_reliable_session()
        try:
            query = session.query(Series, FeeStructure).\
                join(FeeStructure, Series.isin == FeeStructure.series_isin)

            # Apply filters
            if isin:
                query = query.filter(Series.isin == isin)
            if series_number:
                query = query.filter(Series.series_number == series_number)
            if fee_type:
                query = query.filter(FeeStructure.fee_type == fee_type)
            if category:
                query = query.filter(
                    FeeStructure.fee_type_category == category)

            # Get total count
            total = query.count()

            # Apply pagination
            results = query.offset((page - 1) * per_page).limit(per_page).all()

            response = {
                'status': 'success',
                'data': [
                    {
                        'series': {
                            'isin': series.isin,
                            'series_number': series.series_number,
                            'series_name': series.series_name
                        },
                        'fee_structure': {
                            'type': fee.fee_type,
                            'category': fee.fee_type_category.value,
                            'aum_threshold': fee.aum_threshold,
                            'fee_percentage': fee.fee_percentage,
                            'fixed_amount': fee.fixed_amount,
                            'currency': fee.currency
                        }
                    }
                    for series, fee in results
                ],
                'pagination': {
                    'current_page': page,
                    'per_page': per_page,
                    'total_pages': math.ceil(total / per_page),
                    'total_entries': total
                }
            }

            return jsonify(response), 200
        finally:
            session.close()

    except Exception as e:
        error_traceback = traceback.format_exc()
        print(f"Error in get_fee_structures_summary: {str(e)}")
        print(f"Traceback: {error_traceback}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'traceback': error_traceback
        }), 500


@app.route('/statistics', methods=['GET'])
@require_api_key
def get_statistics():
    """Get overall statistics about the NAV data"""
    try:
        session = get_reliable_session()
        try:
            # Get series statistics
            total_series = session.query(Series).count()
            active_series = session.query(Series).filter(
                Series.status == SeriesStatus.ACTIVE).count()

            # Get NAV statistics
            nav_stats = processor.db_manager.verify_nav_entries()

            response = {
                'status': 'success',
                'data': {
                    'series': {
                        'total': total_series,
                        'active': active_series,
                        'inactive': total_series - active_series
                    },
                    'nav_entries': {
                        'total': nav_stats['total_entries'],
                        'date_range': {
                            'earliest': nav_stats['date_range']['earliest'].isoformat() if nav_stats['date_range']['earliest'] else None,
                            'latest': nav_stats['date_range']['latest'].isoformat() if nav_stats['date_range']['latest'] else None
                        },
                        'distribution': nav_stats['distribution_stats']
                    }
                }
            }

            return jsonify(response), 200
        finally:
            session.close()

    except Exception as e:
        error_traceback = traceback.format_exc()
        print(f"Error in get_statistics: {str(e)}")
        print(f"Traceback: {error_traceback}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'traceback': error_traceback
        }), 500


@app.route('/series-qualitative/changes', methods=['POST'])
@require_api_key
def detect_series_changes():
    """Compare a new series qualitative data file with the master file and detect changes"""
    try:
        if 'file' not in request.files:
            return jsonify({
                'status': 'error',
                'message': 'No file uploaded'
            }), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({
                'status': 'error',
                'message': 'No file selected'
            }), 400

        # Save the uploaded file temporarily
        temp_dir = os.path.join(os.path.dirname(__file__), 'temp')
        os.makedirs(temp_dir, exist_ok=True)
        temp_file_path = os.path.join(temp_dir, file.filename)
        file.save(temp_file_path)

        # Initialize change detector with master file and session provider
        master_file_path = os.path.join(os.path.dirname(
            __file__), 'input', 'template', 'Series Qualitative Data.xlsx')

        # Create a session provider function that returns a new session
        def session_provider():
            return get_reliable_session()

        detector = SeriesChangeDetector(
            master_file_path, session_provider)

        # Detect changes
        changes = detector.detect_changes(temp_file_path)
        report = detector.generate_change_report(changes)

        # Clean up temporary file
        os.remove(temp_file_path)

        # Format changes for JSON response
        changes_json = [
            {
                'isin': change.isin,
                'series_number': change.series_number,
                'nav_frequency': change.nav_frequency,
                'change_type': change.change_type,
                'field_name': change.field_name,
                'old_value': str(change.old_value) if change.old_value is not None else None,
                'new_value': str(change.new_value) if change.new_value is not None else None
            }
            for change in changes
        ]

        return jsonify({
            'status': 'success',
            'changes': changes_json,
            'report': report
        }), 200

    except Exception as e:
        error_traceback = traceback.format_exc()
        print(f"Error in detect_series_changes: {str(e)}")
        print(f"Traceback: {error_traceback}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'traceback': error_traceback
        }), 500


@app.route('/series-qualitative/update', methods=['POST'])
@require_api_key
def update_series_master():
    """Update the master series qualitative data file with a new version"""
    try:
        if 'file' not in request.files:
            return jsonify({
                'status': 'error',
                'message': 'No file uploaded'
            }), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({
                'status': 'error',
                'message': 'No file selected'
            }), 400

        # Save the uploaded file temporarily
        temp_dir = os.path.join(os.path.dirname(__file__), 'temp')
        os.makedirs(temp_dir, exist_ok=True)
        temp_file_path = os.path.join(temp_dir, file.filename)
        file.save(temp_file_path)

        # Initialize change detector and update master file
        master_file_path = os.path.join(os.path.dirname(
            __file__), 'input', 'template', 'Series Qualitative Data.xlsx')

        # Create a session provider function that returns a new session
        def session_provider():
            return get_reliable_session()

        detector = SeriesChangeDetector(
            master_file_path, session_provider)

        detector.update_master_file(temp_file_path)

        # Clean up temporary file
        os.remove(temp_file_path)

        return jsonify({
            'status': 'success',
            'message': 'Master file and database have been updated successfully'
        }), 200

    except Exception as e:
        error_traceback = traceback.format_exc()
        print(f"Error in update_series_master: {str(e)}")
        print(f"Traceback: {error_traceback}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'traceback': error_traceback
        }), 500


@app.route('/')
def index():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>NAV Processor Dashboard</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <link href="https://cdn.jsdelivr.net/npm/vue@2.6.14/dist/vue.js">
        <style>
            .table-container { max-height: 600px; overflow-y: auto; }
            .action-buttons { margin: 20px 0; }
            .filters { margin-bottom: 20px; }
            .modal-mask {
                position: fixed;
                z-index: 9998;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                background-color: rgba(0, 0, 0, .5);
                display: flex;
                transition: opacity .3s ease;
                align-items: center;
            }
            .modal-container {
                width: 500px;
                max-height: 90vh;
                margin: auto;
                background-color: #fff;
                border-radius: 4px;
                box-shadow: 0 2px 8px rgba(0, 0, 0, .33);
                transition: all .3s ease;
                display: flex;
                flex-direction: column;
            }
            .modal-header {
                padding: 15px;
                border-bottom: 1px solid #dee2e6;
                flex-shrink: 0;
            }
            .modal-body {
                padding: 15px;
                overflow-y: auto;
            }
            .modal-footer {
                padding: 15px;
                border-top: 1px solid #dee2e6;
                flex-shrink: 0;
            }
            .ellipsis { pointer-events: none; }
            .pagination { justify-content: center; }
            .nav-tabs { margin-bottom: 20px; }
            .series-details { 
                max-height: 300px;
                overflow-y: auto;
                margin-top: 10px;
                padding: 10px;
                border: 1px solid #dee2e6;
                border-radius: 4px;
            }
            .search-group {
                margin-bottom: 15px;
            }
            .search-group label {
                display: block;
                margin-bottom: 5px;
                font-weight: 500;
            }
            .fee-type-badge {
                font-size: 0.85em;
                padding: 3px 8px;
                border-radius: 12px;
                background-color: #e9ecef;
                display: inline-block;
                margin-bottom: 3px;
            }
            .stakeholder-role {
                color: #495057;
                font-weight: 600;
                border-bottom: 2px solid #e9ecef;
                padding-bottom: 0.5rem;
                margin-bottom: 1rem;
            }
            .stakeholder-item {
                padding: 1rem;
                background-color: #f8f9fa;
                border-radius: 0.25rem;
            }
            .custodian-item {
                padding: 0.5rem;
                background-color: #f8f9fa;
                border-radius: 0.25rem;
            }
            .badge {
                font-size: 0.875rem;
                padding: 0.5em 1em;
            }
            /* Snackbar styles */
            .snackbar {
                position: fixed;
                bottom: 20px;
                left: 50%;
                transform: translateX(-50%);
                background-color: #333;
                color: white;
                padding: 12px 24px;
                border-radius: 4px;
                z-index: 9999;
                display: flex;
                align-items: center;
                box-shadow: 0 2px 5px rgba(0,0,0,0.2);
                min-width: 250px;
                max-width: 80%;
            }
            .snackbar.success {
                background-color: #4caf50;
            }
            .snackbar.error {
                background-color: #f44336;
            }
            .snackbar.warning {
                background-color: #ff9800;
            }
            .snackbar-content {
                flex-grow: 1;
                margin-right: 12px;
            }
            .snackbar-close {
                cursor: pointer;
                opacity: 0.7;
            }
            .snackbar-close:hover {
                opacity: 1;
            }
        </style>
    </head>
    <body>
        <div id="app" class="container mt-4">
            <!-- Snackbar Component -->
            <div v-if="snackbar.show" class="snackbar" :class="snackbar.type">
                <div class="snackbar-content">{{ snackbar.message }}</div>
                <div class="snackbar-close" @click="hideSnackbar">&times;</div>
            </div>

            <h1>NAV Processor Dashboard</h1>
            
            <!-- Tab Navigation -->
            <ul class="nav nav-tabs">
                <li class="nav-item">
                    <a class="nav-link" :class="{ active: activeTab === 'nav' }" 
                       href="#" @click.prevent="activeTab = 'nav'">NAV Data</a>
                </li>
                <li class="nav-item">
                    <a class="nav-link" :class="{ active: activeTab === 'series' }" 
                       href="#" @click.prevent="activeTab = 'series'">Series Data</a>
                </li>
                <li class="nav-item">
                    <a class="nav-link" :class="{ active: activeTab === 'stakeholders' }" 
                       href="#" @click.prevent="activeTab = 'stakeholders'">Stakeholders</a>
                </li>
                <li class="nav-item">
                    <a class="nav-link" :class="{ active: activeTab === 'fees' }" 
                       href="#" @click.prevent="activeTab = 'fees'">Fee Structures</a>
                </li>
                <li class="nav-item">
                    <a class="nav-link" :class="{ active: activeTab === 'series-qualitative' }" 
                       href="#" @click.prevent="activeTab = 'series-qualitative'">Series Qualitative Data</a>
                </li>
                <li class="nav-item">
                    <a class="nav-link" :class="{ active: activeTab === 'trades' }" 
                       href="#" @click.prevent="activeTab = 'trades'">Trades</a>
                </li>
            </ul>

            <!-- NAV Data Tab -->
            <div v-if="activeTab === 'nav'">
                <div class="card mb-4">
                    <div class="card-header">
                        <h5 class="mb-0">Search NAV Data</h5>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-3">
                                <div class="search-group">
                                    <label>ISIN</label>
                                    <input type="text" v-model="filters.isin" 
                                           class="form-control" placeholder="Enter ISIN">
                                </div>
                            </div>
                            <div class="col-md-3">
                                <div class="search-group">
                                    <label>Series Number</label>
                                    <input type="text" v-model="filters.series_number" 
                                           class="form-control" placeholder="Enter Series Number">
                                </div>
                            </div>
                            <div class="col-md-3">
                                <div class="search-group">
                                    <label>Start Date</label>
                                    <input type="date" v-model="filters.startDate" class="form-control">
                                </div>
                            </div>
                            <div class="col-md-3">
                                <div class="search-group">
                                    <label>End Date</label>
                                    <input type="date" v-model="filters.endDate" class="form-control">
                                </div>
                            </div>
                        </div>
                        <div class="row mt-3">
                            <div class="col-12">
                                <button @click="loadData" class="btn btn-primary">Search</button>
                                <button @click="clearNavFilters" class="btn btn-secondary ms-2">Clear Filters</button>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Action Buttons -->
                <div class="action-buttons mb-3">
                    <button @click="showFetchNavModal" class="btn btn-success">Fetch Remote NAVs</button>
                    <button @click="showGenerateTemplatesModal" class="btn btn-info">Generate Templates</button>
                </div>

                <!-- NAV Data Table -->
                <div class="table-container">
                    <table class="table table-striped">
                        <thead>
                            <tr>
                                <th>Series Info</th>
                                <th>Date</th>
                                <th>NAV Value</th>
                                <th>Issuer</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr v-for="nav in navData">
                                <td>
                                    <div><strong>ISIN:</strong> {{ nav.isin }}</div>
                                    <div class="small text-muted">Series: {{ nav.series_number }}</div>
                                </td>
                                <td>{{ nav.nav_date }}</td>
                                <td>{{ nav.nav_value }}</td>
                                <td>{{ nav.emitter }}</td>
                            </tr>
                            <tr v-if="navData.length === 0">
                                <td colspan="4" class="text-center">No NAV data found matching your criteria</td>
                            </tr>
                        </tbody>
                    </table>
                </div>

                <!-- Pagination -->
                <nav v-if="totalPages > 0">
                    <ul class="pagination">
                        <li class="page-item" :class="{ disabled: currentPage === 1 }">
                            <a class="page-link" href="#" @click.prevent="changePage(currentPage - 1)">Previous</a>
                        </li>
                        <li v-for="page in middlePages" class="page-item" :class="{ active: page === currentPage }">
                            <a class="page-link" href="#" @click.prevent="changePage(page)">{{ page }}</a>
                        </li>
                        <li class="page-item" :class="{ disabled: currentPage === totalPages }">
                            <a class="page-link" href="#" @click.prevent="changePage(currentPage + 1)">Next</a>
                        </li>
                    </ul>
                </nav>
            </div>

            <!-- Series Data Tab -->
            <div v-if="activeTab === 'series'">
                <!-- Series Filters -->
                <div class="card mb-4">
                    <div class="card-header">
                        <h5 class="mb-0">Search Series</h5>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-3">
                                <div class="search-group">
                                    <label>ISIN</label>
                                    <input type="text" v-model="seriesFilters.isin" 
                                           class="form-control" placeholder="Enter ISIN">
                                </div>
                            </div>
                            <div class="col-md-3">
                                <div class="search-group">
                                    <label>Series Number</label>
                                    <input type="text" v-model="seriesFilters.series_number" 
                                           class="form-control" placeholder="Enter Series Number">
                                </div>
                            </div>
                            <div class="col-md-3">
                                <div class="search-group">
                                    <label>Status</label>
                                    <select v-model="seriesFilters.status" class="form-control">
                                        <option value="">All Status</option>
                                        <option value="A">Active</option>
                                        <option value="D">Discontinued</option>
                                        <option value="MATURED">Matured</option>
                                    </select>
                                </div>
                            </div>
                            <div class="col-md-3">
                                <div class="search-group">
                                    <label>Region</label>
                                    <input type="text" v-model="seriesFilters.region" 
                                           class="form-control" placeholder="Enter Region">
                                </div>
                            </div>
                        </div>
                        <div class="row mt-3">
                            <div class="col-12">
                                <button @click="loadSeriesData" class="btn btn-primary">Search</button>
                                <button @click="clearSeriesFilters" class="btn btn-secondary ms-2">Clear Filters</button>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Series Table -->
                <div class="table-container">
                    <table class="table table-striped">
                        <thead>
                            <tr>
                                <th>Series Info</th>
                                <th>Status</th>
                                <th>Region</th>
                                <th>Currency</th>
                                <th>NAV Frequency</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr v-for="series in seriesData">
                                <td>
                                    <div><strong>{{ series.series_name }}</strong></div>
                                    <div class="small text-muted">
                                        ISIN: {{ series.isin }}<br>
                                        Series: {{ series.series_number }}
                                    </div>
                                </td>
                                <td>
                                    <span class="badge" :class="getStatusBadgeClass(series.status)">
                                        {{ series.status }}
                                    </span>
                                </td>
                                <td>{{ series.region }}</td>
                                <td>{{ series.currency }}</td>
                                <td>{{ series.nav_frequency }}</td>
                                <td>
                                    <button @click="viewSeriesDetails(series.isin)" class="btn btn-sm btn-info">View Details</button>
                                </td>
                            </tr>
                            <tr v-if="seriesData.length === 0">
                                <td colspan="6" class="text-center">No series found matching your criteria</td>
                            </tr>
                        </tbody>
                    </table>
                </div>

                <!-- Series Pagination -->
                <nav v-if="totalSeriesPages > 0">
                    <ul class="pagination">
                        <li class="page-item" :class="{ disabled: currentSeriesPage === 1 }">
                            <a class="page-link" href="#" @click.prevent="changeSeriesPage(currentSeriesPage - 1)">Previous</a>
                        </li>
                        <li v-for="page in seriesMiddlePages" class="page-item" :class="{ active: page === currentSeriesPage }">
                            <a class="page-link" href="#" @click.prevent="changeSeriesPage(page)">{{ page }}</a>
                        </li>
                        <li class="page-item" :class="{ disabled: currentSeriesPage === totalSeriesPages }">
                            <a class="page-link" href="#" @click.prevent="changeSeriesPage(currentSeriesPage + 1)">Next</a>
                        </li>
                    </ul>
                </nav>

                <!-- Series Details Modal -->
                <div v-if="showSeriesDetails" class="modal-mask">
                    <div class="modal-container">
                        <div class="modal-header">
                            <h4>Series Details</h4>
                        </div>
                        <div class="modal-body">
                            <div v-if="selectedSeriesDetails" class="series-details">
                                <h5>{{ selectedSeriesDetails.series_name }}</h5>
                                <div class="row">
                                    <div class="col-md-6">
                                        <p><strong>ISIN:</strong> {{ selectedSeriesDetails.isin }}</p>
                                        <p><strong>Series Number:</strong> {{ selectedSeriesDetails.series_number }}</p>
                                        <p><strong>Status:</strong> <span class="badge" :class="getStatusBadgeClass(selectedSeriesDetails.status)">{{ selectedSeriesDetails.status }}</span></p>
                                    </div>
                                    <div class="col-md-6">
                                        <p><strong>Currency:</strong> {{ selectedSeriesDetails.financial.currency }}</p>
                                        <p><strong>NAV Frequency:</strong> {{ selectedSeriesDetails.financial.nav_frequency }}</p>
                                        <p><strong>Region:</strong> {{ selectedSeriesDetails.details.region }}</p>
                                    </div>
                                </div>
                                <hr>
                                <div class="stakeholder-section">
                                    <h6>Key Stakeholders</h6>
                                    <div class="row">
                                        <div class="col-md-6">
                                            <p><strong>Issuer:</strong> {{ selectedSeriesDetails.details.issuer }}</p>
                                            <p><strong>Portfolio Manager:</strong> {{ selectedSeriesDetails.details.portfolio_manager.name }}</p>
                                            <p><strong>Relationship Manager:</strong> {{ selectedSeriesDetails.details.relationship_manager }}</p>
                                        </div>
                                        <div class="col-md-6">
                                            <p><strong>Borrower:</strong> {{ selectedSeriesDetails.details.borrower }}</p>
                                            <p><strong>Asset Manager:</strong> {{ selectedSeriesDetails.details.asset_manager }}</p>
                                        </div>
                                    </div>
                                </div>
                                <hr>
                                <div class="custodians-section">
                                    <h6>Custodians</h6>
                                    <div class="custodian-list">
                                        <div v-for="custodian in selectedSeriesDetails.custodians" class="custodian-item mb-2">
                                            <p class="mb-1"><strong>{{ custodian.name }}</strong></p>
                                            <p class="mb-0 small text-muted">Account: {{ custodian.account_number || 'N/A' }}</p>
                                        </div>
                                    </div>
                                </div>
                                <hr>
                                <div class="fees-section">
                                    <h6>Fee Structures</h6>
                                    <div class="fee-list">
                                        <div v-for="fee in selectedSeriesDetails.fee_structures" class="mb-2">
                                            <span class="fee-type-badge">{{ fee.type }}</span>
                                            <p class="mb-1">
                                                <span v-if="fee.percentage !== null">{{ (fee.percentage * 100).toFixed(2) }}%</span>
                                                <span v-else-if="fee.fixed_amount !== null">{{ fee.fixed_amount }} {{ fee.currency }}</span>
                                                <span v-if="fee.aum_threshold">(AUM Threshold: {{ fee.aum_threshold }}MM)</span>
                                            </p>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div class="modal-footer">
                            <button class="btn btn-secondary" @click="showSeriesDetails = false">Close</button>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Stakeholders Tab -->
            <div v-if="activeTab === 'stakeholders'">
                <div class="card mb-4">
                    <div class="card-header">
                        <h5 class="mb-0">Search Stakeholders</h5>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-6">
                                <div class="search-group">
                                    <label>ISIN or Series Number</label>
                                    <input type="text" v-model="stakeholderFilters.identifier" 
                                           class="form-control" placeholder="Enter ISIN or Series Number">
                                </div>
                            </div>
                            <div class="col-12 mt-3">
                                <button @click="loadStakeholderData" class="btn btn-primary">Search</button>
                                <button @click="clearStakeholderFilters" class="btn btn-secondary ms-2">Clear</button>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Stakeholders Data -->
                <div v-if="stakeholderData" class="stakeholder-details card">
                    <div class="card-header">
                        <h5 class="mb-0">{{ stakeholderData.series_info.series_name }}</h5>
                        <div class="small text-muted">
                            ISIN: {{ stakeholderData.series_info.isin }} | 
                            Series: {{ stakeholderData.series_info.series_number }}
                        </div>
                    </div>
                    <div class="card-body">
                        <!-- Primary Stakeholders -->
                        <div class="mb-4">
                            <h6 class="stakeholder-role">Primary Stakeholders</h6>
                            <div class="row">
                                <div class="col-md-4 mb-3" v-for="(stakeholder, role) in stakeholderData.key_stakeholders">
                                    <div class="stakeholder-item">
                                        <h6 class="mb-2">{{ formatStakeholderRole(role) }}</h6>
                                        <p class="mb-1">{{ stakeholder.name }}</p>
                                        <small v-if="stakeholder.jurisdiction" class="text-muted">
                                            {{ stakeholder.jurisdiction }}
                                        </small>
                                    </div>
                                </div>
                            </div>
                        </div>

                        <!-- Service Providers -->
                        <div class="mb-4">
                            <h6 class="stakeholder-role">Service Providers</h6>
                            <div class="row">
                                <div class="col-md-4 mb-3" v-for="(provider, role) in stakeholderData.service_providers" 
                                     v-if="role !== 'custodians'">
                                    <div class="stakeholder-item">
                                        <h6 class="mb-2">{{ formatStakeholderRole(role) }}</h6>
                                        <p class="mb-1">{{ provider.name }}</p>
                                    </div>
                                </div>
                            </div>
                        </div>

                        <!-- Custodians -->
                        <div class="mb-4">
                            <h6 class="stakeholder-role">Custodians</h6>
                            <div class="row">
                                <div class="col-md-4 mb-3" v-for="custodian in stakeholderData.service_providers.custodians">
                                    <div class="stakeholder-item">
                                        <p class="mb-1"><strong>{{ custodian.name }}</strong></p>
                                        <small class="text-muted">Account: {{ custodian.account || 'N/A' }}</small>
                                    </div>
                                </div>
                            </div>
                        </div>

                        <!-- Fee Information -->
                        <div>
                            <h6 class="stakeholder-role">Fee Information</h6>
                            <div class="row">
                                <div class="col-md-6">
                                    <p><strong>Frequency:</strong> {{ stakeholderData.fees.frequency || 'N/A' }}</p>
                                </div>
                                <div class="col-md-6">
                                    <p><strong>Payment Method:</strong> {{ stakeholderData.fees.payment_method || 'N/A' }}</p>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Fee Structures Tab -->
            <div v-if="activeTab === 'fees'">
                <div class="card mb-4">
                    <div class="card-header">
                        <h5 class="mb-0">Search Fee Structures</h5>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-4">
                                <div class="search-group">
                                    <label>ISIN</label>
                                    <input type="text" v-model="feeFilters.isin" 
                                           class="form-control" placeholder="Enter ISIN">
                                </div>
                            </div>
                            <div class="col-md-4">
                                <div class="search-group">
                                    <label>Series Number</label>
                                    <input type="text" v-model="feeFilters.series_number" 
                                           class="form-control" placeholder="Enter Series Number">
                                </div>
                            </div>
                            <div class="col-md-4">
                                <div class="search-group">
                                    <label>Fee Category</label>
                                    <select v-model="feeFilters.category" class="form-control">
                                        <option value="">All Categories</option>
                                        <option value="FIXED">Fixed</option>
                                        <option value="AUM_BASED">AUM Based</option>
                                    </select>
                                </div>
                            </div>
                        </div>
                        <div class="row mt-3">
                            <div class="col-12">
                                <button @click="loadFeeStructures" class="btn btn-primary">Search</button>
                                <button @click="clearFeeFilters" class="btn btn-secondary ms-2">Clear Filters</button>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Fee Structures Table -->
                <div class="table-container">
                    <table class="table table-striped">
                        <thead>
                            <tr>
                                <th>Series Info</th>
                                <th>Fee Type</th>
                                <th>Category</th>
                                <th>Value</th>
                                <th>AUM Threshold</th>
                                <th>Currency</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr v-for="item in feeStructures">
                                <td>
                                    <div><strong>{{ item.series.series_name }}</strong></div>
                                    <div class="small text-muted">
                                        ISIN: {{ item.series.isin }}<br>
                                        Series: {{ item.series.series_number }}
                                    </div>
                                </td>
                                <td>{{ item.fee_structure.type }}</td>
                                <td>{{ item.fee_structure.category }}</td>
                                <td>
                                    <span v-if="item.fee_structure.fee_percentage !== null">
                                        {{ (item.fee_structure.fee_percentage * 100).toFixed(2) }}%
                                    </span>
                                    <span v-else-if="item.fee_structure.fixed_amount !== null">
                                        {{ item.fee_structure.fixed_amount }}
                                    </span>
                                    <span v-else>-</span>
                                </td>
                                <td>
                                    <span v-if="item.fee_structure.aum_threshold">
                                        {{ formatAUMThreshold(item.fee_structure.aum_threshold) }}
                                    </span>
                                    <span v-else>-</span>
                                </td>
                                <td>{{ item.fee_structure.currency || '-' }}</td>
                            </tr>
                            <tr v-if="feeStructures.length === 0">
                                <td colspan="6" class="text-center">
                                    No fee structures found matching your criteria
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>

                <!-- Fee Structures Pagination -->
                <nav v-if="totalFeePages > 0">
                    <ul class="pagination">
                        <li class="page-item" :class="{ disabled: currentFeePage === 1 }">
                            <a class="page-link" href="#" @click.prevent="changeFeePage(currentFeePage - 1)">Previous</a>
                        </li>
                        <li v-for="page in feePages" class="page-item" :class="{ active: page === currentFeePage }">
                            <a class="page-link" href="#" @click.prevent="changeFeePage(page)">{{ page }}</a>
                        </li>
                        <li class="page-item" :class="{ disabled: currentFeePage === totalFeePages }">
                            <a class="page-link" href="#" @click.prevent="changeFeePage(currentFeePage + 1)">Next</a>
                        </li>
                    </ul>
                </nav>
            </div>

            <!-- Series Qualitative Data Tab -->
            <div v-if="activeTab === 'series-qualitative'" class="mt-4">
                <div class="card mb-4">
                    <div class="card-header">
                        <h5 class="mb-0">Series Qualitative Data Management</h5>
                    </div>
                    <div class="card-body">
                        <div class="row mb-4">
                            <div class="col-md-6">
                                <h6>Upload New Series Data File</h6>
                                <div class="input-group">
                                    <input type="file" class="form-control" ref="fileInput" @change="handleFileSelect" accept=".xlsx">
                                    <button class="btn btn-primary" @click="detectChanges" :disabled="!selectedFile">
                                        Detect Changes
                                    </button>
                                </div>
                                <small class="text-muted">Upload a new Series Qualitative Data file to detect changes</small>
                            </div>
                            <div class="col-md-6">
                                <h6>Import from Google Drive</h6>
                                <div class="input-group">
                                    <button class="btn btn-success" @click="importFromGoogleDrive">
                                        Import Latest Master File
                                    </button>
                                </div>
                                <small class="text-muted">Import the most recent Series Qualitative Data file from Google Drive</small>
                            </div>
                        </div>

                        <!-- Change Detection Results -->
                        <div v-if="changeResults" class="mt-4">
                            <h6>Change Detection Results</h6>
                            
                            <!-- New Series -->
                            <div v-if="getChangesByType('NEW_SERIES').length > 0" class="mb-4">
                                <h6 class="text-success">New Series Added</h6>
                                <ul class="list-group">
                                    <li v-for="change in getChangesByType('NEW_SERIES')" class="list-group-item">
                                        <div class="d-flex justify-content-between align-items-start">
                                            <div>
                                                <strong>ISIN:</strong> {{ change.isin }}
                                                <br>
                                                <strong>Series Number:</strong> {{ change.series_number }}
                                                <br>
                                                <strong>NAV Frequency:</strong> {{ change.nav_frequency }}
                                            </div>
                                        </div>
                                    </li>
                                </ul>
                            </div>

                            <!-- Removed Series -->
                            <div v-if="getChangesByType('REMOVED_SERIES').length > 0" class="mb-4">
                                <h6 class="text-danger">Series Removed</h6>
                                <ul class="list-group">
                                    <li v-for="change in getChangesByType('REMOVED_SERIES')" class="list-group-item">
                                        <div class="d-flex justify-content-between align-items-start">
                                            <div>
                                                <strong>ISIN:</strong> {{ change.isin }}
                                                <br>
                                                <strong>Series Number:</strong> {{ change.series_number }}
                                            </div>
                                        </div>
                                    </li>
                                </ul>
                            </div>

                            <!-- Field Updates -->
                            <div v-if="getChangesByType('FIELD_UPDATE').length > 0" class="mb-4">
                                <h6 class="text-primary">Field Updates</h6>
                                <div class="table-responsive">
                                    <table class="table table-striped">
                                        <thead>
                                            <tr>
                                                <th>ISIN</th>
                                                <th>Series Number</th>
                                                <th>Field</th>
                                                <th>Old Value</th>
                                                <th>New Value</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            <tr v-for="change in getChangesByType('FIELD_UPDATE')">
                                                <td>{{ change.isin }}</td>
                                                <td>{{ change.series_number }}</td>
                                                <td>{{ change.field_name }}</td>
                                                <td>{{ change.old_value || 'N/A' }}</td>
                                                <td>{{ change.new_value || 'N/A' }}</td>
                                            </tr>
                                        </tbody>
                                    </table>
                                </div>
                            </div>

                            <!-- Combined Update Button -->
                            <div class="mt-4" v-if="changeResults">
                                <button class="btn btn-success" @click="applyChanges">
                                    Apply Changes
                                </button>
                                <small class="text-muted ms-2">
                                    This will update the master file with the detected changes. A backup will be created automatically.
                                </small>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Trades Tab -->
            <div v-if="activeTab === 'trades'">
                <div class="card mb-4">
                    <div class="card-header">
                        <h5 class="mb-0">Search Trades</h5>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-3">
                                <div class="search-group">
                                    <label>Series Number</label>
                                    <input type="text" v-model="tradeFilters.series_number" 
                                           class="form-control" placeholder="Enter Series Number">
                                </div>
                            </div>
                            <div class="col-md-3">
                                <div class="search-group">
                                    <label>Start Date</label>
                                    <input type="date" v-model="tradeFilters.startDate" class="form-control">
                                </div>
                            </div>
                            <div class="col-md-3">
                                <div class="search-group">
                                    <label>End Date</label>
                                    <input type="date" v-model="tradeFilters.endDate" class="form-control">
                                </div>
                            </div>
                            <div class="col-md-3">
                                <div class="search-group">
                                    <label>Security Type</label>
                                    <input type="text" v-model="tradeFilters.security_type" 
                                           class="form-control" placeholder="Enter Security Type">
                                </div>
                            </div>
                        </div>
                        <div class="row mt-3">
                            <div class="col-12">
                                <button @click="loadTradeData" class="btn btn-primary">Search</button>
                                <button @click="clearTradeFilters" class="btn btn-secondary ms-2">Clear Filters</button>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Trade Summary -->
                <div class="card mb-4" v-if="tradeSummary">
                    <div class="card-header">
                        <h5 class="mb-0">Trade Summary</h5>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-3">
                                <div class="card bg-light">
                                    <div class="card-body text-center">
                                        <h6 class="card-title">Total Trades</h6>
                                        <h3 class="mb-0">{{ tradeSummary.total_trades }}</h3>
                                    </div>
                                </div>
                            </div>
                            <div class="col-md-3">
                                <div class="card bg-light">
                                    <div class="card-body text-center">
                                        <h6 class="card-title">Date Range</h6>
                                        <p class="mb-0">
                                            {{ tradeSummary.date_range.earliest }} to {{ tradeSummary.date_range.latest }}
                                        </p>
                                    </div>
                                </div>
                            </div>
                            <div class="col-md-6">
                                <div class="card bg-light">
                                    <div class="card-body">
                                        <h6 class="card-title">Trades by Source</h6>
                                        <div class="row">
                                            <div class="col-6" v-for="folder in tradeSummary.trades_by_folder">
                                                <p class="mb-1">
                                                    {{ folder.folder }}: {{ folder.count }} trades
                                                </p>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Trades Table -->
                <div class="table-container">
                    <table class="table table-striped">
                        <thead>
                            <tr>
                                <th>Trade Date</th>
                                <th>Series</th>
                                <th>Security</th>
                                <th>Type</th>
                                <th>Quantity</th>
                                <th>Price</th>
                                <th>Value</th>
                                <th>Currency</th>
                                <th>Source</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr v-for="trade in tradeData">
                                <td>{{ trade.trade_date }}</td>
                                <td>{{ trade.series_number }}</td>
                                <td>
                                    <div><strong>{{ trade.security_name }}</strong></div>
                                    <div class="small text-muted">{{ trade.security_id }}</div>
                                </td>
                                <td>
                                    <span class="badge" :class="getTradeTypeBadgeClass(trade.trade_type)">
                                        {{ trade.trade_type }}
                                    </span>
                                </td>
                                <td>{{ formatNumber(trade.quantity) }}</td>
                                <td>{{ formatNumber(trade.price) }}</td>
                                <td>{{ formatNumber(trade.trade_value) }}</td>
                                <td>{{ trade.currency }}</td>
                                <td>
                                    <div>{{ trade.source_folder }}</div>
                                    <div class="small text-muted">{{ trade.source_file }}</div>
                                </td>
                            </tr>
                            <tr v-if="tradeData.length === 0">
                                <td colspan="9" class="text-center">No trades found matching your criteria</td>
                            </tr>
                        </tbody>
                    </table>
                </div>

                <!-- Trade Pagination -->
                <nav v-if="totalTradePages > 0">
                    <ul class="pagination">
                        <li class="page-item" :class="{ disabled: currentTradePage === 1 }">
                            <a class="page-link" href="#" @click.prevent="changeTradePage(currentTradePage - 1)">Previous</a>
                        </li>
                        <li v-for="page in tradeMiddlePages" class="page-item" :class="{ active: page === currentTradePage }">
                            <a class="page-link" href="#" @click.prevent="changeTradePage(page)">{{ page }}</a>
                        </li>
                        <li class="page-item" :class="{ disabled: currentTradePage === totalTradePages }">
                            <a class="page-link" href="#" @click.prevent="changeTradePage(currentTradePage + 1)">Next</a>
                        </li>
                    </ul>
                </nav>
            </div>

            <!-- Fetch Remote NAVs Modal -->
            <div v-if="showFetchNav" class="modal-mask">
                <div class="modal-container">
                    <div class="modal-header">
                        <h4>Fetch Remote NAVs</h4>
                    </div>
                    <div class="modal-body">
                        <div class="mb-3">
                            <label class="form-label">Date</label>
                            <input type="date" v-model="fetchNavForm.date_str" class="form-control">
                        </div>
                        <div class="mb-3">
                            <label class="form-label">NAV Frequency Filters</label>
                            <div class="form-check">
                                <input class="form-check-input" type="checkbox" value="daily" v-model="fetchNavForm.isin_filters">
                                <label class="form-check-label">Daily Series</label>
                            </div>
                            <div class="form-check">
                                <input class="form-check-input" type="checkbox" value="weekly" v-model="fetchNavForm.isin_filters">
                                <label class="form-check-label">Weekly Series</label>
                            </div>
                            <div class="form-check">
                                <input class="form-check-input" type="checkbox" value="monthly" v-model="fetchNavForm.isin_filters">
                                <label class="form-check-label">Monthly Series</label>
                            </div>
                        </div>
                        <div class="mb-3">
                            <label class="form-label">Series Type Filter</label>
                            <div class="form-check">
                                <input class="form-check-input" type="radio" name="fetchNavSeriesType" value="" v-model="fetchNavForm.series_type" checked>
                                <label class="form-check-label">All Series Types</label>
                            </div>
                            <div class="form-check">
                                <input class="form-check-input" type="radio" name="fetchNavSeriesType" value="wrappers_hybrid" v-model="fetchNavForm.series_type">
                                <label class="form-check-label">Wrappers Hybrid Series</label>
                            </div>
                            <div class="form-check">
                                <input class="form-check-input" type="radio" name="fetchNavSeriesType" value="loan" v-model="fetchNavForm.series_type">
                                <label class="form-check-label">Loan Series</label>
                            </div>
                        </div>
                        <div class="mb-3">
                            <label class="form-label">Specific ISINs</label>
                            <div class="input-group mb-2">
                                <input type="text" v-model="newIsin" class="form-control" placeholder="Enter ISIN">
                                <button class="btn btn-outline-secondary" @click="addIsin('fetchNav')">Add</button>
                            </div>
                            <div v-if="fetchNavForm.isins.length > 0" class="mt-2">
                                <div v-for="(isin, index) in fetchNavForm.isins" class="badge bg-secondary me-2 mb-2">
                                    {{ isin }}
                                    <button type="button" class="btn-close btn-close-white ms-2" 
                                            @click="removeIsin('fetchNav', index)" aria-label="Close"></button>
                                </div>
                            </div>
                        </div>
                        <div class="mb-3">
                            <label class="form-label">Series Number</label>
                            <input type="text" v-model="fetchNavForm.series_number" class="form-control" placeholder="Enter Series Number">
                        </div>
                        <div v-if="loadingStates.fetchNav" class="alert alert-info">
                            {{ progressMessages.fetchNav }}
                        </div>
                    </div>
                    <div class="modal-footer">
                        <button class="btn btn-secondary" @click="showFetchNav = false">Cancel</button>
                        <button class="btn btn-primary" @click="fetchRemoteNavs" :disabled="loadingStates.fetchNav">
                            Fetch NAVs
                        </button>
                    </div>
                </div>
            </div>

            <!-- Generate Templates Modal -->
            <div v-if="showGenerateTemplates" class="modal-mask">
                <div class="modal-container">
                    <div class="modal-header">
                        <h4>Generate Templates</h4>
                    </div>
                    <div class="modal-body">
                        <div class="mb-3">
                            <label class="form-label">Date</label>
                            <input type="date" v-model="generateTemplatesForm.date_str" class="form-control">
                        </div>
                        <div class="mb-3">
                            <label class="form-label">Distribution Type</label>
                            <select v-model="generateTemplatesForm.distribution_type" class="form-control" @change="updateEmailList">
                                <option value="morningstar">Morningstar</option>
                                <option value="six">SIX Financial</option>
                                <option value="custom">Custom Email</option>
                            </select>
                        </div>
                        <div v-if="generateTemplatesForm.distribution_type === 'custom'" class="mb-3">
                            <label class="form-label">Custom Email</label>
                            <input type="email" v-model="generateTemplatesForm.custom_email" class="form-control" placeholder="Enter email address">
                        </div>
                        <div class="mb-3">
                            <label class="form-label">Template Types</label>
                            <div class="form-check">
                                <input class="form-check-input" type="checkbox" value="morningstar" v-model="generateTemplatesForm.template_types">
                                <label class="form-check-label">Morningstar Template</label>
                            </div>
                            <div class="form-check">
                                <input class="form-check-input" type="checkbox" value="six" v-model="generateTemplatesForm.template_types">
                                <label class="form-check-label">SIX Financial Template</label>
                            </div>
                        </div>
                        <div class="mb-3">
                            <label class="form-label">NAV Frequency Filters</label>
                            <div class="form-check">
                                <input class="form-check-input" type="checkbox" value="daily" v-model="generateTemplatesForm.isin_filters">
                                <label class="form-check-label">Daily Series</label>
                            </div>
                            <div class="form-check">
                                <input class="form-check-input" type="checkbox" value="weekly" v-model="generateTemplatesForm.isin_filters">
                                <label class="form-check-label">Weekly Series</label>
                            </div>
                            <div class="form-check">
                                <input class="form-check-input" type="checkbox" value="monthly" v-model="generateTemplatesForm.isin_filters">
                                <label class="form-check-label">Monthly Series</label>
                            </div>
                        </div>
                        <div class="mb-3">
                            <label class="form-label">Series Type Filter</label>
                            <div class="form-check">
                                <input class="form-check-input" type="radio" name="generateTemplatesSeriesType" value="" v-model="generateTemplatesForm.series_type" checked>
                                <label class="form-check-label">All Series Types</label>
                            </div>
                            <div class="form-check">
                                <input class="form-check-input" type="radio" name="generateTemplatesSeriesType" value="wrappers_hybrid" v-model="generateTemplatesForm.series_type">
                                <label class="form-check-label">Wrappers Hybrid Series</label>
                            </div>
                            <div class="form-check">
                                <input class="form-check-input" type="radio" name="generateTemplatesSeriesType" value="loan" v-model="generateTemplatesForm.series_type">
                                <label class="form-check-label">Loan Series</label>
                            </div>
                        </div>
                        <div class="mb-3">
                            <label class="form-label">Specific ISINs</label>
                            <div class="input-group mb-2">
                                <input type="text" v-model="newIsin" class="form-control" placeholder="Enter ISIN">
                                <button class="btn btn-outline-secondary" @click="addIsin('generateTemplates')">Add</button>
                            </div>
                            <div v-if="generateTemplatesForm.isins.length > 0" class="mt-2">
                                <div v-for="(isin, index) in generateTemplatesForm.isins" class="badge bg-secondary me-2 mb-2">
                                    {{ isin }}
                                    <button type="button" class="btn-close btn-close-white ms-2" 
                                            @click="removeIsin('generateTemplates', index)" aria-label="Close"></button>
                                </div>
                            </div>
                        </div>
                        <div class="mb-3">
                            <label class="form-label">Series Number</label>
                            <input type="text" v-model="generateTemplatesForm.series_number" class="form-control" placeholder="Enter Series Number">
                        </div>
                        <div v-if="loadingStates.generateTemplates" class="alert alert-info">
                            {{ progressMessages.generateTemplates }}
                        </div>
                    </div>
                    <div class="modal-footer">
                        <button class="btn btn-secondary" @click="showGenerateTemplates = false">Cancel</button>
                        <button class="btn btn-primary" @click="generateTemplates" :disabled="loadingStates.generateTemplates">
                            Generate Templates
                        </button>
                    </div>
                </div>
            </div>
        </div>

        <script src="https://cdn.jsdelivr.net/npm/vue@2.6.14"></script>
        <script src="https://cdn.jsdelivr.net/npm/axios/dist/axios.min.js"></script>
        <script>
            new Vue({
                el: '#app',
                data: {
                    navData: [],
                    currentPage: 1,
                    totalPages: 1,
                    perPage: 50,
                    filters: {
                        isin: '',
                        series_number: '',
                        startDate: '',
                        endDate: ''
                    },
                    showFetchNav: false,
                    showGenerateTemplates: false,
                    fetchNavForm: {
                        date_str: '',
                        isin_filters: [],
                        isins: [],
                        series_number: '',
                        series_type: ''
                    },
                    generateTemplatesForm: {
                        date_str: '',
                        distribution_type: 'morningstar',
                        custom_email: '',
                        isin_filters: [],
                        isins: [],
                        series_number: '',
                        template_types: ['morningstar'],
                        series_type: ''
                    },
                    activeTab: 'nav',
                    seriesData: [],
                    seriesFilters: {
                        isin: '',
                        series_number: '',
                        status: '',
                        region: ''
                    },
                    showSeriesDetails: false,
                    selectedSeriesDetails: null,
                    currentSeriesPage: 1,
                    totalSeriesPages: 1,
                    stakeholderData: null,
                    stakeholderFilters: {
                        identifier: ''
                    },
                    feeStructures: [],
                    feeFilters: {
                        isin: '',
                        series_number: '',
                        category: ''
                    },
                    currentFeePage: 1,
                    totalFeePages: 1,
                    selectedFile: null,
                    changeResults: null,
                    distributionEmails: {
                        morningstar: [
                            "productoperations@flexfunds.com",
                            "nav@morningstareurope.com"
                        ],
                        six: [
                            "data-qc-usa@six-financial-information.com",
                            "dc.us@telekurs.com",
                            "Darren.Smith@six-group.com",
                            "matthew.leventhal@six-group.com",
                            "dpsfixedincome.us@six-group.com",
                            "notification.us@six-financial-information.com",
                            "usdata.us@six-group.com",
                            "notification.eurobonds@six-financial-information.com",
                            "productoperations@flexfunds.com"
                        ]
                    },
                    newIsin: '',
                    snackbar: {
                        show: false,
                        message: '',
                        type: 'success'
                    },
                    loadingStates: {
                        fetchNav: false,
                        generateTemplates: false
                    },
                    progressMessages: {
                        fetchNav: '',
                        generateTemplates: ''
                    },
                    tradeData: [],
                    tradeSummary: null,
                    currentTradePage: 1,
                    totalTradePages: 1,
                    tradeFilters: {
                        series_number: '',
                        startDate: '',
                        endDate: '',
                        security_type: ''
                    },
                    tempFilePath: null,
                },
                computed: {
                    // Calculate which middle pages to show
                    middlePages() {
                        const delta = 2; // Number of pages to show on each side of current page
                        let pages = [];
                        
                        // Calculate range
                        let left = Math.max(2, this.currentPage - delta);
                        let right = Math.min(this.totalPages - 1, this.currentPage + delta);

                        // Adjust range if current page is near the edges
                        if (this.currentPage - delta <= 2) {
                            right = Math.min(this.totalPages - 1, 5);
                        }
                        if (this.currentPage + delta >= this.totalPages - 1) {
                            left = Math.max(2, this.totalPages - 4);
                        }

                        // Add pages in range
                        for (let i = left; i <= right; i++) {
                            pages.push(i);
                        }

                        return pages;
                    },
                    // Show left ellipsis if there's a gap between first page and middle pages
                    showLeftEllipsis() {
                        return this.middlePages.length > 0 && this.middlePages[0] > 2;
                    },
                    // Show right ellipsis if there's a gap between middle pages and last page
                    showRightEllipsis() {
                        return this.middlePages.length > 0 && 
                               this.middlePages[this.middlePages.length - 1] < this.totalPages - 1;
                    },
                    seriesMiddlePages() {
                        const delta = 2;
                        let pages = [];
                        let left = Math.max(2, this.currentSeriesPage - delta);
                        let right = Math.min(this.totalSeriesPages - 1, this.currentSeriesPage + delta);

                        if (this.currentSeriesPage - delta <= 2) {
                            right = Math.min(this.totalSeriesPages - 1, 5);
                        }
                        if (this.currentSeriesPage + delta >= this.totalSeriesPages - 1) {
                            left = Math.max(2, this.totalSeriesPages - 4);
                        }

                        for (let i = left; i <= right; i++) {
                            pages.push(i);
                        }
                        return pages;
                    },
                    showSeriesLeftEllipsis() {
                        return this.seriesMiddlePages.length > 0 && this.seriesMiddlePages[0] > 2;
                    },
                    showSeriesRightEllipsis() {
                        return this.seriesMiddlePages.length > 0 && 
                               this.seriesMiddlePages[this.seriesMiddlePages.length - 1] < this.totalSeriesPages - 1;
                    },
                    feePages() {
                        const delta = 2;
                        let pages = [];
                        let left = Math.max(2, this.currentFeePage - delta);
                        let right = Math.min(this.totalFeePages - 1, this.currentFeePage + delta);

                        if (this.currentFeePage - delta <= 2) {
                            right = Math.min(this.totalFeePages - 1, 5);
                        }
                        if (this.currentFeePage + delta >= this.totalFeePages - 1) {
                            left = Math.max(2, this.totalFeePages - 4);
                        }

                        for (let i = left; i <= right; i++) {
                            pages.push(i);
                        }
                        return pages;
                    },
                    tradeMiddlePages() {
                        const delta = 2;
                        let pages = [];
                        let left = Math.max(2, this.currentTradePage - delta);
                        let right = Math.min(this.totalTradePages - 1, this.currentTradePage + delta);

                        if (this.currentTradePage - delta <= 2) {
                            right = Math.min(this.totalTradePages - 1, 5);
                        }
                        if (this.currentTradePage + delta >= this.totalTradePages - 1) {
                            left = Math.max(2, this.totalTradePages - 4);
                        }

                        for (let i = left; i <= right; i++) {
                            pages.push(i);
                        }
                        return pages;
                    }
                },
                created() {
                    // Add default headers for all axios requests
                    axios.defaults.headers.common['X-API-Key'] = 'flexfundsetp';
                },
                methods: {
                    showSnackbar(message, type = 'success') {
                        this.snackbar = {
                            show: true,
                            message: message,
                            type: type
                        };
                        // Auto-hide after 5 seconds
                        setTimeout(() => {
                            this.hideSnackbar();
                        }, 5000);
                    },
                    hideSnackbar() {
                        this.snackbar.show = false;
                    },
                    loadData() {
                        const params = {
                            page: this.currentPage,
                            per_page: this.perPage,
                            isin: this.filters.isin || undefined,
                            series_number: this.filters.series_number || undefined,
                            start_date: this.filters.startDate || undefined,
                            end_date: this.filters.endDate || undefined
                        };

                        axios.get('/nav-data', { params })
                            .then(response => {
                                if (response.data.status === 'success') {
                                    this.navData = response.data.data;
                                    this.totalPages = response.data.pagination.total_pages;
                                } else {
                                    this.showSnackbar(response.data.message, 'error');
                                }
                            })
                            .catch(error => {
                                console.error('Error loading NAV data:', error);
                                this.showSnackbar('Error loading NAV data: ' + (error.response?.data?.message || error.message), 'error');
                            });
                    },
                    changePage(page) {
                        if (page >= 1 && page <= this.totalPages) {
                            this.currentPage = page;
                            this.loadData();
                        }
                    },
                    showFetchNavModal() {
                        this.showFetchNav = true;
                    },
                    showGenerateTemplatesModal() {
                        this.showGenerateTemplates = true;
                    },
                    fetchRemoteNavs() {
                        if (!this.fetchNavForm.date_str) {
                            this.showSnackbar('Please select a date', 'warning');
                            return;
                        }

                        // Set loading state
                        this.loadingStates.fetchNav = true;
                        this.progressMessages.fetchNav = 'Fetching NAV data...';

                        // Format date from YYYY-MM-DD to MMDDYYYY
                        const date = new Date(this.fetchNavForm.date_str + 'T12:00:00Z');
                        const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
                        const day = date.getUTCDate().toString().padStart(2, '0');
                        const year = date.getUTCFullYear();
                        const formattedDate = month + day + year;

                        const data = {
                            date_str: formattedDate,
                            isin_filters: this.fetchNavForm.isin_filters.length > 0 ? this.fetchNavForm.isin_filters : undefined,
                            isins: this.fetchNavForm.isins.length > 0 ? this.fetchNavForm.isins : undefined,
                            series_number: this.fetchNavForm.series_number || undefined,
                            series_type: this.fetchNavForm.series_type || undefined
                        };

                        axios.post('/fetch-remote-navs', data)
                            .then(response => {
                                // Load fresh data first before showing success message
                                this.loadData();
                                this.showSnackbar('Successfully fetched NAV data: ' + 
                                      response.data.stats.added + ' new entries added, ' +
                                      response.data.stats.duplicates + ' duplicates skipped');
                                this.showFetchNav = false;
                                this.fetchNavForm.isins = [];
                                this.fetchNavForm.isin_filters = [];
                                this.fetchNavForm.date_str = '';
                            })
                            .catch(error => {
                                this.showSnackbar('Error fetching NAV data: ' + error.message, 'error');
                            })
                            .finally(() => {
                                this.loadingStates.fetchNav = false;
                                this.progressMessages.fetchNav = '';
                            });
                    },
                    generateTemplates() {
                        if (!this.generateTemplatesForm.date_str) {
                            this.showSnackbar('Please select a date', 'warning');
                            return;
                        }

                        let emails = [];
                        if (this.generateTemplatesForm.distribution_type === 'custom') {
                            if (this.generateTemplatesForm.custom_email) {
                                emails = [this.generateTemplatesForm.custom_email];
                            } else {
                                this.showSnackbar('Please enter a custom email address', 'warning');
                                return;
                            }
                            
                            if (this.generateTemplatesForm.template_types.length === 0) {
                                this.showSnackbar('Please select at least one template type', 'warning');
                                return;
                            }
                        } else {
                            emails = this.distributionEmails[this.generateTemplatesForm.distribution_type] || [];
                        }

                        // Set loading state
                        this.loadingStates.generateTemplates = true;
                        this.progressMessages.generateTemplates = 'Generating templates and preparing emails...';

                        const date = new Date(this.generateTemplatesForm.date_str + 'T12:00:00Z');
                        const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
                        const day = date.getUTCDate().toString().padStart(2, '0');
                        const year = date.getUTCFullYear();
                        const formattedDate = month + day + year;

                        const data = {
                            date_str: formattedDate,
                            emails: emails,
                            isin_filters: this.generateTemplatesForm.isin_filters.length > 0 ? this.generateTemplatesForm.isin_filters : undefined,
                            isins: this.generateTemplatesForm.isins.length > 0 ? this.generateTemplatesForm.isins : undefined,
                            series_number: this.generateTemplatesForm.series_number || undefined,
                            template_types: this.generateTemplatesForm.template_types,
                            series_type: this.generateTemplatesForm.series_type || undefined
                        };

                        axios.post('/generate-templates', data)
                            .then(response => {
                                this.showSnackbar('Successfully generated templates and sent emails');
                                this.showGenerateTemplates = false;
                                this.generateTemplatesForm.isins = [];
                                this.generateTemplatesForm.isin_filters = [];
                                this.generateTemplatesForm.date_str = '';
                            })
                            .catch(error => {
                                this.showSnackbar('Error generating templates: ' + error.message, 'error');
                            })
                            .finally(() => {
                                this.loadingStates.generateTemplates = false;
                                this.progressMessages.generateTemplates = '';
                            });
                    },
                    loadSeriesData() {
                        const params = {
                            page: this.currentSeriesPage,
                            per_page: this.perPage,
                            isin: this.seriesFilters.isin || undefined,
                            series_number: this.seriesFilters.series_number || undefined,
                            status: this.seriesFilters.status || undefined,
                            region: this.seriesFilters.region || undefined
                        };

                        Object.keys(params).forEach(key => {
                            if (params[key] === undefined) {
                                delete params[key];
                            }
                        });

                        axios.get('/series', { params })
                            .then(response => {
                                if (response.data.status === 'success') {
                                    this.seriesData = response.data.data;
                                    this.totalSeriesPages = response.data.pagination.total_pages;
                                } else {
                                    this.showSnackbar(response.data.message, 'error');
                                }
                            })
                            .catch(error => {
                                console.error('Error loading series data:', error);
                                this.showSnackbar('Error loading series data: ' + error.message, 'error');
                            });
                    },
                    viewSeriesDetails(isin) {
                        axios.get(`/series/${isin}/details`)
                            .then(response => {
                                if (response.data.status === 'success') {
                                    this.selectedSeriesDetails = response.data.data;
                                    this.showSeriesDetails = true;
                                } else {
                                    this.showSnackbar(response.data.message, 'error');
                                }
                            })
                            .catch(error => {
                                console.error('Error loading series details:', error);
                                this.showSnackbar('Error loading series details: ' + error.message, 'error');
                            });
                    },
                    changeSeriesPage(page) {
                        if (page >= 1 && page <= this.totalSeriesPages) {
                            this.currentSeriesPage = page;
                            this.loadSeriesData();
                        }
                    },
                    clearSeriesFilters() {
                        this.seriesFilters = {
                            isin: '',
                            series_number: '',
                            status: '',
                            region: ''
                        };
                        this.currentSeriesPage = 1;
                        this.loadSeriesData();
                    },
                    getStatusBadgeClass(status) {
                        switch (status) {
                            case 'A': return 'bg-success';
                            case 'D': return 'bg-danger';
                            case 'MATURED': return 'bg-warning';
                            default: return 'bg-secondary';
                        }
                    },
                    formatDate(dateString) {
                        if (!dateString) return 'N/A';
                        return new Date(dateString).toLocaleDateString();
                    },
                    clearNavFilters() {
                        this.filters = {
                            isin: '',
                            series_number: '',
                            startDate: '',
                            endDate: ''
                        };
                        this.currentPage = 1;
                        this.loadData();
                    },
                    loadStakeholderData() {
                        if (!this.stakeholderFilters.identifier) {
                            this.showSnackbar('Please enter an ISIN or Series Number', 'warning');
                            return;
                        }

                        axios.get(`/series/${this.stakeholderFilters.identifier}/stakeholders`)
                            .then(response => {
                                if (response.data.status === 'success') {
                                    this.stakeholderData = response.data.data;
                                } else {
                                    this.showSnackbar(response.data.message, 'error');
                                }
                            })
                            .catch(error => {
                                console.error('Error loading stakeholder data:', error);
                                this.showSnackbar('Error loading stakeholder data: ' + (error.response?.data?.message || error.message), 'error');
                            });
                    },
                    clearStakeholderFilters() {
                        this.stakeholderFilters.identifier = '';
                        this.stakeholderData = null;
                    },
                    loadFeeStructures() {
                        const params = {
                            page: this.currentFeePage,
                            per_page: this.perPage,
                            category: this.feeFilters.category || undefined,
                            isin: this.feeFilters.isin || undefined,
                            series_number: this.feeFilters.series_number || undefined
                        };

                        axios.get('/fee-structures/summary', { params })
                            .then(response => {
                                if (response.data.status === 'success') {
                                    this.feeStructures = response.data.data;
                                    this.totalFeePages = response.data.pagination.total_pages;
                                } else {
                                    this.showSnackbar(response.data.message, 'error');
                                }
                            })
                            .catch(error => {
                                console.error('Error loading fee structures:', error);
                                this.showSnackbar('Error loading fee structures: ' + (error.response?.data?.message || error.message), 'error');
                            });
                    },
                    changeFeePage(page) {
                        if (page >= 1 && page <= this.totalFeePages) {
                            this.currentFeePage = page;
                            this.loadFeeStructures();
                        }
                    },
                    clearFeeFilters() {
                        this.feeFilters = {
                            isin: '',
                            series_number: '',
                            category: ''
                        };
                        this.currentFeePage = 1;
                        this.loadFeeStructures();
                    },
                    formatAUMThreshold(value) {
                        if (!value) return '-';
                        return value >= 1000000 
                            ? (value / 1000000).toFixed(2) + 'M' 
                            : value.toLocaleString();
                    },
                    formatStakeholderRole(role) {
                        // Convert snake_case to Title Case
                        return role.split('_')
                            .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
                            .join(' ');
                    },
                    handleFileSelect(event) {
                        this.selectedFile = event.target.files[0];
                        this.changeResults = null;
                    },
                    detectChanges() {
                        if (!this.selectedFile) return;

                        const formData = new FormData();
                        formData.append('file', this.selectedFile);

                        axios.post('/series-qualitative/changes', formData, {
                            headers: {
                                'Content-Type': 'multipart/form-data'
                            }
                        })
                        .then(response => {
                            if (response.data.status === 'success') {
                                this.changeResults = response.data.changes;
                            } else {
                                this.showSnackbar(response.data.message, 'error');
                            }
                        })
                        .catch(error => {
                            console.error('Error detecting changes:', error);
                            this.showSnackbar('Error detecting changes: ' + (error.response?.data?.message || error.message), 'error');
                        });
                    },
                    applyChanges() {
                        if (!this.changeResults) return;

                        if (!confirm('Are you sure you want to apply these changes? A backup will be created automatically.')) {
                            return;
                        }

                        // If we have a tempFilePath, use the confirm-update endpoint for Google Drive imports
                        if (this.tempFilePath) {
                            axios.post('/series-qualitative/confirm-update', {
                                file_path: this.tempFilePath
                            })
                            .then(response => {
                                if (response.data.status === 'success') {
                                    this.showSnackbar('Changes applied successfully');
                                    this.tempFilePath = null;
                                    this.changeResults = null;
                                } else {
                                    this.showSnackbar(response.data.message, 'error');
                                }
                            })
                            .catch(error => {
                                console.error('Error applying changes:', error);
                                this.showSnackbar('Error applying changes: ' + 
                                    (error.response?.data?.message || error.message), 'error');
                            });
                        } 
                        // Otherwise, use the update endpoint for manually uploaded files
                        else if (this.selectedFile) {
                            const formData = new FormData();
                            formData.append('file', this.selectedFile);

                            axios.post('/series-qualitative/update', formData, {
                                headers: {
                                    'Content-Type': 'multipart/form-data'
                                }
                            })
                            .then(response => {
                                if (response.data.status === 'success') {
                                    this.showSnackbar('Changes applied successfully');
                                    this.selectedFile = null;
                                    this.changeResults = null;
                                    this.$refs.fileInput.value = '';
                                } else {
                                    this.showSnackbar(response.data.message, 'error');
                                }
                            })
                            .catch(error => {
                                console.error('Error applying changes:', error);
                                this.showSnackbar('Error applying changes: ' + 
                                    (error.response?.data?.message || error.message), 'error');
                            });
                        }
                        else {
                            this.showSnackbar('No file to apply changes from', 'error');
                        }
                    },
                    getChangesByType(type) {
                        if (!this.changeResults) return [];
                        return this.changeResults.filter(change => change.change_type === type);
                    },
                    updateEmailList() {
                        if (this.generateTemplatesForm.distribution_type !== 'custom') {
                            // For non-custom distribution types, template type matches distribution type
                            this.generateTemplatesForm.template_types = [this.generateTemplatesForm.distribution_type];
                        } else {
                            // For custom distribution, reset template types to empty array
                            this.generateTemplatesForm.template_types = [];
                        }
                    },
                    addIsin(formType) {
                        if (this.newIsin.trim()) {
                            if (formType === 'fetchNav') {
                                this.fetchNavForm.isins.push(this.newIsin.trim());
                            } else if (formType === 'generateTemplates') {
                                this.generateTemplatesForm.isins.push(this.newIsin.trim());
                            }
                            this.newIsin = '';
                        }
                    },
                    
                    removeIsin(formType, index) {
                        if (formType === 'fetchNav') {
                            this.fetchNavForm.isins.splice(index, 1);
                        } else if (formType === 'generateTemplates') {
                            this.generateTemplatesForm.isins.splice(index, 1);
                        }
                    },
                    loadTradeData() {
                        const params = {
                            page: this.currentTradePage,
                            per_page: this.perPage,
                            series_number: this.tradeFilters.series_number || undefined,
                            start_date: this.tradeFilters.startDate || undefined,
                            end_date: this.tradeFilters.endDate || undefined,
                            security_type: this.tradeFilters.security_type || undefined
                        };

                        axios.get('/trades', { params })
                            .then(response => {
                                if (response.data.status === 'success') {
                                    this.tradeData = response.data.data;
                                    this.totalTradePages = response.data.pagination.total_pages;
                                } else {
                                    this.showSnackbar(response.data.message, 'error');
                                }
                            })
                            .catch(error => {
                                console.error('Error loading trade data:', error);
                                this.showSnackbar('Error loading trade data: ' + error.message, 'error');
                            });
                    },
                    loadTradeSummary() {
                        axios.get('/trades/summary')
                            .then(response => {
                                if (response.data.status === 'success') {
                                    this.tradeSummary = response.data.data;
                                } else {
                                    this.showSnackbar(response.data.message, 'error');
                                }
                            })
                            .catch(error => {
                                console.error('Error loading trade summary:', error);
                                this.showSnackbar('Error loading trade summary: ' + error.message, 'error');
                            });
                    },
                    changeTradePage(page) {
                        if (page >= 1 && page <= this.totalTradePages) {
                            this.currentTradePage = page;
                            this.loadTradeData();
                        }
                    },
                    clearTradeFilters() {
                        this.tradeFilters = {
                            series_number: '',
                            startDate: '',
                            endDate: '',
                            security_type: ''
                        };
                        this.currentTradePage = 1;
                        this.loadTradeData();
                    },
                    getTradeTypeBadgeClass(type) {
                        switch (type?.toUpperCase()) {
                            case 'BUY': return 'bg-success';
                            case 'SELL': return 'bg-danger';
                            default: return 'bg-secondary';
                        }
                    },
                    formatNumber(value) {
                        if (value === null || value === undefined) return '-';
                        return new Intl.NumberFormat('en-US', {
                            minimumFractionDigits: 2,
                            maximumFractionDigits: 2
                        }).format(value);
                    },
                    importFromGoogleDrive() {
                        if (!confirm('Are you sure you want to import the latest Series Qualitative Data file from Google Drive? A backup of the current master file will be created automatically.')) {
                            return;
                        }
                        
                        axios.post('/series-qualitative/import-from-drive', {})
                        .then(response => {
                            if (response.data.status === 'changes_detected') {
                                // Set the change results to display them
                                this.changeResults = response.data.changes;
                                
                                // Store the temporary file path for later confirmation
                                this.tempFilePath = response.data.file_path;
                                
                                this.showSnackbar('Changes detected from Google Drive. Please review and confirm.', 'success');
                            } else if (response.data.status === 'success') {
                                this.showSnackbar(response.data.message, 'success');
                                // Clear the file input and change results
                                this.selectedFile = null;
                                this.changeResults = null;
                                if (this.$refs.fileInput) {
                                    this.$refs.fileInput.value = '';
                                }
                            } else {
                                this.showSnackbar(response.data.message, 'error');
                            }
                        })
                        .catch(error => {
                            console.error('Error importing from Google Drive:', error);
                            this.showSnackbar(
                                'Error importing from Google Drive: ' + 
                                (error.response?.data?.message || error.message), 
                                'error'
                            );
                        });
                    },
                    confirmGoogleDriveUpdate() {
                        if (!this.tempFilePath) {
                            this.showSnackbar('No file to confirm', 'error');
                            return;
                        }
                        
                        if (!confirm('Are you sure you want to update the master file with these changes?')) {
                            return;
                        }
                        
                        axios.post('/series-qualitative/confirm-update', {
                            file_path: this.tempFilePath
                        })
                        .then(response => {
                            if (response.data.status === 'success') {
                                this.showSnackbar('Master file updated successfully', 'success');
                                // Clear the temp file path and change results
                                this.tempFilePath = null;
                                this.changeResults = null;
                            } else {
                                this.showSnackbar(response.data.message, 'error');
                            }
                        })
                        .catch(error => {
                            console.error('Error confirming update:', error);
                            this.showSnackbar(
                                'Error confirming update: ' + 
                                (error.response?.data?.message || error.message), 
                                'error'
                            );
                        });
                    },
                },
                mounted() {
                    this.loadData();
                },
                watch: {
                    activeTab(newTab) {
                        if (newTab === 'nav') {
                            this.loadData();
                        } else if (newTab === 'series') {
                            this.loadSeriesData();
                        } else if (newTab === 'fees') {
                            this.loadFeeStructures();
                        } else if (newTab === 'trades') {
                            this.loadTradeData();
                            this.loadTradeSummary();
                        }
                    },
                    'filters.isin'() {
                        this.currentPage = 1;
                        this.loadData();
                    },
                    'filters.series_number'() {
                        this.currentPage = 1;
                        this.loadData();
                    },
                    'filters.startDate'() {
                        this.currentPage = 1;
                        this.loadData();
                    },
                    'filters.endDate'() {
                        this.currentPage = 1;
                        this.loadData();
                    },
                    'seriesFilters.isin'() {
                        this.currentSeriesPage = 1;
                        this.loadSeriesData();
                    },
                    'seriesFilters.series_number'() {
                        this.currentSeriesPage = 1;
                        this.loadSeriesData();
                    },
                    'seriesFilters.status'() {
                        this.currentSeriesPage = 1;
                        this.loadSeriesData();
                    },
                    'seriesFilters.region'() {
                        this.currentSeriesPage = 1;
                        this.loadSeriesData();
                    },
                    'tradeFilters.series_number'() {
                        this.currentTradePage = 1;
                        this.loadTradeData();
                    },
                    'tradeFilters.startDate'() {
                        this.currentTradePage = 1;
                        this.loadTradeData();
                    },
                    'tradeFilters.endDate'() {
                        this.currentTradePage = 1;
                        this.loadTradeData();
                    },
                    'tradeFilters.security_type'() {
                        this.currentTradePage = 1;
                        this.loadTradeData();
                    }
                }
            });
        </script>
    </body>
    </html>
    """


@app.errorhandler(Exception)
def handle_error(error):
    """Global error handler"""
    response = {
        'status': 'error',
        'message': str(error),
        'type': error.__class__.__name__
    }

    if app.debug:
        response['traceback'] = traceback.format_exc()

    return jsonify(response), 500


@app.route('/trades', methods=['GET'])
@require_api_key
def get_trades():
    """Get paginated trade data with filtering options"""
    try:
        # Get query parameters
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        series_number = request.args.get('series_number')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        security_type = request.args.get('security_type')
        trade_type = request.args.get('trade_type')

        # Convert dates if provided
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d')

        # Use our reliable session helper
        session = get_reliable_session()
        try:
            query = session.query(Trade)

            # Apply filters
            if series_number:
                query = query.filter(Trade.series_number == series_number)
            if start_date:
                query = query.filter(Trade.trade_date >= start_date)
            if end_date:
                query = query.filter(Trade.trade_date <= end_date)
            if security_type:
                query = query.filter(Trade.security_type == security_type)
            if trade_type:
                query = query.filter(Trade.trade_type == trade_type)

            # Get total count
            total = query.count()

            # Apply pagination
            trades = query.order_by(Trade.trade_date.desc())\
                .offset((page - 1) * per_page)\
                .limit(per_page)\
                .all()

            # Convert trades to safe dictionaries
            trade_data = []
            for trade in trades:
                # Safely get attributes
                trade_dict = {
                    'id': getattr(trade, 'id', None),
                    'series_number': getattr(trade, 'series_number', None),
                    'trade_date': getattr(trade, 'trade_date', None).strftime('%Y-%m-%d') if getattr(trade, 'trade_date', None) else None,
                    'trade_type': getattr(trade, 'trade_type', None),
                    'security_type': getattr(trade, 'security_type', None),
                    'security_name': getattr(trade, 'security_name', None),
                    'security_id': getattr(trade, 'security_id', None),
                    'quantity': getattr(trade, 'quantity', None),
                    'price': getattr(trade, 'price', None),
                    'currency': getattr(trade, 'currency', None),
                    'trade_value': getattr(trade, 'trade_value', None),
                    'broker': getattr(trade, 'broker', None),
                    'account': getattr(trade, 'account', None),
                    'source_folder': getattr(trade, 'source_folder', None)
                }

                # Handle nullable date fields
                settlement_date = getattr(trade, 'settlement_date', None)
                if settlement_date:
                    trade_dict['settlement_date'] = settlement_date.strftime(
                        '%Y-%m-%d')
                else:
                    trade_dict['settlement_date'] = None

                # Special handling for source_file attribute which might be missing in some DB versions
                try:
                    trade_dict['source_file'] = trade.source_file
                except (AttributeError, KeyError):
                    trade_dict['source_file'] = None

                trade_data.append(trade_dict)

            response = {
                'status': 'success',
                'data': trade_data,
                'pagination': {
                    'current_page': page,
                    'per_page': per_page,
                    'total_pages': math.ceil(total / per_page),
                    'total_entries': total
                }
            }

            return jsonify(response), 200
        finally:
            session.close()

    except Exception as e:
        error_traceback = traceback.format_exc()
        print(f"Error in get_trades: {str(e)}")
        print(f"Traceback: {error_traceback}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'traceback': error_traceback
        }), 500


@app.route('/trades/summary', methods=['GET'])
@require_api_key
def get_trades_summary():
    """Get summary statistics about trades"""
    try:
        # Use our reliable session helper
        session = get_reliable_session()
        try:
            # Get total trades
            total_trades = session.query(Trade).count()

            # Get trades by source folder
            try:
                trades_by_folder = session.query(
                    Trade.source_folder,
                    func.count(Trade.id)
                ).group_by(Trade.source_folder).all()

                folder_data = [
                    {'folder': folder or 'Unknown', 'count': count}
                    for folder, count in trades_by_folder
                ]
            except Exception as folder_error:
                print(f"Error getting trades by folder: {str(folder_error)}")
                folder_data = [
                    {'folder': 'All Folders', 'count': total_trades}]

            # Get trades by security type
            try:
                trades_by_security_type = session.query(
                    Trade.security_type,
                    func.count(Trade.id)
                ).group_by(Trade.security_type).all()

                security_type_data = [
                    {'type': type_ or 'Unknown', 'count': count}
                    for type_, count in trades_by_security_type
                ]
            except Exception as type_error:
                print(
                    f"Error getting trades by security type: {str(type_error)}")
                security_type_data = [
                    {'type': 'All Types', 'count': total_trades}]

            # Get date range
            try:
                date_range = session.query(
                    func.min(Trade.trade_date),
                    func.max(Trade.trade_date)
                ).first()

                date_range_data = {
                    'earliest': date_range[0].strftime('%Y-%m-%d') if date_range[0] else None,
                    'latest': date_range[1].strftime('%Y-%m-%d') if date_range[1] else None
                }
            except Exception as date_error:
                print(f"Error getting trade date range: {str(date_error)}")
                date_range_data = {
                    'earliest': None,
                    'latest': None
                }

            response = {
                'status': 'success',
                'data': {
                    'total_trades': total_trades,
                    'trades_by_folder': folder_data,
                    'trades_by_security_type': security_type_data,
                    'date_range': date_range_data
                }
            }

            return jsonify(response), 200
        finally:
            session.close()

    except Exception as e:
        error_traceback = traceback.format_exc()
        print(f"Error in get_trades_summary: {str(e)}")
        print(f"Traceback: {error_traceback}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'traceback': error_traceback
        }), 500


@app.route('/series-qualitative/import-from-drive', methods=['POST'])
@require_api_key
def import_series_qualitative_from_drive():
    """Import the most recent Series Qualitative Data file from Google Drive"""
    try:
        data = request.get_json() or {}

        # Hard code the Google Drive folder ID
        folder_id = '1DVlLOzaKQJytWf1IaE0QQy1mQfFjPmCY'

        # Get specific file ID if provided
        file_id = data.get('file_id')

        # Initialize change detector with a function to get a session rather than SessionMaker
        master_file_path = os.path.join(os.path.dirname(
            __file__), 'input', 'template', 'Series Qualitative Data.xlsx')

        # Create a session provider function that returns a new session
        def session_provider():
            return get_reliable_session()

        detector = SeriesChangeDetector(
            master_file_path, session_provider)

        # Get credentials path from environment or drive_config
        credentials_path = drive_config.get(
            'credentials_path') or os.getenv('GOOGLE_DRIVE_CREDENTIALS_PATH')

        if not credentials_path:
            return jsonify({
                'status': 'error',
                'message': 'Google Drive credentials not configured'
            }), 500

        # Import from Google Drive
        result = detector.import_from_google_drive(
            credentials_path=credentials_path,
            folder_id=folder_id,
            file_id=file_id,
            backup=True  # Always create a backup
        )

        # Based on the new flow, we expect a 'changes_detected' status instead of 'success'
        if result['status'] == 'changes_detected':
            # Return the changes for review - this will be shown in the UI
            response = {
                'status': 'changes_detected',
                'message': result['message'],
                'changes': result.get('changes', []),
                'change_report': result.get('change_report', ''),
                'file_path': result.get('file_path', ''),
                'file_name': result.get('file_name', '')
            }
            return jsonify(response), 200
        else:
            return jsonify(result), 400

    except Exception as e:
        error_traceback = traceback.format_exc()
        print(f"Error in import_series_qualitative_from_drive: {str(e)}")
        print(f"Traceback: {error_traceback}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'traceback': error_traceback
        }), 500


@app.route('/series-qualitative/confirm-update', methods=['POST'])
@require_api_key
def confirm_series_qualitative_update():
    """Confirm the update after reviewing changes from a Google Drive import"""
    try:
        data = request.get_json() or {}

        # Get the temporary file path from the request
        temp_file_path = data.get('file_path')

        if not temp_file_path:
            return jsonify({
                'status': 'error',
                'message': 'No file path provided. Please provide the temporary file path.'
            }), 400

        if not os.path.exists(temp_file_path):
            return jsonify({
                'status': 'error',
                'message': f'Temporary file not found: {temp_file_path}'
            }), 404

        # Initialize change detector
        master_file_path = os.path.join(os.path.dirname(
            __file__), 'input', 'template', 'Series Qualitative Data.xlsx')

        # Create a session provider function that returns a new session
        def session_provider():
            return get_reliable_session()

        detector = SeriesChangeDetector(
            master_file_path, session_provider)

        # Confirm the update
        result = detector.confirm_update(
            temp_file_path=temp_file_path,
            backup=True  # Always create a backup
        )

        if result['status'] == 'success':
            return jsonify(result), 200
        else:
            return jsonify(result), 400

    except Exception as e:
        error_traceback = traceback.format_exc()
        print(f"Error in confirm_series_qualitative_update: {str(e)}")
        print(f"Traceback: {error_traceback}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'traceback': error_traceback
        }), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8080)))
