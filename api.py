from flask import Flask, request, jsonify
from nav_processor import NAVProcessor
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from functools import wraps
from models import Series, SeriesStatus, FeeStructure
from series_change_detector import SeriesChangeDetector
import pandas as pd
import math
import traceback

app = Flask(__name__)

# Configure timeouts
app.config['TIMEOUT'] = 300  # 5 minutes timeout

# Load environment variables
load_dotenv()


def get_previous_business_day():
    """Get the previous business day date string in MMDDYYYY format"""
    today = pd.Timestamp.now()
    prev_business_day = today

    while True:
        prev_business_day = prev_business_day - pd.Timedelta(days=1)
        if prev_business_day.dayofweek < 5:  # Monday = 0, Friday = 4
            break

    return prev_business_day.strftime('%m%d%Y')


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


processor = NAVProcessor(
    mode="remote",
    ftp_configs=ftp_configs,
    smtp_config=smtp_config,
    drive_config=drive_config
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
        nav_entries = processor.db_service.get_nav_history(
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

        print(f"Processing NAVs for date: {date_str}")
        print(f"Filter types: {isin_filters}")
        print(f"Specific ISINs: {specific_isins}")
        print(f"Series number: {series_number}")

        # If specific ISINs or series number is provided, use that as target
        target_isins = None
        if specific_isins or series_number:
            with processor.db_service.SessionMaker() as session:
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
        else:
            # Get target ISINs for each filter type and combine them
            target_isins = set()
            for filter_type in isin_filters:
                filter_isins = processor._get_target_isins(filter_type)
                target_isins.update(filter_isins)
            target_isins = list(target_isins)

        # Process NAVs without sending email or generating templates
        nav_dfs = processor._process_nav_files(
            date_str=date_str,
            target_isins=target_isins
        )

        # Save to database
        try:
            total_added, total_duplicates, total_invalids = processor._save_to_database(
                nav_dfs, 'morningstar')

            return jsonify({
                'status': 'success',
                'message': f'Successfully processed NAV files',
                'stats': {
                    'added': total_added,
                    'duplicates': total_duplicates,
                    'invalids': total_invalids
                },
                'date_processed': date_str,
                'filters_applied': {
                    'filter_types': isin_filters,
                    'specific_isins': specific_isins,
                    'series_number': series_number
                }
            }), 200

        except Exception as e:
            # Log the error but don't treat it as a failure
            print(
                f"Note: Some NAV entries were duplicates or invalid: {str(e)}")
            return jsonify({
                'status': 'success',
                'message': 'Processed NAV files with some duplicates/invalid entries',
                'stats': {
                    'added': 0,
                    'duplicates': 'unknown',
                    'invalids': 'unknown'
                },
                'date_processed': date_str,
                'filters_applied': {
                    'filter_types': isin_filters,
                    'specific_isins': specific_isins,
                    'series_number': series_number
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
        # Changed to accept array of filters
        isin_filters = data.get('isin_filters', [])
        specific_isins = data.get('isins', [])
        series_number = data.get('series_number')

        # Get template types (default to both)
        template_types = data.get('template_types', ['morningstar', 'six'])

        print(f"Generating templates for date: {date_str}")
        print(f"Sending to emails: {emails}")
        print(f"Filter types: {isin_filters}")
        print(f"Specific ISINs: {specific_isins}")
        print(f"Series number: {series_number}")
        print(f"Template types: {template_types}")

        # If specific ISINs or series number is provided, use that as target
        isin_filter_value = None
        if specific_isins or series_number:
            with processor.db_service.SessionMaker() as session:
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
        else:
            # Get target ISINs for each filter type and combine them
            target_isins = set()
            for filter_type in isin_filters:
                filter_isins = processor._get_target_isins(filter_type)
                target_isins.update(filter_isins)
            isin_filter_value = list(target_isins)

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
                'series_number': series_number
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
        with processor.db_service.SessionMaker() as session:
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

    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
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
        nav_entries = processor.db_service.get_nav_history(
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
        with processor.db_service.SessionMaker() as session:
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

    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/series/<identifier>/stakeholders', methods=['GET'])
@require_api_key
def get_series_stakeholders(identifier):
    """Get all stakeholders associated with a specific series by ISIN or series number"""
    try:
        with processor.db_service.SessionMaker() as session:
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

    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/series/<identifier>/fee-structures', methods=['GET'])
@require_api_key
def get_series_fee_structures(identifier):
    """Get all fee structures associated with a specific series by ISIN or series number"""
    try:
        with processor.db_service.SessionMaker() as session:
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

    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
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

        with processor.db_service.SessionMaker() as session:
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

    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/statistics', methods=['GET'])
@require_api_key
def get_statistics():
    """Get overall statistics about the NAV data"""
    try:
        with processor.db_service.SessionMaker() as session:
            # Get series statistics
            total_series = session.query(Series).count()
            active_series = session.query(Series).filter(
                Series.status == SeriesStatus.ACTIVE).count()

            # Get NAV statistics
            nav_stats = processor.db_service.verify_nav_entries()

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

    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
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

        # Initialize change detector with master file and session maker
        master_file_path = os.path.join(os.path.dirname(
            __file__), 'input', 'template', 'Series Qualitative Data.xlsx')
        detector = SeriesChangeDetector(
            master_file_path, processor.db_service.SessionMaker)

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
        detector = SeriesChangeDetector(
            master_file_path, processor.db_service.SessionMaker)
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

                            <!-- Update Master File Button -->
                            <div class="mt-4">
                                <button class="btn btn-success" @click="updateMasterFile" :disabled="!selectedFile">
                                    Update Master File
                                </button>
                                <small class="text-muted ms-2">
                                    This will update the master file with the new changes. A backup will be created automatically.
                                </small>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Fetch NAV Modal -->
            <div v-if="showFetchNav" class="modal-mask">
                <div class="modal-container">
                    <div class="modal-header">
                        <h4 class="mb-0">Fetch Remote NAVs</h4>
                    </div>
                    <div class="modal-body">
                        <div v-if="loadingStates.fetchNav" class="text-center mb-4">
                            <div class="spinner-border text-primary" role="status">
                                <span class="visually-hidden">Loading...</span>
                            </div>
                            <div class="mt-2">{{ progressMessages.fetchNav }}</div>
                        </div>
                        <div v-else>
                            <div class="form-group mb-3">
                                <label>Date</label>
                                <input type="date" v-model="fetchNavForm.date_str" class="form-control" required>
                            </div>
                            <div class="form-group mb-3">
                                <label>Filter Types</label>
                                <div class="form-check">
                                    <input type="checkbox" v-model="fetchNavForm.isin_filters" value="daily" class="form-check-input" id="dailyFilter">
                                    <label class="form-check-label" for="dailyFilter">Daily</label>
                                </div>
                                <div class="form-check">
                                    <input type="checkbox" v-model="fetchNavForm.isin_filters" value="weekly" class="form-check-input" id="weeklyFilter">
                                    <label class="form-check-label" for="weeklyFilter">Weekly</label>
                                </div>
                                <div class="form-check">
                                    <input type="checkbox" v-model="fetchNavForm.isin_filters" value="monthly" class="form-check-input" id="monthlyFilter">
                                    <label class="form-check-label" for="monthlyFilter">Monthly</label>
                                </div>
                            </div>
                            <div class="form-group mb-3">
                                <label>ISINs</label>
                                <div class="input-group mb-2">
                                    <input type="text" v-model="newIsin" class="form-control" placeholder="Enter ISIN">
                                    <button class="btn btn-outline-secondary" type="button" @click="addIsin('fetchNav')">Add</button>
                                </div>
                                <div class="isin-list">
                                    <div v-for="(isin, index) in fetchNavForm.isins" class="badge bg-primary me-2 mb-2">
                                        {{ isin }}
                                        <button class="btn-close btn-close-white ms-1" @click="removeIsin('fetchNav', index)"></button>
                                    </div>
                                </div>
                            </div>
                            <div class="form-group mb-0">
                                <label>Series Number</label>
                                <input type="text" v-model="fetchNavForm.series_number" class="form-control" placeholder="Enter specific series number">
                            </div>
                        </div>
                    </div>
                    <div class="modal-footer">
                        <button class="btn btn-secondary" @click="showFetchNav = false" :disabled="loadingStates.fetchNav">Cancel</button>
                        <button class="btn btn-primary" @click="fetchRemoteNavs" :disabled="loadingStates.fetchNav">
                            <span v-if="loadingStates.fetchNav">
                                <span class="spinner-border spinner-border-sm me-1" role="status"></span>
                                Processing...
                            </span>
                            <span v-else>Fetch NAVs</span>
                        </button>
                    </div>
                </div>
            </div>

            <!-- Generate Templates Modal -->
            <div v-if="showGenerateTemplates" class="modal-mask">
                <div class="modal-container">
                    <div class="modal-header">
                        <h4 class="mb-0">Generate Templates</h4>
                    </div>
                    <div class="modal-body">
                        <div v-if="loadingStates.generateTemplates" class="text-center mb-4">
                            <div class="spinner-border text-primary" role="status">
                                <span class="visually-hidden">Loading...</span>
                            </div>
                            <div class="mt-2">{{ progressMessages.generateTemplates }}</div>
                        </div>
                        <div v-else>
                            <div class="form-group mb-3">
                                <label>Date</label>
                                <input type="date" v-model="generateTemplatesForm.date_str" class="form-control" required>
                            </div>
                            <div class="form-group mb-3">
                                <label>Distribution Type</label>
                                <select v-model="generateTemplatesForm.distribution_type" class="form-control" @change="updateEmailList">
                                    <option value="morningstar">Morningstar</option>
                                    <option value="six">SIX</option>
                                    <option value="custom">Custom Email</option>
                                </select>
                            </div>
                            <div v-if="generateTemplatesForm.distribution_type === 'custom'" class="form-group mb-3">
                                <label>Custom Email</label>
                                <input type="email" v-model="generateTemplatesForm.custom_email" class="form-control" placeholder="Enter email address">
                            </div>
                            <div v-else class="form-group mb-3">
                                <label>Distribution Emails</label>
                                <div class="alert alert-info py-2">
                                    <small>Emails will be sent to the predefined distribution list for {{ generateTemplatesForm.distribution_type }}</small>
                                </div>
                            </div>
                            <div class="form-group mb-3">
                                <label>Filter Types</label>
                                <div class="form-check">
                                    <input type="checkbox" v-model="generateTemplatesForm.isin_filters" value="daily" class="form-check-input" id="dailyFilterGen">
                                    <label class="form-check-label" for="dailyFilterGen">Daily</label>
                                </div>
                                <div class="form-check">
                                    <input type="checkbox" v-model="generateTemplatesForm.isin_filters" value="weekly" class="form-check-input" id="weeklyFilterGen">
                                    <label class="form-check-label" for="weeklyFilterGen">Weekly</label>
                                </div>
                                <div class="form-check">
                                    <input type="checkbox" v-model="generateTemplatesForm.isin_filters" value="monthly" class="form-check-input" id="monthlyFilterGen">
                                    <label class="form-check-label" for="monthlyFilterGen">Monthly</label>
                                </div>
                            </div>
                            <div class="form-group mb-3">
                                <label>ISINs</label>
                                <div class="input-group mb-2">
                                    <input type="text" v-model="newIsin" class="form-control" placeholder="Enter ISIN">
                                    <button class="btn btn-outline-secondary" type="button" @click="addIsin('generateTemplates')">Add</button>
                                </div>
                                <div class="isin-list">
                                    <div v-for="(isin, index) in generateTemplatesForm.isins" class="badge bg-primary me-2 mb-2">
                                        {{ isin }}
                                        <button class="btn-close btn-close-white ms-1" @click="removeIsin('generateTemplates', index)"></button>
                                    </div>
                                </div>
                            </div>
                            <div class="form-group" :class="{ 'mb-0': generateTemplatesForm.distribution_type !== 'custom' }">
                                <label>Series Number</label>
                                <input type="text" v-model="generateTemplatesForm.series_number" class="form-control" placeholder="Enter specific series number">
                            </div>
                            <div v-if="generateTemplatesForm.distribution_type === 'custom'" class="form-group mb-0">
                                <label>Template Type</label>
                                <div class="form-check">
                                    <input type="checkbox" v-model="generateTemplatesForm.template_types" value="morningstar" class="form-check-input" id="morningstarTemplate">
                                    <label class="form-check-label" for="morningstarTemplate">Morningstar</label>
                                </div>
                                <div class="form-check">
                                    <input type="checkbox" v-model="generateTemplatesForm.template_types" value="six" class="form-check-input" id="sixTemplate">
                                    <label class="form-check-label" for="sixTemplate">SIX</label>
                                </div>
                            </div>
                        </div>
                    </div>
                    <div class="modal-footer">
                        <button class="btn btn-secondary" @click="showGenerateTemplates = false" :disabled="loadingStates.generateTemplates">Cancel</button>
                        <button class="btn btn-primary" @click="generateTemplates" :disabled="loadingStates.generateTemplates">
                            <span v-if="loadingStates.generateTemplates">
                                <span class="spinner-border spinner-border-sm me-1" role="status"></span>
                                Processing...
                            </span>
                            <span v-else>Generate</span>
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
                        series_number: ''
                    },
                    generateTemplatesForm: {
                        date_str: '',
                        distribution_type: 'morningstar',
                        custom_email: '',
                        isin_filters: [],
                        isins: [],
                        series_number: '',
                        template_types: ['morningstar']
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
                    }
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
                            series_number: this.fetchNavForm.series_number || undefined
                        };

                        axios.post('/fetch-remote-navs', data)
                            .then(response => {
                                this.showSnackbar('Successfully fetched NAV data: ' + 
                                      response.data.stats.added + ' new entries added, ' +
                                      response.data.stats.duplicates + ' duplicates skipped');
                                this.loadData();
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
                            template_types: this.generateTemplatesForm.template_types
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
                    updateMasterFile() {
                        if (!this.selectedFile) return;

                        if (!confirm('Are you sure you want to update the master file? A backup will be created automatically.')) {
                            return;
                        }

                        const formData = new FormData();
                        formData.append('file', this.selectedFile);

                        axios.post('/series-qualitative/update', formData, {
                            headers: {
                                'Content-Type': 'multipart/form-data'
                            }
                        })
                        .then(response => {
                            if (response.data.status === 'success') {
                                this.showSnackbar('Master file updated successfully');
                                this.selectedFile = null;
                                this.changeResults = null;
                                this.$refs.fileInput.value = '';
                            } else {
                                this.showSnackbar(response.data.message, 'error');
                            }
                        })
                        .catch(error => {
                            console.error('Error updating master file:', error);
                            this.showSnackbar('Error updating master file: ' + (error.response?.data?.message || error.message), 'error');
                        });
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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8080)))
