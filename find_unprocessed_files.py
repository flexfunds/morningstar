#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script to find and optionally process unprocessed BNY trade files

Requirements:
    - pandas
    - sqlalchemy
    - xlsxwriter (for Excel output formatting)

Install with: pip install pandas sqlalchemy xlsxwriter
"""

import os
import pandas as pd
import argparse
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, distinct, func
from sqlalchemy.orm import sessionmaker
from models import Trade, Base
from process_bny_trades import BNYTradeProcessor


def analyze_excel_structure(file_path):
    """
    Analyze an Excel file to determine its structure (table start positions, header rows, etc.)

    Args:
        file_path: Path to the Excel file

    Returns:
        dict: Information about the file structure
    """
    try:
        # Special handling for S462 folder files which may have single row tables
        if 'S462' in file_path:
            # First check if this is a special file format
            try:
                # Read the entire file
                all_sheets = pd.read_excel(
                    file_path, sheet_name=None, header=None)

                # For each sheet, look for key trade information
                for sheet_name, df in all_sheets.items():
                    # Look for cells containing 'BUY' or 'SELL'
                    buy_sell_found = False
                    isin_found = False
                    qty_found = False
                    buy_sell_row = None
                    buy_sell_col = None

                    # First, search for key terms
                    for idx, row in df.iterrows():
                        for col_idx, cell in enumerate(row):
                            if pd.notna(cell) and isinstance(cell, str):
                                cell_upper = cell.upper().strip()
                                if cell_upper in ('BUY', 'SELL'):
                                    buy_sell_found = True
                                    buy_sell_row = idx
                                    buy_sell_col = col_idx
                                    break
                                # Check for ISIN/CUSIP values (usually alphanumeric with specific lengths)
                                elif (cell_upper.startswith('US') or cell_upper.startswith('IE')) and len(cell_upper) >= 10:
                                    isin_found = True
                                # Check for quantity - numeric values above 1000 are likely to be quantities
                                elif isinstance(cell, (int, float)) and cell > 1000:
                                    qty_found = True
                        if buy_sell_found and (isin_found or qty_found):
                            break

                    # If we found key elements, construct a synthetic table layout
                    if buy_sell_found and (isin_found or qty_found):
                        result = {
                            'file_path': file_path,
                            'filename': os.path.basename(file_path),
                            'tables': [{
                                'header_row': max(0, buy_sell_row - 1),
                                'column_offset': buy_sell_col,
                                'key_columns': {},
                                'data_rows': 1  # Assume at least one data row
                            }],
                            'structure_type': 'single_row_table',
                            'analysis_success': True,
                            'processable': True,
                            'processing_approach': 'special_single_row'
                        }
                        return result
            except Exception as e:
                print(f"Special S462 file format analysis failed: {str(e)}")
                # Continue with regular analysis if special handling fails

        # Regular file analysis
        # Read the Excel file without headers first
        df = pd.read_excel(file_path, header=None)

        # Initialize result
        result = {
            'file_path': file_path,
            'filename': os.path.basename(file_path),
            'tables': [],
            'structure_type': 'unknown',
            'analysis_success': False
        }

        # Look for potential header rows containing 'BUY/SELL'
        header_rows = []
        for idx, row in df.iterrows():
            for cell in row:
                if isinstance(cell, str) and 'BUY/SELL' in cell.upper():
                    header_rows.append(idx)
                    break

        if header_rows:
            result['header_rows'] = header_rows
            result['structure_type'] = 'multiple_tables' if len(
                header_rows) > 1 else 'single_table'

            # For each header row, determine the column offset
            for header_idx in header_rows:
                header_row = df.iloc[header_idx]

                # Find the column where BUY/SELL appears
                buy_sell_col = None
                for col_idx, cell in enumerate(header_row):
                    if isinstance(cell, str) and 'BUY/SELL' in cell.upper():
                        buy_sell_col = col_idx
                        break

                if buy_sell_col is not None:
                    # Find other key columns in this header row
                    columns = {}
                    for col_idx, cell in enumerate(header_row):
                        if pd.notna(cell) and isinstance(cell, str):
                            columns[cell.upper().strip()] = col_idx

                    table_info = {
                        'header_row': header_idx,
                        'column_offset': buy_sell_col,
                        'key_columns': columns
                    }

                    # Try to determine the number of data rows in this table
                    data_rows = 0
                    curr_row = header_idx + 1
                    while curr_row < len(df):
                        if pd.notna(df.iloc[curr_row, buy_sell_col]) and isinstance(df.iloc[curr_row, buy_sell_col], str):
                            cell_val = df.iloc[curr_row,
                                               buy_sell_col].upper().strip()
                            if cell_val in ('BUY', 'SELL'):
                                data_rows += 1
                                curr_row += 1
                            else:
                                break
                        else:
                            break

                    table_info['data_rows'] = data_rows
                    result['tables'].append(table_info)

            result['analysis_success'] = True

            # All files are considered processable with the new adaptive approach
            result['processable'] = True

            # Add processing approach info
            if result['structure_type'] == 'multiple_tables':
                result['processing_approach'] = "adaptive_multiple_tables"
            else:
                # Single table but may have different column offset
                result['processing_approach'] = "adaptive_single_table"

        return result

    except Exception as e:
        return {
            'file_path': file_path,
            'filename': os.path.basename(file_path),
            'error': str(e),
            'analysis_success': False,
            'processable': False
        }


def process_file_adaptive(file_path, db_path='sqlite:///nav_data.db'):
    """
    Process a file adaptively, handling different table formats automatically

    Args:
        file_path: Path to the Excel file
        db_path: Path to the database

    Returns:
        tuple: (success, count, error_message)
    """
    try:
        # First analyze the file structure
        structure_info = analyze_excel_structure(file_path)

        if not structure_info['analysis_success'] or not structure_info['tables']:
            return False, 0, "Could not analyze file structure or no valid tables found"

        # Extract series number from file path
        series_number = None
        path_parts = file_path.split(os.sep)
        for part in path_parts:
            if part.startswith('S') and part[1:].isdigit():
                series_number = part[1:]  # Remove the 'S' prefix
                break

        if not series_number:
            return False, 0, "Could not extract series number from file path"

        # Initialize the trade processor
        processor = BNYTradeProcessor(db_path=db_path)
        processor.current_file = file_path

        # Initialize an empty list to store trade objects
        trades = []

        # Special handling for S462 folder single row tables
        if 'S462' in file_path and structure_info.get('structure_type') == 'single_row_table':
            try:
                # Read all sheets
                all_sheets = pd.read_excel(
                    file_path, sheet_name=None, header=None)

                for sheet_name, df in all_sheets.items():
                    # Create a new trade object
                    trade = Trade()

                    # Look for trade information
                    buy_sell = None
                    isin = None
                    qty = None
                    trade_date = None
                    settle_date = None
                    value = None

                    # Scan the entire sheet for key data
                    for idx, row in df.iterrows():
                        for col_idx, cell in enumerate(row):
                            if pd.isna(cell):
                                continue

                            # Check cell content
                            if isinstance(cell, str):
                                cell_upper = cell.upper().strip()

                                # Trade type
                                if cell_upper in ('BUY', 'SELL'):
                                    buy_sell = cell_upper

                                # ISIN/CUSIP
                                elif (cell_upper.startswith('US') or cell_upper.startswith('IE')) and len(cell_upper) >= 10:
                                    isin = cell_upper

                                # Look for date patterns in nearby cells (same row or column)
                                elif any(date_pattern in cell_upper for date_pattern in ['DATE', 'TD', 'SD']):
                                    # Look right for the date value
                                    if col_idx + 1 < len(row) and pd.notna(row[col_idx + 1]):
                                        date_val = row[col_idx + 1]
                                        try:
                                            parsed_date = pd.to_datetime(
                                                date_val, errors='coerce')
                                            if not pd.isna(parsed_date):
                                                if 'TRADE' in cell_upper or 'TD' in cell_upper:
                                                    trade_date = parsed_date.date()
                                                elif 'SETTLE' in cell_upper or 'SD' in cell_upper:
                                                    settle_date = parsed_date.date()
                                        except:
                                            pass

                                # Check for special date formats (e.g., "15DIC23")
                                elif len(cell_upper) == 7 and cell_upper[2:5].isalpha():
                                    try:
                                        # Handle formats like 15DIC23, 04JAN24, etc.
                                        day = int(cell_upper[:2])
                                        month_str = cell_upper[2:5]
                                        year = int(cell_upper[5:])

                                        # Map month abbreviations
                                        month_map = {
                                            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                                            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
                                            'ENE': 1, 'FEB': 2, 'MAR': 3, 'ABR': 4, 'MAY': 5, 'JUN': 6,
                                            'JUL': 7, 'AGO': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DIC': 12
                                        }

                                        if month_str in month_map:
                                            month = month_map[month_str]
                                            # Assume 20xx for the year
                                            full_year = 2000 + year
                                            date_obj = datetime(
                                                full_year, month, day).date()

                                            # Determine if it's trade or settlement date based on context
                                            # For simplicity, we'll use the first date found as trade date,
                                            # and the second as settlement date
                                            if trade_date is None:
                                                trade_date = date_obj
                                            elif settle_date is None:
                                                settle_date = date_obj
                                    except:
                                        pass

                            # Handle numeric values
                            elif isinstance(cell, (int, float)):
                                # Look for quantity (larger numbers)
                                if cell > 1000 and qty is None:
                                    qty = cell
                                # Look for price or value
                                elif value is None:
                                    value = cell

                    # If we found enough information, create a trade
                    if buy_sell and isin and qty and trade_date:
                        trade.trade_type = buy_sell
                        trade.isin = isin
                        trade.quantity = float(qty)
                        trade.trade_date = trade_date
                        if settle_date:
                            trade.settlement_date = settle_date
                        if value:
                            trade.trade_value = value
                        trade.currency = 'USD'  # Default currency
                        trade.broker = "Extracted from special format"
                        trade.account = "DEFAULT_ACCOUNT"

                        trades.append(trade)
                        break  # Stop after finding one valid trade per file

                # If we didn't find a trade date but have other trade details, try to extract date from filename
                if not trades and buy_sell and isin and qty:
                    # Try to extract date from filename
                    filename = os.path.basename(file_path)

                    # Look for date patterns in filename
                    date_patterns = [
                        # Match patterns like "15DIC23"
                        r'(\d{2})([A-Za-z]{3})(\d{2})',
                        # Match patterns like "date 15DIC23"
                        r'date\s+(\d{2})([A-Za-z]{3})(\d{2})',
                        # Match patterns like "date-15DIC23"
                        r'date[\s-]+(\d{2})([A-Za-z]{3})(\d{2})',
                        # Match patterns like "042022" (MMYYYY)
                        r'(\d{2})(\d{4})',
                        # Match patterns like "-042022" (MMYYYY)
                        r'-(\d{2})(\d{4})'
                    ]

                    extracted_date = None

                    for pattern in date_patterns:
                        import re
                        match = re.search(pattern, filename, re.IGNORECASE)
                        if match:
                            try:
                                groups = match.groups()

                                if len(groups) == 3:  # Pattern like 15DIC23
                                    day = int(groups[0])
                                    month_str = groups[1].upper()
                                    year = int(groups[2])

                                    # Map month abbreviations
                                    month_map = {
                                        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                                        'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
                                        'ENE': 1, 'FEB': 2, 'MAR': 3, 'ABR': 4, 'MAY': 5, 'JUN': 6,
                                        'JUL': 7, 'AGO': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DIC': 12
                                    }

                                    if month_str in month_map:
                                        month = month_map[month_str]
                                        # Assume 20xx for the year
                                        full_year = 2000 + year
                                        extracted_date = datetime(
                                            full_year, month, day).date()

                                elif len(groups) == 2:  # Pattern like 042022 (MMYYYY)
                                    month = int(groups[0])
                                    year = int(groups[1])

                                    if 1 <= month <= 12 and 2000 <= year <= 2100:
                                        # Default to 1st day of month
                                        extracted_date = datetime(
                                            year, month, 1).date()

                                if extracted_date:
                                    break
                            except Exception as e:
                                print(
                                    f"Error extracting date from filename: {str(e)}")

                    if extracted_date and buy_sell and isin and qty:
                        # Create a new trade object
                        trade = Trade()
                        trade.trade_type = buy_sell
                        trade.isin = isin
                        trade.quantity = float(qty)
                        trade.trade_date = extracted_date

                        # Estimate settlement date (T+2)
                        from datetime import timedelta
                        trade.settlement_date = extracted_date + \
                            timedelta(days=2)

                        if value:
                            trade.trade_value = value
                        trade.currency = 'USD'  # Default currency
                        trade.broker = f"Extracted from special format with date from filename"
                        trade.account = "DEFAULT_ACCOUNT"

                        trades.append(trade)

            except Exception as e:
                print(f"Error in special S462 processing: {str(e)}")
                # Continue with regular processing if special handling fails

        # Standard processing for regular tables
        if not trades:  # Only proceed with standard processing if no trades found yet
            # Process each table found in the file
            for table_info in structure_info['tables']:
                header_row = table_info['header_row']

                # Read the table with the correct header row
                try:
                    df = pd.read_excel(file_path, header=header_row)

                    # Clean column names and convert to uppercase for consistency
                    df.columns = [str(col).strip().upper()
                                  for col in df.columns]

                    # Process each row in this table
                    for _, row in df.iterrows():
                        # Skip rows without BUY/SELL or where it's not BUY or SELL
                        if 'BUY/SELL' not in df.columns or pd.isna(row['BUY/SELL']):
                            continue

                        buy_sell_val = str(row['BUY/SELL']).strip().upper()
                        if buy_sell_val not in ('BUY', 'SELL'):
                            continue

                        # Create a new trade object
                        trade = Trade()

                        # Process trade type
                        trade.trade_type = buy_sell_val

                        # Process ISIN
                        if 'ISIN' in df.columns and pd.notna(row['ISIN']):
                            trade.isin = str(row['ISIN']).strip()
                        elif 'CUSIP' in df.columns and pd.notna(row['CUSIP']):
                            # Handle CUSIP as if it were ISIN
                            trade.isin = str(row['CUSIP']).strip()
                        elif 'CUSIP/ISIN' in df.columns and pd.notna(row['CUSIP/ISIN']):
                            # Handle combined CUSIP/ISIN column
                            trade.isin = str(row['CUSIP/ISIN']).strip()
                        else:
                            continue  # Skip if no ISIN or CUSIP

                        # Process quantity
                        if 'QTY' in df.columns and pd.notna(row['QTY']):
                            try:
                                qty_str = str(row['QTY']).strip()
                                # Remove 'shares' or other text if present and non-numeric characters
                                qty_str = ''.join(
                                    c for c in qty_str if c.isdigit() or c in '.-,' or c.isspace())
                                qty_str = qty_str.replace(',', '')
                                trade.quantity = float(qty_str)
                            except (ValueError, TypeError):
                                print(
                                    f"Could not convert quantity to float: {row['QTY']}")
                                continue

                        # Process currency
                        if 'CURRENCY' in df.columns and pd.notna(row['CURRENCY']):
                            trade.currency = str(row['CURRENCY']).strip()
                        else:
                            # Default to USD if currency not specified
                            trade.currency = 'USD'

                        # Process trade value - try both NET CASH and TOTAL fields
                        value_field = None
                        for field in ['NET CASH', 'TOTAL', 'TOTAL SETTLEMENT AMOUNT']:
                            if field in df.columns and pd.notna(row[field]):
                                value_field = field
                                break

                        if value_field:
                            try:
                                value_str = str(row[value_field]).strip()
                                # Remove currency symbols and other non-numeric characters
                                value_str = ''.join(
                                    c for c in value_str if c.isdigit() or c in '.-,' or c.isspace())
                                value_str = value_str.replace(',', '')
                                trade.trade_value = float(value_str)
                            except (ValueError, TypeError):
                                print(
                                    f"Could not convert trade value to float: {row[value_field]}")
                                continue

                        # Process trade date - try different date field names
                        date_field = None
                        for field in ['TD', 'TRADE DATE', 'T DATE']:
                            if field in df.columns and pd.notna(row[field]):
                                date_field = field
                                break

                        if date_field:
                            try:
                                date_val = row[date_field]
                                # Handle common date formatting issues
                                if isinstance(date_val, str):
                                    # Fix common typos
                                    date_val = date_val.replace(
                                        '/022/', '/02/')
                                    if '/222' in date_val:
                                        date_val = date_val.replace(
                                            '/222', '/2022')
                                trade_date = pd.to_datetime(
                                    date_val, errors='coerce')
                                if pd.isna(trade_date):
                                    print(
                                        f"Could not parse trade date: {row[date_field]}")
                                    continue
                                trade.trade_date = trade_date.date()
                            except (ValueError, TypeError, AttributeError):
                                print(
                                    f"Could not parse trade date: {row[date_field]}")
                                continue
                        else:
                            continue  # Skip if no trade date

                        # Process settlement date - try different field names
                        settle_field = None
                        for field in ['SD', 'SETTLEMENT DATE', 'S DATE']:
                            if field in df.columns and pd.notna(row[field]):
                                settle_field = field
                                break

                        if settle_field:
                            try:
                                date_val = row[settle_field]
                                # Handle common date formatting issues
                                if isinstance(date_val, str):
                                    # Fix common typos like 05/022/2019 -> 05/02/2019
                                    date_val = date_val.replace(
                                        '/022/', '/02/')
                                    # Fix invalid year formats like 05/02/222 -> 05/02/2022
                                    if '/222' in date_val:
                                        date_val = date_val.replace(
                                            '/222', '/2022')
                                trade.settlement_date = pd.to_datetime(
                                    date_val, errors='coerce').date()
                                # Continue even if settlement date parsing fails (it's not mandatory)
                                if pd.isna(trade.settlement_date):
                                    print(
                                        f"Warning: Could not parse settlement date: {row[settle_field]}")
                                    trade.settlement_date = None
                            except (ValueError, TypeError, AttributeError):
                                print(
                                    f"Warning: Could not parse settlement date: {row[settle_field]}")
                                # Continue even if settlement date is missing
                                trade.settlement_date = None

                        # Process price if available - try different field names
                        price_field = None
                        for field in ['PRICE', 'UNIT PRICE']:
                            if field in df.columns and pd.notna(row[field]):
                                price_field = field
                                break

                        if price_field:
                            try:
                                price_str = str(row[price_field]).strip()
                                # Remove currency symbols and other non-numeric characters
                                price_str = ''.join(
                                    c for c in price_str if c.isdigit() or c in '.-,' or c.isspace())
                                price_str = price_str.replace(',', '')
                                trade.price = float(price_str)
                            except (ValueError, TypeError):
                                print(
                                    f"Could not convert price to float: {row[price_field]}")
                                # Continue processing even if price is missing

                        # Process counterparty - try different field names
                        cp_field = None
                        for field in ['COUNTERPARTY CONTACT INFO', 'COUNTERPARTY', 'BROKER']:
                            if field in df.columns and pd.notna(row[field]):
                                cp_field = field
                                break

                        if cp_field:
                            try:
                                # Limit counterparty length to avoid DB issues
                                counterparty_value = str(row[cp_field]).strip()
                                trade.broker = counterparty_value[:100] if len(
                                    counterparty_value) > 100 else counterparty_value
                            except Exception as e:
                                print(
                                    f"Warning: Could not process counterparty: {e}")
                                # Continue even with counterparty missing
                                trade.broker = None

                        # Process account number - try different field names
                        acct_field = None
                        for field in ['ACC NO AT DEPOSITORY', 'ACCOUNT', 'ACCOUNT NUMBER']:
                            if field in df.columns and pd.notna(row[field]):
                                acct_field = field
                                break

                        if acct_field:
                            try:
                                # Limit account number length to avoid DB issues
                                account_value = str(row[acct_field]).strip()
                                trade.account = account_value[:100] if len(
                                    account_value) > 100 else account_value
                            except Exception as e:
                                print(
                                    f"Warning: Could not process account number: {e}")
                                trade.account = "DEFAULT_ACCOUNT"
                        else:
                            # Default account number if not specified
                            trade.account = "DEFAULT_ACCOUNT"

                        # Add the trade to our list
                        trades.append(trade)

                except Exception as e:
                    print(
                        f"Error processing table at row {header_row}: {str(e)}")
                    continue  # Try next table if this one fails

        # If we found trades, save them to the database
        if trades:
            processor.save_trades_to_db(trades, series_number)
            return True, len(trades), None
        else:
            return False, 0, "No valid trades found in the file"

    except Exception as e:
        return False, 0, str(e)


def find_unprocessed_files(base_folder='BNY_Trades', db_path='sqlite:///nav_data.db', process_files=False, series_filter=None, dry_run=False, analyze_structures=False):
    """
    Find Excel files in the BNY_Trades folder that haven't been processed yet,
    and optionally process them with our adaptive approach.

    Args:
        base_folder: The base folder to search for files
        db_path: The path to the database
        process_files: If True, process the unprocessed files
        series_filter: Optional filter to only process specific series
        dry_run: If True, don't actually process files even if process_files is True
        analyze_structures: If True, analyze the structure of unprocessed files

    Returns:
        List of unprocessed file information
    """
    # Connect to the database
    engine = create_engine(db_path)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Get the list of files that have already been processed
    processed_files = {}
    try:
        # Get all processed files with their series numbers
        query = session.query(Trade.source_file, Trade.series_number).filter(
            Trade.source_folder == 'BNY_Trades',
            Trade.source_file != None
        ).distinct()

        for file_tuple in query.all():
            if file_tuple[0]:  # Handle None values
                processed_files[file_tuple[0]] = file_tuple[1]

        print(f"Found {len(processed_files)} processed files in the database")

        # Show some sample files from the database
        if processed_files:
            print("Sample processed files (for verification):")
            for file, series in list(processed_files.items())[:5]:
                print(f"  - Series {series}: {file}")

            # Get trade counts by series
            series_counts = session.query(
                Trade.series_number,
                func.count(Trade.id)
            ).filter(
                Trade.source_folder == 'BNY_Trades'
            ).group_by(Trade.series_number).all()

            print("\nProcessed trades by series:")
            for series, count in series_counts:
                print(f"  - Series {series}: {count} trades")
    except Exception as e:
        print(f"Error querying database: {str(e)}")
        processed_files = {}

    # Walk through the BNY_Trades directory to find all Excel files
    all_files = []
    for root, _, files in os.walk(base_folder):
        for file in files:
            if file.endswith('.xlsx') or file.endswith('.xls'):
                series_match = None
                # Extract series from path, for example BNY_Trades/HFMX/S461/file.xlsx -> S461
                path_parts = root.split(os.sep)
                for part in path_parts:
                    if part.startswith('S') and part[1:].isdigit():
                        series_match = part
                        break

                # Skip files that don't match the series filter if specified
                if series_filter and series_match != series_filter:
                    continue

                # Check if filename is in processed files
                is_processed = file in processed_files

                all_files.append({
                    'full_path': os.path.join(root, file),
                    'filename': file,
                    'series': series_match,
                    'subfolder': root,
                    'is_processed': is_processed,
                    'type': 'Trade Report' if 'trade' in file.lower() else 'Unknown'
                })

    print(f"\nFound {len(all_files)} total Excel files in {base_folder}")

    # Find files that haven't been processed
    unprocessed_files = [f for f in all_files if not f['is_processed']]

    print(f"Found {len(unprocessed_files)} unprocessed files")

    # Analyze the structure of unprocessed files if requested
    if analyze_structures and unprocessed_files:
        print("\nAnalyzing structure of unprocessed files...")
        for i, file_info in enumerate(unprocessed_files, 1):
            print(
                f"Analyzing file {i} of {len(unprocessed_files)}: {file_info['filename']}")
            structure_info = analyze_excel_structure(file_info['full_path'])
            file_info.update(structure_info)

        # Count structure types
        structure_counts = {}
        for file_info in unprocessed_files:
            structure_type = file_info.get('structure_type', 'unknown')
            structure_counts[structure_type] = structure_counts.get(
                structure_type, 0) + 1

        print("\nStructure types found:")
        for structure_type, count in structure_counts.items():
            print(f"  - {structure_type}: {count} files")

        # With adaptive approach, all files should be processable
        print("\nAll files will be processed with the adaptive approach.")

    # Group unprocessed files by series
    series_groups = {}
    for file_info in unprocessed_files:
        series = file_info['series'] or 'Unknown'
        if series not in series_groups:
            series_groups[series] = []
        series_groups[series].append(file_info)

    # Print summary by series
    print("\nUnprocessed files by series:")
    for series, files in sorted(series_groups.items()):
        print(f"{series}: {len(files)} files")
        for file_info in files:
            print(f"  - {file_info['full_path']}")

    # Save results to Excel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Create a DataFrame with both processed and unprocessed files
    df_all = pd.DataFrame(all_files)
    df_all['status'] = df_all['is_processed'].apply(
        lambda x: 'Processed' if x else 'Unprocessed')

    # Sort by series and status
    df_all = df_all.sort_values(['series', 'status', 'filename'])

    # Create an Excel file with multiple sheets
    output_file = f'bny_files_report_{timestamp}.xlsx'

    try:
        # Try using xlsxwriter for fancy formatting
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            # Write all files worksheet
            df_all.to_excel(writer, sheet_name='All Files', index=False)

            try:
                # Add conditional formatting
                workbook = writer.book
                worksheet = writer.sheets['All Files']

                # Add conditional formatting to highlight unprocessed files
                red_format = workbook.add_format(
                    {'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
                green_format = workbook.add_format(
                    {'bg_color': '#C6EFCE', 'font_color': '#006100'})

                # Apply conditional formatting to the status column
                status_col = df_all.columns.get_loc(
                    'status') + 1  # +1 for Excel 1-indexing
                worksheet.conditional_format(1, status_col, len(df_all) + 1, status_col, {
                    'type': 'cell',
                    'criteria': 'equal to',
                    'value': '"Unprocessed"',
                    'format': red_format
                })
                worksheet.conditional_format(1, status_col, len(df_all) + 1, status_col, {
                    'type': 'cell',
                    'criteria': 'equal to',
                    'value': '"Processed"',
                    'format': green_format
                })
            except Exception as e:
                print(f"Warning: Could not apply Excel formatting: {e}")

            # Write unprocessed files worksheet
            df_unprocessed = pd.DataFrame(unprocessed_files)
            if not df_unprocessed.empty:
                df_unprocessed.to_excel(
                    writer, sheet_name='Unprocessed Files', index=False)

            # Write series summary worksheet
            summary_data = []
            for series, files in series_groups.items():
                summary_data.append({
                    'Series': series,
                    'Unprocessed Files': len(files),
                    'File Examples': ', '.join([f['filename'] for f in files[:3]])
                })

            if summary_data:
                df_summary = pd.DataFrame(summary_data)
                df_summary.to_excel(
                    writer, sheet_name='Series Summary', index=False)

            # If file structures were analyzed, add a structure analysis sheet
            if analyze_structures and unprocessed_files:
                structure_data = []
                for file_info in unprocessed_files:
                    if 'structure_type' in file_info:
                        structure_data.append({
                            'Filename': file_info['filename'],
                            'Series': file_info['series'] or 'Unknown',
                            'Structure Type': file_info.get('structure_type', 'unknown'),
                            'Tables Found': len(file_info.get('tables', [])),
                            'Header Rows': ', '.join(map(str, file_info.get('header_rows', []))),
                            'Column Offsets': ', '.join([str(table.get('column_offset', 'N/A')) for table in file_info.get('tables', [])]),
                            'Data Rows': ', '.join([str(table.get('data_rows', 0)) for table in file_info.get('tables', [])]),
                            'Processing Approach': file_info.get('processing_approach', 'standard')
                        })

                if structure_data:
                    df_structure = pd.DataFrame(structure_data)
                    df_structure.to_excel(
                        writer, sheet_name='Structure Analysis', index=False)

    except Exception as e:
        print(f"Warning: Could not create formatted Excel file: {e}")
        print("Saving as basic Excel file instead...")

        # Fallback to basic Excel export
        df_all.to_excel(output_file, sheet_name='All Files', index=False)

    print(f"\nComplete report saved to {output_file}")

    # Process the unprocessed files if requested
    if process_files and unprocessed_files and not dry_run:
        print("\nProcessing unprocessed files using adaptive approach...")

        processed_count = 0
        error_count = 0
        error_files = []

        for i, file_info in enumerate(unprocessed_files, 1):
            print(
                f"\nProcessing file {i} of {len(unprocessed_files)}: {file_info['full_path']}")

            # Always use the adaptive processing approach for all files
            try:
                success, count, error = process_file_adaptive(
                    file_info['full_path'], db_path)

                if success:
                    processed_count += 1
                    print(
                        f"Successfully processed {count} trades from {file_info['full_path']}")
                else:
                    error_count += 1
                    error_files.append({
                        'file': file_info['full_path'],
                        'error': error,
                        'structure_type': file_info.get('structure_type', 'unknown')
                    })
                    print(
                        f"Error processing {file_info['full_path']}: {error}")
            except Exception as e:
                error_count += 1
                error_files.append({
                    'file': file_info['full_path'],
                    'error': str(e),
                    'structure_type': file_info.get('structure_type', 'unknown')
                })
                print(f"Error processing {file_info['full_path']}: {str(e)}")

        print(
            f"\nProcessing complete. Successfully processed {processed_count} files. Errors: {error_count}")

        # Save error report if there were any errors
        if error_files:
            error_df = pd.DataFrame(error_files)
            error_file = f'processing_errors_{timestamp}.xlsx'
            error_df.to_excel(error_file, index=False)
            print(f"Error details saved to {error_file}")
    elif process_files and unprocessed_files and dry_run:
        print("\nDRY RUN: Would process the following files with adaptive approach:")
        for file_info in unprocessed_files:
            print(f"  - {file_info['full_path']}")
        print("\nTo actually process these files, run without the --dry-run flag")

    session.close()
    return unprocessed_files


def process_file_with_custom_approach(file_path, db_path='sqlite:///nav_data.db'):
    """
    Process a file that has an unusual structure, handling different table formats

    Args:
        file_path: Path to the Excel file
        db_path: Path to the database

    Returns:
        tuple: (success, count, error_message)
    """
    try:
        # First analyze the file structure
        structure_info = analyze_excel_structure(file_path)

        if not structure_info['analysis_success']:
            return False, 0, "Could not analyze file structure"

        # Extract series number from file path
        series_number = None
        path_parts = file_path.split(os.sep)
        for part in path_parts:
            if part.startswith('S') and part[1:].isdigit():
                series_number = part[1:]  # Remove the 'S' prefix
                break

        if not series_number:
            return False, 0, "Could not extract series number from file path"

        # Initialize the trade processor
        processor = BNYTradeProcessor(db_path=db_path)
        processor.current_file = file_path

        # Initialize an empty list to store trade objects
        trades = []

        # Process each table found in the file
        for table_info in structure_info['tables']:
            header_row = table_info['header_row']
            column_offset = table_info['column_offset']

            # Read the table with the correct header row
            df = pd.read_excel(file_path, header=header_row)

            # Process each row in this table
            for _, row in df.iterrows():
                buy_sell_val = row.get('BUY/SELL')
                if pd.isna(buy_sell_val) or buy_sell_val.strip().upper() not in ('BUY', 'SELL'):
                    continue

                # Create a new trade object
                trade = Trade()

                # Process trade type
                trade.trade_type = buy_sell_val.strip().upper()

                # Process ISIN
                if pd.notna(row.get('ISIN')):
                    trade.isin = str(row.get('ISIN')).strip()
                elif pd.notna(row.get('CUSIP')):
                    # Handle CUSIP as if it were ISIN
                    trade.isin = str(row.get('CUSIP')).strip()
                elif pd.notna(row.get('CUSIP/ISIN')):
                    # Handle combined CUSIP/ISIN column
                    trade.isin = str(row.get('CUSIP/ISIN')).strip()
                elif pd.notna(row.get('cusip/isin')):
                    # Handle combined CUSIP/ISIN column in lower case
                    trade.isin = str(row.get('cusip/isin')).strip()
                else:
                    continue

                # Process quantity
                if pd.notna(row.get('QTY')):
                    try:
                        qty_str = str(row.get('QTY')).strip()
                        # Remove 'shares' or other text if present and non-numeric characters
                        qty_str = ''.join(
                            c for c in qty_str if c.isdigit() or c in '.-,' or c.isspace())
                        qty_str = qty_str.replace(',', '')
                        trade.quantity = float(qty_str)
                    except (ValueError, TypeError):
                        print(
                            f"Could not convert quantity to float: {row.get('QTY')}")
                        continue

                # Process currency
                if pd.notna(row.get('CURRENCY')):
                    trade.currency = str(row.get('CURRENCY')).strip()
                else:
                    # Default to USD if currency not specified
                    trade.currency = 'USD'

                # Process trade value
                if pd.notna(row.get('NET CASH')):
                    try:
                        value_str = str(row.get('NET CASH')).strip()
                        # Remove currency symbols and other non-numeric characters
                        value_str = ''.join(
                            c for c in value_str if c.isdigit() or c in '.-,' or c.isspace())
                        value_str = value_str.replace(',', '')
                        trade.trade_value = float(value_str)
                    except (ValueError, TypeError):
                        print(
                            f"Could not convert trade value to float: {row.get('NET CASH')}")
                        continue

                # Process trade date
                if pd.notna(row.get('TD')):
                    try:
                        date_val = row.get('TD')
                        # Handle common date formatting issues
                        if isinstance(date_val, str):
                            # Fix common typos
                            date_val = date_val.replace('/022/', '/02/')
                            if '/222' in date_val:
                                date_val = date_val.replace('/222', '/2022')
                        trade_date = pd.to_datetime(date_val, errors='coerce')
                        if pd.isna(trade_date):
                            print(
                                f"Could not parse trade date: {row.get('TD')}")
                            continue
                        trade.trade_date = trade_date.date()
                    except (ValueError, TypeError, AttributeError):
                        print(f"Could not parse trade date: {row.get('TD')}")
                        continue

                # Process settlement date
                if pd.notna(row.get('SD')):
                    try:
                        trade.settlement_date = pd.to_datetime(
                            row.get('SD')).date()
                    except (ValueError, TypeError):
                        print(
                            f"Could not parse settlement date: {row.get('SD')}")
                        continue

                # Process price if available
                if 'PRICE' in row.index and pd.notna(row.get('PRICE')):
                    try:
                        price_str = str(row.get('PRICE')).strip()
                        trade.price = float(price_str.replace(',', ''))
                    except (ValueError, TypeError):
                        print(
                            f"Could not convert price to float: {row.get('PRICE')}")

                # Process counterparty
                if 'COUNTERPARTY CONTACT INFO' in row.index and pd.notna(row.get('COUNTERPARTY CONTACT INFO')):
                    trade.counterparty = str(
                        row.get('COUNTERPARTY CONTACT INFO')).strip()

                # Process account number
                if 'ACC NO AT DEPOSITORY' in row.index and pd.notna(row.get('ACC NO AT DEPOSITORY')):
                    trade.account_number = str(
                        row.get('ACC NO AT DEPOSITORY')).strip()
                else:
                    # Default account number if not specified
                    trade.account_number = "DEFAULT_ACCOUNT"

                # Add the trade to our list
                trades.append(trade)

        # If we found trades, save them to the database
        if trades:
            processor.save_trades_to_db(trades, series_number)
            return True, len(trades), None
        else:
            return False, 0, "No valid trades found in the file"

    except Exception as e:
        return False, 0, str(e)


def main():
    parser = argparse.ArgumentParser(
        description='Find and optionally process unprocessed BNY trade files')
    parser.add_argument('--process', action='store_true',
                        help='Process unprocessed files after finding them')
    parser.add_argument('--folder', default='BNY_Trades',
                        help='Base folder to search for files')
    parser.add_argument(
        '--db', default='sqlite:///nav_data.db', help='Database path')
    parser.add_argument(
        '--series', help='Filter to a specific series (e.g., S461)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Dry run mode (don\'t actually process files)')
    parser.add_argument('--analyze', action='store_true',
                        help='Analyze file structures to identify processing challenges')
    args = parser.parse_args()

    find_unprocessed_files(
        base_folder=args.folder,
        db_path=args.db,
        process_files=args.process,
        series_filter=args.series,
        dry_run=args.dry_run,
        analyze_structures=args.analyze
    )


if __name__ == "__main__":
    main()
