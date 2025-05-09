import pandas as pd
import os
import re
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Trade, Base


class BNYTradeProcessor:
    def __init__(self, db_path='sqlite:///nav_data.db', batch_size=50):
        self.trades = []
        self.engine = create_engine(db_path)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.batch_size = batch_size
        self.total_saved = 0

    def extract_series_number(self, file_path):
        """Extract series number from file path (e.g., 'BNY_Trades/HFMX/S464/...' -> '464')"""
        match = re.search(r'S(\d+)', file_path)
        if match:
            return match.group(1)
        return None

    def save_trades_to_db(self, trades_batch, series_number):
        """Save a batch of processed trades to the database"""
        if not trades_batch:
            return

        session = self.Session()
        try:
            for trade_obj in trades_batch:
                # Create a dictionary with trade data to handle attribute errors gracefully
                trade_data = {
                    'series_number': series_number,  # Use extracted series number
                    'trade_date': getattr(trade_obj, 'trade_date', None),
                    'trade_type': getattr(trade_obj, 'trade_type', None),
                    'security_type': "Fixed Income",  # Default for BNY trades
                    'security_id': getattr(trade_obj, 'isin', None),
                    'quantity': getattr(trade_obj, 'quantity', None),
                    'price': getattr(trade_obj, 'price', None),
                    'currency': getattr(trade_obj, 'currency', 'USD'),
                    'settlement_date': getattr(trade_obj, 'settlement_date', None),
                    'trade_value': getattr(trade_obj, 'trade_value', None),
                    'source_file': os.path.basename(self.current_file) if hasattr(
                        self, 'current_file') else None,
                    'source_folder': "BNY_Trades"
                }

                # Set security name
                if hasattr(trade_obj, 'isin') and trade_obj.isin:
                    trade_data['security_name'] = f"ISIN: {trade_obj.isin}"
                else:
                    trade_data['security_name'] = "Unknown Security"

                # Handle broker (counterparty) field
                if hasattr(trade_obj, 'broker') and trade_obj.broker:
                    trade_data['broker'] = trade_obj.broker
                elif hasattr(trade_obj, 'counterparty') and trade_obj.counterparty:
                    # Limit length to 100 chars to match model definition
                    counterparty = str(trade_obj.counterparty).strip()
                    trade_data['broker'] = counterparty[:100] if len(
                        counterparty) > 100 else counterparty

                # Handle account field
                if hasattr(trade_obj, 'account') and trade_obj.account:
                    trade_data['account'] = trade_obj.account
                elif hasattr(trade_obj, 'account_number') and trade_obj.account_number:
                    # Limit length to 100 chars to match model definition
                    account = str(trade_obj.account_number).strip()
                    trade_data['account'] = account[:100] if len(
                        account) > 100 else account
                else:
                    trade_data['account'] = "DEFAULT_ACCOUNT"

                # Create Trade object
                trade = Trade(**trade_data)
                session.add(trade)

            session.commit()
            self.total_saved += len(trades_batch)
            print(
                f"\nSaved batch of {len(trades_batch)} trades to database for Series {series_number}. Total saved: {self.total_saved}")
        except Exception as e:
            session.rollback()
            print(f"Error saving trades to database: {str(e)}")
            raise
        finally:
            session.close()

    def process_file(self, file_path):
        self.current_file = file_path

        # Extract series number from file path
        series_number = self.extract_series_number(file_path)
        if not series_number:
            print(f"Could not extract series number from path: {file_path}")
            return

        print(f"\nProcessing file: {file_path} for Series {series_number}")

        # Read Excel file
        df = pd.read_excel(file_path, header=None)
        print(f"File shape: {df.shape}")

        # Find first header row
        header_row = self._find_header_row(df)
        if header_row is None:
            print(f"Could not find header row in {file_path}")
            return

        # Read file with correct header
        df = pd.read_excel(file_path, header=header_row)
        print(f"Columns found: {df.columns.tolist()}")

        # Clean column names
        df.columns = [str(col).strip().upper() for col in df.columns]
        print(f"Cleaned columns: {df.columns.tolist()}")

        # Process each row
        current_row = 0
        batch = []
        while current_row < len(df):
            row = df.iloc[current_row]

            try:
                # Skip empty rows and header rows
                if pd.isna(row['BUY/SELL']) or str(row['BUY/SELL']).strip().upper() == 'BUY/SELL':
                    current_row += 1
                    continue

                trade = Trade()

                # Process trade type
                trade_type = str(row['BUY/SELL']).strip().upper()
                if trade_type in ['BUY', 'SELL']:
                    trade.trade_type = trade_type
                else:
                    current_row += 1
                    continue

                # Process ISIN
                if pd.notna(row['ISIN']):
                    trade.isin = str(row['ISIN']).strip()
                else:
                    current_row += 1
                    continue

                # Process quantity
                if pd.notna(row['QTY']):
                    try:
                        qty_str = str(row['QTY']).strip()
                        # Remove 'shares' or other text if present
                        qty_str = ''.join(
                            c for c in qty_str if c.isdigit() or c == '.' or c == ',')
                        trade.quantity = float(qty_str.replace(',', ''))
                    except (ValueError, TypeError):
                        print(
                            f"Could not convert quantity to float: {row['QTY']}")
                        current_row += 1
                        continue

                # Process currency
                if pd.notna(row['CURRENCY']):
                    trade.currency = str(row['CURRENCY']).strip()
                else:
                    # Default to USD if currency not specified
                    trade.currency = 'USD'

                # Process trade value
                if pd.notna(row['NET CASH']):
                    try:
                        value_str = str(row['NET CASH']).strip()
                        trade.trade_value = float(value_str.replace(',', ''))
                    except (ValueError, TypeError):
                        print(
                            f"Could not convert trade value to float: {row['NET CASH']}")
                        current_row += 1
                        continue

                # Process trade date
                if pd.notna(row['TD']):
                    try:
                        trade.trade_date = pd.to_datetime(row['TD']).date()
                    except (ValueError, TypeError):
                        print(f"Could not parse trade date: {row['TD']}")
                        current_row += 1
                        continue

                # Process settlement date
                if pd.notna(row['SD']):
                    try:
                        trade.settlement_date = pd.to_datetime(
                            row['SD']).date()
                    except (ValueError, TypeError):
                        print(f"Could not parse settlement date: {row['SD']}")
                        current_row += 1
                        continue

                # Process price if available
                if 'PRICE' in row.index and pd.notna(row['PRICE']):
                    try:
                        price_str = str(row['PRICE']).strip()
                        trade.price = float(price_str.replace(',', ''))
                    except (ValueError, TypeError):
                        print(
                            f"Could not convert price to float: {row['PRICE']}")

                # Process counterparty
                if 'COUNTERPARTY CONTACT INFO' in row.index and pd.notna(row['COUNTERPARTY CONTACT INFO']):
                    trade.counterparty = str(
                        row['COUNTERPARTY CONTACT INFO']).strip()

                # Process account number
                if 'ACC NO AT DEPOSITORY' in row.index and pd.notna(row['ACC NO AT DEPOSITORY']):
                    trade.account_number = str(
                        row['ACC NO AT DEPOSITORY']).strip()
                else:
                    # Default account number if not specified
                    trade.account_number = "DEFAULT_ACCOUNT"

                batch.append(trade)

                # Save batch if it reaches the batch size
                if len(batch) >= self.batch_size:
                    self.save_trades_to_db(batch, series_number)
                    batch = []

                current_row += 1

            except Exception as e:
                print(f"Error processing row {current_row}: {str(e)}")
                current_row += 1

        # Save any remaining trades in the batch
        if batch:
            self.save_trades_to_db(batch, series_number)

        print(f"\nProcessed {self.total_saved} valid trades from {file_path}")

    def _find_header_row(self, df):
        """Find the first row containing 'BUY/SELL'"""
        for idx, row in df.iterrows():
            if any(str(cell).strip().upper() == 'BUY/SELL' for cell in row):
                return idx
        return None

    def process_folder(self, folder_path):
        """Process all Excel files in a folder"""
        for root, _, files in os.walk(folder_path):
            for file in files:
                if file.endswith('.xlsx') or file.endswith('.xls'):
                    file_path = os.path.join(root, file)
                    try:
                        self.process_file(file_path)
                    except Exception as e:
                        print(f"Error processing file {file_path}: {str(e)}")
                        continue


def main():
    processor = BNYTradeProcessor()
    folder_path = 'BNY_Trades'
    processor.process_folder(folder_path)


if __name__ == "__main__":
    main()
