import pandas as pd
import numpy as np
from datetime import datetime
import re


class DataAnalyzer:
    """
    Handles uploading and processing of OPay financial transaction data.
    Extracts account metadata, cleans transactions, and computes analytics.
    """

    def __init__(self):
        self.df = None
        self.monthly_df = None
        self.metadata = {}

    # ============================================================
    # HELPER METHODS
    # ============================================================

    def _extract_metadata(self, df):
        """
        Extracts account metadata from the header rows of OPay statement.
        """
        metadata = {}

        try:
            for i in range(min(len(df), 10)):
                row = [str(val).strip() for val in df.iloc[i].values]
                row_text = ' '.join(row)

                # Row with Account Name & Account Number
                if 'Account Name' in row_text:
                    for j, val in enumerate(row):
                        if val == 'Account Name' and j + 1 < len(row):
                            metadata['account_name'] = row[j + 1]
                        if val == 'Account Number' and j + 1 < len(row):
                            metadata['account_number'] = row[j + 1]

                # Row with Account Type & Period
                if 'Account Type' in row_text:
                    for j, val in enumerate(row):
                        if val == 'Account Type' and j + 1 < len(row):
                            metadata['account_type'] = row[j + 1]
                        if val == 'Period' and j + 1 < len(row):
                            metadata['period'] = row[j + 1]

                # Row with Opening Balance & Total Debit
                if 'Opening Balance' in row_text:
                    for j, val in enumerate(row):
                        if val == 'Opening Balance' and j + 1 < len(row):
                            metadata['opening_balance'] = row[j + 1]
                        if val == 'Total Debit' and j + 1 < len(row):
                            metadata['total_debit'] = row[j + 1]
                        if val == 'Debit Count' and j + 1 < len(row):
                            metadata['debit_count'] = row[j + 1]

                # Row with Closing Balance & Total Credit
                if 'Closing Balance' in row_text:
                    for j, val in enumerate(row):
                        if val == 'Closing Balance' and j + 1 < len(row):
                            metadata['closing_balance'] = row[j + 1]
                        if val == 'Total Credit' and j + 1 < len(row):
                            metadata['total_credit'] = row[j + 1]
                        if val == 'Credit Count' and j + 1 < len(row):
                            metadata['credit_count'] = row[j + 1]

                # Date Printed
                if 'Date Printed' in row_text:
                    for j, val in enumerate(row):
                        if val == 'Date Printed' and j + 1 < len(row):
                            metadata['date_printed'] = row[j + 1]

        except Exception as e:
            print(f"Metadata extraction warning: {e}")

        return metadata

    def _find_header_row(self, df):
        """
        Finds the row index containing 'Trans. Date' column header.
        """
        for i in range(min(len(df), 20)):
            row_values = [str(val).strip() for val in df.iloc[i].values]
            if 'Trans. Date' in row_values:
                return i
        return -1

    def _clean_currency(self, value):
        """
        Converts currency string to float.
        Handles: ₦50,000.10, --, empty, etc.
        """
        if pd.isna(value):
            return 0.0

        val_str = str(value).strip()

        if val_str in ['--', '-', '', 'nan', 'None', 'NaN']:
            return 0.0

        # Remove ₦ symbol, commas, spaces, quotes
        cleaned = re.sub(r'[₦,\s"\']', '', val_str)

        try:
            return float(cleaned)
        except ValueError:
            return 0.0

    def _parse_datetime(self, date_str):
        """
        Parses OPay date formats:
        - '04 Jan 2025 17:20:01' (Trans. Date)
        - '04 Jan 2025' (Value Date)
        """
        if pd.isna(date_str):
            return None

        date_str = str(date_str).strip()

        formats = [
            '%d %b %Y %H:%M:%S',  # 04 Jan 2025 17:20:01
            '%d %b %Y',           # 04 Jan 2025
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%d/%m/%Y %H:%M:%S',
            '%d/%m/%Y',
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue

        return None

    def _extract_recipient(self, desc):
        """
        Extracts clean recipient name from transaction description.
        """
        if pd.isna(desc):
            return 'Unknown'

        desc = str(desc)

        # Remove common prefixes
        prefixes = [
            r'^Transfer to\s*',
            r'^Payment to\s*',
            r'^Sent to\s*',
            r'^Transfer from\s*',
            r'^Payment from\s*',
            r'^Received from\s*',
        ]

        name = desc
        for pattern in prefixes:
            name = re.sub(pattern, '', name, flags=re.IGNORECASE)

        # Take first part before separators
        name = re.split(r'[-/|()]', name)[0].strip()

        # Truncate for display
        return name[:25] if len(name) > 25 else name

    # ============================================================
    # MAIN PROCESSING METHOD
    # ============================================================

    def process_data(self, file_path: str):
        """
        Main method to read and process OPay statement file.
        Returns: (success: bool, message: str)
        """
        try:
            # Read file without headers first
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, header=None, on_bad_lines='skip')
            elif file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path, header=None)
            else:
                return False, "Unsupported file format. Please upload CSV or Excel file."

            # Extract metadata from header rows
            self.metadata = self._extract_metadata(df)

            # Find the header row
            header_row = self._find_header_row(df)

            if header_row == -1:
                return False, "Could not find transaction table. Looking for 'Trans. Date' column."

            # Set column headers
            df.columns = [str(c).strip() for c in df.iloc[header_row].values]
            df = df.iloc[header_row + 1:].reset_index(drop=True)

            # Expected OPay columns
            column_mapping = {
                'Trans. Date': 'trans_date',
                'Value Date': 'value_date',
                'Description': 'description',
                'Debit(₦)': 'debit',
                'Credit(₦)': 'credit',
                'Balance After(₦)': 'balance',
                'Channel': 'channel',
                'Transaction Reference': 'reference'
            }

            # Check required columns exist
            required = ['Trans. Date', 'Description', 'Debit(₦)', 'Credit(₦)']
            missing = [col for col in required if col not in df.columns]

            if missing:
                return False, f"Missing required columns: {', '.join(missing)}"

            # Rename columns
            df = df.rename(columns=column_mapping)

            # Clean currency columns
            df['debit'] = df['debit'].apply(self._clean_currency)
            df['credit'] = df['credit'].apply(self._clean_currency)

            if 'balance' in df.columns:
                df['balance'] = df['balance'].apply(self._clean_currency)

            # Parse dates
            df['trans_date'] = df['trans_date'].apply(self._parse_datetime)
            df = df.dropna(subset=['trans_date'])

            if df.empty:
                return False, "No valid transactions found after parsing dates."

            if 'value_date' in df.columns:
                df['value_date'] = df['value_date'].apply(self._parse_datetime)

            # Sort by date
            df = df.sort_values('trans_date').reset_index(drop=True)

            # Add derived columns
            df['month_year'] = df['trans_date'].dt.strftime('%b %Y')
            df['day_name'] = df['trans_date'].dt.day_name()
            df['hour'] = df['trans_date'].dt.hour
            df['date_only'] = df['trans_date'].dt.date
            df['week'] = df['trans_date'].dt.isocalendar().week

            self.df = df

            # Create monthly aggregation
            self.monthly_df = df.groupby('month_year', sort=False).agg({
                'debit': 'sum',
                'credit': 'sum',
                'description': 'count'
            }).rename(columns={'description': 'count'}).reset_index()

            return True, f"Successfully processed {len(df)} transactions."

        except Exception as e:
            return False, f"Error processing file: {str(e)}"

    # ============================================================
    # GETTER METHODS
    # ============================================================

    def get_account_info(self):
        """Returns extracted account metadata."""
        return self.metadata

    def get_date_range(self):
        """Returns the date range of transactions."""
        if self.df is None or self.df.empty:
            return None

        return {
            'start': self.df['trans_date'].min().strftime('%d %b %Y'),
            'end': self.df['trans_date'].max().strftime('%d %b %Y'),
            'days': (self.df['trans_date'].max() - self.df['trans_date'].min()).days
        }

    # ============================================================
    # ANALYTICS METHODS
    # ============================================================

    def get_summary_stats(self):
        """Returns basic summary statistics."""
        if self.df is None or self.df.empty:
            return {}

        df = self.df

        total_debit = df['debit'].sum()
        total_credit = df['credit'].sum()
        net_flow = total_credit - total_debit

        return {
            'total_transactions': len(df),
            'debit_count': len(df[df['debit'] > 0]),
            'credit_count': len(df[df['credit'] > 0]),
            'total_debit': f"₦{total_debit:,.2f}",
            'total_credit': f"₦{total_credit:,.2f}",
            'net_flow': f"₦{net_flow:,.2f}",
            'total_debit_raw': total_debit,
            'total_credit_raw': total_credit,
            'net_flow_raw': net_flow
        }

    def get_fun_stats(self):

        df = self.df
        if df is None or df.empty:
            return {}

        total_out = df['debit'].sum()
        total_in = df['credit'].sum()

        # ---- TOP RECIPIENTS ----
        # Ignore internal/automatic transactions
        ignore_keywords = [
            'OWealth', 'Auto-save', 'Interest', 'Withdrawal(Transaction',
            'Internal', 'Reversal', 'Cashback', 'Bonus'
        ]

        payments_df = df[df['debit'] > 0].copy()
        for kw in ignore_keywords:
            payments_df = payments_df[
                ~payments_df['description'].str.contains(kw, case=False, na=False, regex=False)
            ]

        if not payments_df.empty:
            payments_df['recipient'] = payments_df['description'].apply(self._extract_recipient)
            top_recipients = payments_df.groupby('recipient')['debit'].agg(['sum', 'count'])
            top_recipients = top_recipients.sort_values('sum', ascending=False).head(5)
            top_people = {
                name: {
                    'total': f"₦{row['sum']:,.2f}",
                    'count': int(row['count'])
                }
                for name, row in top_recipients.iterrows()
            }
        else:
            top_people = {}

        # ---- AVERAGES ----
        debit_txns = df[df['debit'] > 0]
        avg_spend = debit_txns['debit'].mean() if not debit_txns.empty else 0

        credit_txns = df[df['credit'] > 0]
        avg_income = credit_txns['credit'].mean() if not credit_txns.empty else 0

        # ---- WEEKEND VS WEEKDAY ----
        weekend_mask = df['day_name'].isin(['Saturday', 'Sunday'])
        weekend_spend = df[weekend_mask]['debit'].sum()
        weekday_spend = df[~weekend_mask]['debit'].sum()
        weekend_pct = (weekend_spend / total_out * 100) if total_out > 0 else 0

        # ---- BUSIEST DAY ----
        daily_spending = df.groupby('date_only')['debit'].sum()
        if not daily_spending.empty:
            busiest_day = daily_spending.idxmax()
            busiest_amount = daily_spending.max()
        else:
            busiest_day = None
            busiest_amount = 0

        # ---- DATA & AIRTIME ----
        data_keywords = [
            'Mobile Data', 'Data Purchase', 'Airtime', 'VTU',
            'MTN', 'GLO', 'Airtel', '9mobile', 'DSTV', 'GOTV'
        ]
        data_mask = df['description'].str.contains(
            '|'.join(data_keywords), case=False, na=False
        )
        data_spend = df[data_mask & (df['debit'] > 0)]['debit'].sum()
        data_pct = (data_spend / total_out * 100) if total_out > 0 else 0

        # ---- SPENDING BY DAY OF WEEK ----
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        weekday_grouped = (
            df[df['debit'] > 0]
            .groupby('day_name')
            .agg(
                total_spent=('debit', 'sum'),
                transaction_count=('debit', 'count')
            )
            .reindex(day_order, fill_value=0)
        )

        weekday_breakdown = [
            {
                "day": day,
                "total_spent": float(row["total_spent"]),
                "transaction_count": int(row["transaction_count"])
            }
            for day, row in weekday_grouped.iterrows()
        ]
        if 'day_name' not in df.columns:
            print("DEBUG: 'day_name' column missing in df!")
        else:
            print("DEBUG: day_name unique values:", df['day_name'].unique())
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            spending_by_day = df.groupby('day_name')['debit'].sum().reindex(day_order, fill_value=0).to_dict()
            print("DEBUG: spending_by_day dict:", spending_by_day)



        # ---- SPENDING BY HOUR ----
        spending_by_hour = df.groupby('hour')['debit'].sum().to_dict()

        # ---- CHANNEL BREAKDOWN ----
        if 'channel' in df.columns:
            channel_spending = df.groupby('channel')['debit'].sum().to_dict()
        else:
            channel_spending = {}

        # ---- TOP SPENDING MONTH ----
        if self.monthly_df is not None and not self.monthly_df.empty:
            top_month_idx = self.monthly_df['debit'].idxmax()
            top_month = self.monthly_df.loc[top_month_idx, 'month_year']
            top_month_amount = self.monthly_df.loc[top_month_idx, 'debit']
        else:
            top_month = 'N/A'
            top_month_amount = 0

        # ---- TRANSACTION FREQUENCY ----
        date_range = (df['trans_date'].max() - df['trans_date'].min()).days
        avg_txns_per_day = len(df) / date_range if date_range > 0 else len(df)

        return {
            # Main totals
            'total_out': f"₦{total_out:,.2f}",
            'total_in': f"₦{total_in:,.2f}",
            'net': f"₦{(total_in - total_out):,.2f}",
            'net_status': 'positive' if total_in >= total_out else 'negative',

            # Counts
            'transaction_count': len(df),
            'debit_count': len(debit_txns),
            'credit_count': len(credit_txns),

            # Averages
            'avg_spend': f"₦{avg_spend:,.2f}",
            'avg_income': f"₦{avg_income:,.2f}",
            'avg_txns_per_day': f"{avg_txns_per_day:.1f}",

            # Top recipients
            'top_recipients': top_people,

            # Weekend analysis
            
            'weekend_spend': f"₦{weekend_spend:,.2f}",
            'weekday_spend': f"₦{weekday_spend:,.2f}",
            'weekend_pct': f"{weekend_pct:.1f}%",
            'weekday_breakdown': weekday_breakdown,

            # Busiest day
            'busiest_day': busiest_day.strftime('%d %b %Y') if busiest_day else 'N/A',
            'busiest_day_amount': f"₦{busiest_amount:,.2f}",

            # Data spending
            'data_airtime_spend': f"₦{data_spend:,.2f}",
            'data_pct': f"{data_pct:.1f}%",

            # Monthly
            'top_spending_month': top_month,
            'top_month_amount': f"₦{top_month_amount:,.2f}",

            # Patterns (for charts)
            'spending_by_hour': spending_by_hour,
            'spending_by_channel': channel_spending,

            # Monthly data for charts
            'monthly_data': self.monthly_df.to_dict('records') if self.monthly_df is not None else []
        }

    def get_transaction_categories(self):
        """
        Categorizes transactions based on description patterns.
        Returns spending breakdown by category.
        """
        if self.df is None or self.df.empty:
            return {}

        categories = {
            'Transfers': ['Transfer to', 'Transfer from', 'Sent to', 'Received from'],
            'OWealth/Savings': ['OWealth', 'Auto-save', 'Savings'],
            'Data & Airtime': ['Mobile Data', 'Airtime', 'VTU', 'MTN', 'GLO', 'Airtel', '9mobile'],
            'Bills & Utilities': ['Electricity', 'PHCN', 'DSTV', 'GOTV', 'Startimes', 'Cable', 'Water', 'NEPA'],
            'POS Transactions': ['POS', 'Terminal', 'ATM'],
            'Bank Charges': ['Charge', 'Fee', 'Commission', 'VAT', 'Stamp Duty'],
            'Interest & Rewards': ['Interest', 'Cashback', 'Bonus', 'Reward'],
            'Food & Delivery': ['Food', 'Restaurant', 'Jumia Food', 'Chowdeck', 'Glovo'],
            'Transport': ['Uber', 'Bolt', 'Transport', 'Fuel', 'Petrol', 'Ride'],
            'Betting & Gaming': ['Bet', 'Sporty', 'Betnaija', 'Betway', '1xbet', 'Bet9ja'],
        }

        def categorize(desc):
            if pd.isna(desc):
                return 'Other'
            desc_lower = str(desc).lower()
            for category, keywords in categories.items():
                for keyword in keywords:
                    if keyword.lower() in desc_lower:
                        return category
            return 'Other'

        df = self.df.copy()
        df['category'] = df['description'].apply(categorize)

        summary = df.groupby('category').agg({
            'debit': 'sum',
            'credit': 'sum',
            'description': 'count'
        }).rename(columns={'description': 'count'})

        summary = summary.sort_values('debit', ascending=False)

        result = {}
        for cat, row in summary.iterrows():
            result[cat] = {
                'debit': f"₦{row['debit']:,.2f}",
                'credit': f"₦{row['credit']:,.2f}",
                'count': int(row['count']),
                'debit_raw': row['debit'],
                'credit_raw': row['credit']
            }

        return result

    def get_monthly_breakdown(self):
        """Returns monthly income vs expenses breakdown."""
        if self.monthly_df is None or self.monthly_df.empty:
            return []

        result = []
        for _, row in self.monthly_df.iterrows():
            net = row['credit'] - row['debit']
            result.append({
                'month': row['month_year'],
                'debit': f"₦{row['debit']:,.2f}",
                'credit': f"₦{row['credit']:,.2f}",
                'net': f"₦{net:,.2f}",
                'transaction_count': int(row['count']),
                'debit_raw': row['debit'],
                'credit_raw': row['credit'],
                'net_raw': net
            })

        return result

    def get_recent_transactions(self, limit: int = 20):
        """Returns most recent transactions."""
        if self.df is None or self.df.empty:
            return []

        recent = self.df.sort_values('trans_date', ascending=False).head(limit)

        result = []
        for _, row in recent.iterrows():
            result.append({
                'date': row['trans_date'].strftime('%d %b %Y %H:%M'),
                'description': row['description'],
                'debit': f"₦{row['debit']:,.2f}" if row['debit'] > 0 else '--',
                'credit': f"₦{row['credit']:,.2f}" if row['credit'] > 0 else '--',
                'balance': f"₦{row['balance']:,.2f}" if 'balance' in row and row['balance'] > 0 else '--',
                'channel': row.get('channel', 'N/A')
            })

        return result

    def search_transactions(self, query: str, limit: int = 50):
        """Search transactions by description."""
        if self.df is None or self.df.empty:
            return []

        mask = self.df['description'].str.contains(query, case=False, na=False)
        results = self.df[mask].head(limit)

        return self.df[mask].head(limit).to_dict('records')
    

    def get_top_recipients(self, top_n=10):
        if self.df is None or self.df.empty:
            return []

        df = self.df.copy()

        # Filter for actual debit transactions > 0
        df = df[df['debit'] > 0]

        if df.empty:
            return []

        # Remove internal/automatic debits
        ignore_keywords = [
            'OWealth', 'Auto-save', 'Interest', 'Internal', 'Reversal', 'Cashback', 'Bonus'
        ]
        for kw in ignore_keywords:
            df = df[~df['description'].str.contains(kw, case=False, na=False)]

        if df.empty:
            return []

        # Extract recipient from description
        def extract_recipient(desc):
            if pd.isna(desc):
                return 'Unknown'
            desc = str(desc)
            # Patterns for common payment descriptions
            patterns = [
                r'Transfer to\s+(.*)',
                r'Payment to\s+(.*)',
                r'Sent to\s+(.*)',
                r'Withdrawal\((.*?)\)',
            ]
            for pat in patterns:
                match = re.search(pat, desc, re.IGNORECASE)
                if match:
                    return match.group(1).split('(')[0].split('-')[0].strip()
            return desc[:25]  # fallback: first 25 chars

        df['recipient'] = df['description'].apply(extract_recipient)

        # Group by recipient and sum debit amounts
        top_df = df.groupby('recipient')['debit'].sum().reset_index()
        top_df = top_df.sort_values('debit', ascending=False).head(top_n)

        # Return as list of dicts
        return [{"name": row['recipient'], "amount": row['debit']} for _, row in top_df.iterrows()]

