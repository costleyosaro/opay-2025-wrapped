# ================================================================
# analyzer.py — OPay statement parser & analytics (FIXED)
#
# FIXES APPLIED:
#  1. Added net_raw, total_in_raw, total_out_raw to get_fun_stats()
#     (main.py referenced them but they didn't exist → always 0)
#  2. Removed duplicate weekday debug block (was printing to console)
#  3. Removed all debug print() statements
#  4. Added self.last_stats so /chat can access original stats
#  5. Fixed search_transactions to return JSON-serializable data
#  6. Added docstrings to all public methods
# ================================================================

import pandas as pd
import numpy as np
from datetime import datetime
import re


class DataAnalyzer:
    """
    Handles uploading, parsing, and analyzing OPay transaction data.
    Designed for the standard OPay Excel/CSV export format.
    """

    def __init__(self):
        self.df = None
        self.monthly_df = None
        self.metadata = {}
        self.last_stats = {}          # ← NEW: stores most recent stats for /chat

    # ============================================================
    # INTERNAL HELPERS
    # ============================================================

    def _extract_metadata(self, df):
        """
        Extracts account metadata from the header rows of an OPay statement.
        Looks for: Account Name, Account Number, Account Type, Period,
                   Opening/Closing Balance, Total Debit/Credit, Debit/Credit Count.
        """
        metadata = {}

        try:
            for i in range(min(len(df), 10)):
                row = [str(val).strip() for val in df.iloc[i].values]
                row_text = " ".join(row)

                if "Account Name" in row_text:
                    for j, val in enumerate(row):
                        if val == "Account Name" and j + 1 < len(row):
                            metadata["account_name"] = row[j + 1]
                        if val == "Account Number" and j + 1 < len(row):
                            metadata["account_number"] = row[j + 1]

                if "Account Type" in row_text:
                    for j, val in enumerate(row):
                        if val == "Account Type" and j + 1 < len(row):
                            metadata["account_type"] = row[j + 1]
                        if val == "Period" and j + 1 < len(row):
                            metadata["period"] = row[j + 1]

                if "Opening Balance" in row_text:
                    for j, val in enumerate(row):
                        if val == "Opening Balance" and j + 1 < len(row):
                            metadata["opening_balance"] = row[j + 1]
                        if val == "Total Debit" and j + 1 < len(row):
                            metadata["total_debit"] = row[j + 1]
                        if val == "Debit Count" and j + 1 < len(row):
                            metadata["debit_count"] = row[j + 1]

                if "Closing Balance" in row_text:
                    for j, val in enumerate(row):
                        if val == "Closing Balance" and j + 1 < len(row):
                            metadata["closing_balance"] = row[j + 1]
                        if val == "Total Credit" and j + 1 < len(row):
                            metadata["total_credit"] = row[j + 1]
                        if val == "Credit Count" and j + 1 < len(row):
                            metadata["credit_count"] = row[j + 1]

                if "Date Printed" in row_text:
                    for j, val in enumerate(row):
                        if val == "Date Printed" and j + 1 < len(row):
                            metadata["date_printed"] = row[j + 1]

        except Exception:
            pass  # metadata is best-effort

        return metadata

    def _find_header_row(self, df):
        """Finds the row index containing 'Trans. Date'."""
        for i in range(min(len(df), 20)):
            row_values = [str(val).strip() for val in df.iloc[i].values]
            if "Trans. Date" in row_values:
                return i
        return -1

    def _clean_currency(self, value):
        """Converts ₦-formatted currency strings to float."""
        if pd.isna(value):
            return 0.0
        val_str = str(value).strip()
        if val_str in ("--", "-", "", "nan", "None", "NaN"):
            return 0.0
        cleaned = re.sub(r"[₦,\s\"']", "", val_str)
        try:
            return float(cleaned)
        except ValueError:
            return 0.0

    def _parse_datetime(self, date_str):
        """Parses OPay date formats into datetime objects."""
        if pd.isna(date_str):
            return None
        date_str = str(date_str).strip()
        formats = [
            "%d %b %Y %H:%M:%S",
            "%d %b %Y",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return None

    def _extract_recipient(self, desc):
        """Extracts a clean recipient name from a transaction description."""
        if pd.isna(desc):
            return "Unknown"
        desc = str(desc)
        prefixes = [
            r"^Transfer to\s*",
            r"^Payment to\s*",
            r"^Sent to\s*",
            r"^Transfer from\s*",
            r"^Payment from\s*",
            r"^Received from\s*",
        ]
        name = desc
        for pattern in prefixes:
            name = re.sub(pattern, "", name, flags=re.IGNORECASE)
        name = re.split(r"[-/|()]", name)[0].strip()
        return name[:30] if len(name) > 30 else name

    # ============================================================
    # MAIN PROCESSING
    # ============================================================

    def process_data(self, file_path: str):
        """
        Read and process an OPay statement file.
        Returns (success: bool, message: str).
        """
        try:
            # Read raw file
            if file_path.endswith(".csv"):
                df = pd.read_csv(file_path, header=None, on_bad_lines="skip")
            elif file_path.endswith((".xlsx", ".xls")):
                df = pd.read_excel(file_path, header=None)
            else:
                return False, "Unsupported format. Upload CSV or Excel."

            # Metadata from header rows
            self.metadata = self._extract_metadata(df)

            # Locate transaction table header
            header_row = self._find_header_row(df)
            if header_row == -1:
                return False, "Could not find transaction table (expected 'Trans. Date' column)."

            df.columns = [str(c).strip() for c in df.iloc[header_row].values]
            df = df.iloc[header_row + 1 :].reset_index(drop=True)

            # Column mapping
            column_mapping = {
                "Trans. Date":             "trans_date",
                "Value Date":              "value_date",
                "Description":             "description",
                "Debit(₦)":               "debit",
                "Credit(₦)":              "credit",
                "Balance After(₦)":        "balance",
                "Channel":                 "channel",
                "Transaction Reference":   "reference",
            }

            # Validate required columns
            required = ["Trans. Date", "Description", "Debit(₦)", "Credit(₦)"]
            missing = [c for c in required if c not in df.columns]
            if missing:
                return False, f"Missing required columns: {', '.join(missing)}"

            df = df.rename(columns=column_mapping)

            # Clean currency
            df["debit"]  = df["debit"].apply(self._clean_currency)
            df["credit"] = df["credit"].apply(self._clean_currency)
            if "balance" in df.columns:
                df["balance"] = df["balance"].apply(self._clean_currency)

            # Parse dates
            df["trans_date"] = df["trans_date"].apply(self._parse_datetime)
            df = df.dropna(subset=["trans_date"])
            if df.empty:
                return False, "No valid transactions found after parsing dates."

            if "value_date" in df.columns:
                df["value_date"] = df["value_date"].apply(self._parse_datetime)

            # Sort chronologically
            df = df.sort_values("trans_date").reset_index(drop=True)

            # Derived columns
            df["month_year"] = df["trans_date"].dt.strftime("%b %Y")
            df["day_name"]   = df["trans_date"].dt.day_name()
            df["hour"]       = df["trans_date"].dt.hour
            df["date_only"]  = df["trans_date"].dt.date
            df["week"]       = df["trans_date"].dt.isocalendar().week.astype(int)

            self.df = df

            # Monthly aggregation
            self.monthly_df = (
                df.groupby("month_year", sort=False)
                .agg({"debit": "sum", "credit": "sum", "description": "count"})
                .rename(columns={"description": "count"})
                .reset_index()
            )

            return True, f"Successfully processed {len(df)} transactions."

        except Exception as e:
            return False, f"Error processing file: {str(e)}"

    # ============================================================
    # GETTERS
    # ============================================================

    def get_account_info(self):
        """Returns extracted account metadata dict."""
        return self.metadata

    def get_date_range(self):
        """Returns start/end date and span in days."""
        if self.df is None or self.df.empty:
            return None
        return {
            "start": self.df["trans_date"].min().strftime("%d %b %Y"),
            "end":   self.df["trans_date"].max().strftime("%d %b %Y"),
            "days":  (self.df["trans_date"].max() - self.df["trans_date"].min()).days,
        }

    # ============================================================
    # ANALYTICS
    # ============================================================

    def get_summary_stats(self):
        """Basic summary statistics."""
        if self.df is None or self.df.empty:
            return {}
        df = self.df
        total_debit  = df["debit"].sum()
        total_credit = df["credit"].sum()
        net_flow     = total_credit - total_debit

        return {
            "total_transactions": len(df),
            "debit_count":        len(df[df["debit"] > 0]),
            "credit_count":       len(df[df["credit"] > 0]),
            "total_debit":        f"₦{total_debit:,.2f}",
            "total_credit":       f"₦{total_credit:,.2f}",
            "net_flow":           f"₦{net_flow:,.2f}",
            "total_debit_raw":    total_debit,
            "total_credit_raw":   total_credit,
            "net_flow_raw":       net_flow,
        }

    def get_fun_stats(self):
        """
        Comprehensive financial analytics used by both the dashboard
        and the AI advisor.

        Returns a dict with formatted strings AND raw floats so the
        frontend and advisor can both consume the data correctly.
        """
        df = self.df
        if df is None or df.empty:
            return {}

        total_out = df["debit"].sum()
        total_in  = df["credit"].sum()
        net       = total_in - total_out

        # ---- TOP RECIPIENTS (for stats display) ----
        ignore_kw = [
            "OWealth", "Auto-save", "Interest", "Withdrawal(Transaction",
            "Internal", "Reversal", "Cashback", "Bonus",
        ]
        payments = df[df["debit"] > 0].copy()
        for kw in ignore_kw:
            payments = payments[
                ~payments["description"].str.contains(kw, case=False, na=False, regex=False)
            ]

        if not payments.empty:
            payments["recipient"] = payments["description"].apply(self._extract_recipient)
            top_agg = (
                payments.groupby("recipient")["debit"]
                .agg(["sum", "count"])
                .sort_values("sum", ascending=False)
                .head(5)
            )
            top_people = {
                name: {"total": f"₦{row['sum']:,.2f}", "count": int(row["count"])}
                for name, row in top_agg.iterrows()
            }
        else:
            top_people = {}

        # ---- AVERAGES ----
        debit_txns  = df[df["debit"] > 0]
        credit_txns = df[df["credit"] > 0]
        avg_spend   = debit_txns["debit"].mean()  if not debit_txns.empty  else 0
        avg_income  = credit_txns["credit"].mean() if not credit_txns.empty else 0

        # ---- WEEKEND / WEEKDAY ----
        weekend_mask  = df["day_name"].isin(["Saturday", "Sunday"])
        weekend_spend = df[weekend_mask]["debit"].sum()
        weekday_spend = df[~weekend_mask]["debit"].sum()
        weekend_pct   = (weekend_spend / total_out * 100) if total_out > 0 else 0

        # ---- BUSIEST DAY ----
        daily_spending = df.groupby("date_only")["debit"].sum()
        if not daily_spending.empty:
            busiest_day    = daily_spending.idxmax()
            busiest_amount = daily_spending.max()
        else:
            busiest_day    = None
            busiest_amount = 0

        # ---- DATA & AIRTIME ----
        data_keywords = [
            "Mobile Data", "Data Purchase", "Airtime", "VTU",
            "MTN", "GLO", "Airtel", "9mobile", "DSTV", "GOTV",
        ]
        data_mask  = df["description"].str.contains(
            "|".join(data_keywords), case=False, na=False
        )
        data_spend = df[data_mask & (df["debit"] > 0)]["debit"].sum()
        data_pct   = (data_spend / total_out * 100) if total_out > 0 else 0

        # ---- SPENDING BY DAY OF WEEK ----
        day_order = [
            "Monday", "Tuesday", "Wednesday", "Thursday",
            "Friday", "Saturday", "Sunday",
        ]
        weekday_grouped = (
            df[df["debit"] > 0]
            .groupby("day_name")
            .agg(total_spent=("debit", "sum"), transaction_count=("debit", "count"))
            .reindex(day_order, fill_value=0)
        )
        weekday_breakdown = [
            {
                "day":               day,
                "total_spent":       float(row["total_spent"]),
                "transaction_count": int(row["transaction_count"]),
            }
            for day, row in weekday_grouped.iterrows()
        ]

        # ---- SPENDING BY HOUR ----
        spending_by_hour = df.groupby("hour")["debit"].sum().to_dict()

        # ---- CHANNEL BREAKDOWN ----
        channel_spending = (
            df.groupby("channel")["debit"].sum().to_dict()
            if "channel" in df.columns
            else {}
        )

        # ---- TOP SPENDING MONTH ----
        if self.monthly_df is not None and not self.monthly_df.empty:
            top_idx          = self.monthly_df["debit"].idxmax()
            top_month        = self.monthly_df.loc[top_idx, "month_year"]
            top_month_amount = self.monthly_df.loc[top_idx, "debit"]
        else:
            top_month        = "N/A"
            top_month_amount = 0

        # ---- TRANSACTION FREQUENCY ----
        date_range     = (df["trans_date"].max() - df["trans_date"].min()).days
        avg_txns_daily = len(df) / date_range if date_range > 0 else len(df)

        # ---- BUILD RESULT ----
        result = {
            # Formatted strings
            "total_out":           f"₦{total_out:,.2f}",
            "total_in":            f"₦{total_in:,.2f}",
            "net":                 f"₦{net:,.2f}",
            "net_status":          "positive" if net >= 0 else "negative",

            # ★ RAW NUMBERS (FIX: these were missing → main.py got 0 for all)
            "total_out_raw":       total_out,
            "total_in_raw":        total_in,
            "net_raw":             net,

            # Counts
            "transaction_count":   len(df),
            "debit_count":         len(debit_txns),
            "credit_count":        len(credit_txns),

            # Averages
            "avg_spend":           f"₦{avg_spend:,.2f}",
            "avg_income":          f"₦{avg_income:,.2f}",
            "avg_txns_per_day":    f"{avg_txns_daily:.1f}",

            # Top recipients (compact)
            "top_recipients":      top_people,

            # Weekend / weekday
            "weekend_spend":       f"₦{weekend_spend:,.2f}",
            "weekday_spend":       f"₦{weekday_spend:,.2f}",
            "weekend_pct":         f"{weekend_pct:.1f}%",
            "weekday_breakdown":   weekday_breakdown,

            # Busiest day
            "busiest_day":         (
                busiest_day.strftime("%d %b %Y") if busiest_day else "N/A"
            ),
            "busiest_day_amount":  f"₦{busiest_amount:,.2f}",

            # Data & airtime
            "data_airtime_spend":  f"₦{data_spend:,.2f}",
            "data_pct":            f"{data_pct:.1f}%",

            # Monthly
            "top_spending_month":  top_month,
            "top_month_amount":    f"₦{top_month_amount:,.2f}",

            # Chart data
            "spending_by_hour":    spending_by_hour,
            "spending_by_channel": channel_spending,
            "monthly_data":        (
                self.monthly_df.to_dict("records")
                if self.monthly_df is not None
                else []
            ),
        }

        # ★ Store for /chat endpoint to use (FIX: chat was getting wrong keys)
        self.last_stats = result

        return result

    # ============================================================
    # CATEGORY BREAKDOWN
    # ============================================================

    def get_transaction_categories(self):
        """
        Categorizes transactions by description keywords.
        Returns {category: {debit, credit, count, debit_raw, credit_raw}}.
        """
        if self.df is None or self.df.empty:
            return {}

        categories = {
            "Transfers":         ["Transfer to", "Transfer from", "Sent to", "Received from"],
            "OWealth/Savings":   ["OWealth", "Auto-save", "Savings"],
            "Data & Airtime":    ["Mobile Data", "Airtime", "VTU", "MTN", "GLO", "Airtel", "9mobile"],
            "Bills & Utilities": ["Electricity", "PHCN", "DSTV", "GOTV", "Startimes", "Cable", "Water", "NEPA"],
            "POS Transactions":  ["POS", "Terminal", "ATM"],
            "Bank Charges":      ["Charge", "Fee", "Commission", "VAT", "Stamp Duty"],
            "Interest & Rewards":["Interest", "Cashback", "Bonus", "Reward"],
            "Food & Delivery":   ["Food", "Restaurant", "Jumia Food", "Chowdeck", "Glovo"],
            "Transport":         ["Uber", "Bolt", "Transport", "Fuel", "Petrol", "Ride"],
            "Betting & Gaming":  ["Bet", "Sporty", "Betnaija", "Betway", "1xbet", "Bet9ja"],
        }

        def categorize(desc):
            if pd.isna(desc):
                return "Other"
            desc_lower = str(desc).lower()
            for cat, keywords in categories.items():
                if any(kw.lower() in desc_lower for kw in keywords):
                    return cat
            return "Other"

        df = self.df.copy()
        df["category"] = df["description"].apply(categorize)

        summary = (
            df.groupby("category")
            .agg({"debit": "sum", "credit": "sum", "description": "count"})
            .rename(columns={"description": "count"})
            .sort_values("debit", ascending=False)
        )

        return {
            cat: {
                "debit":      f"₦{row['debit']:,.2f}",
                "credit":     f"₦{row['credit']:,.2f}",
                "count":      int(row["count"]),
                "debit_raw":  float(row["debit"]),
                "credit_raw": float(row["credit"]),
            }
            for cat, row in summary.iterrows()
        }

    # ============================================================
    # MONTHLY BREAKDOWN
    # ============================================================

    def get_monthly_breakdown(self):
        """Returns monthly income vs expenses for the bar chart."""
        if self.monthly_df is None or self.monthly_df.empty:
            return []

        result = []
        for _, row in self.monthly_df.iterrows():
            net = row["credit"] - row["debit"]
            result.append({
                "month":             row["month_year"],
                "debit":             f"₦{row['debit']:,.2f}",
                "credit":            f"₦{row['credit']:,.2f}",
                "net":               f"₦{net:,.2f}",
                "transaction_count": int(row["count"]),
                "debit_raw":         float(row["debit"]),
                "credit_raw":        float(row["credit"]),
                "net_raw":           float(net),
            })
        return result

    # ============================================================
    # RECENT TRANSACTIONS
    # ============================================================

    def get_recent_transactions(self, limit: int = 20):
        """Returns the most recent transactions as JSON-safe dicts."""
        if self.df is None or self.df.empty:
            return []

        recent = self.df.sort_values("trans_date", ascending=False).head(limit)

        result = []
        for _, row in recent.iterrows():
            result.append({
                "date":        row["trans_date"].strftime("%d %b %Y %H:%M"),
                "description": str(row["description"]),
                "debit":       f"₦{row['debit']:,.2f}" if row["debit"] > 0 else "--",
                "credit":      f"₦{row['credit']:,.2f}" if row["credit"] > 0 else "--",
                "balance":     (
                    f"₦{row['balance']:,.2f}"
                    if "balance" in row.index and row.get("balance", 0) > 0
                    else "--"
                ),
                "channel":     str(row.get("channel", "N/A")),
            })
        return result

    # ============================================================
    # SEARCH
    # ============================================================

    def search_transactions(self, query: str, limit: int = 50):
        """
        Search transactions by description.
        Returns JSON-serializable list of dicts.
        """
        if self.df is None or self.df.empty:
            return []

        mask    = self.df["description"].str.contains(query, case=False, na=False)
        results = self.df[mask].head(limit)

        # Convert to JSON-safe format (FIX: raw .to_dict had datetime objects)
        output = []
        for _, row in results.iterrows():
            output.append({
                "date":        row["trans_date"].strftime("%d %b %Y %H:%M"),
                "description": str(row["description"]),
                "debit":       float(row["debit"]),
                "credit":      float(row["credit"]),
                "balance":     float(row.get("balance", 0)),
                "channel":     str(row.get("channel", "N/A")),
            })
        return output

    # ============================================================
    # TOP RECIPIENTS
    # ============================================================

    def get_top_recipients(self, top_n: int = 10):
        """
        Returns the top N recipients by total debit amount.
        Output: [{"name": "...", "amount": 12345.67}, ...]
        """
        if self.df is None or self.df.empty:
            return []

        df = self.df[self.df["debit"] > 0].copy()
        if df.empty:
            return []

        # Filter out internal / automated debits
        ignore_kw = [
            "OWealth", "Auto-save", "Interest",
            "Internal", "Reversal", "Cashback", "Bonus",
        ]
        for kw in ignore_kw:
            df = df[~df["description"].str.contains(kw, case=False, na=False)]
        if df.empty:
            return []

        def extract(desc):
            if pd.isna(desc):
                return "Unknown"
            desc = str(desc)
            patterns = [
                r"Transfer to\s+(.*)",
                r"Payment to\s+(.*)",
                r"Sent to\s+(.*)",
                r"Withdrawal\((.*?)\)",
            ]
            for pat in patterns:
                match = re.search(pat, desc, re.IGNORECASE)
                if match:
                    return match.group(1).split("(")[0].split("-")[0].strip()[:30]
            return desc[:30]

        df["recipient"] = df["description"].apply(extract)

        top = (
            df.groupby("recipient")["debit"]
            .sum()
            .reset_index()
            .sort_values("debit", ascending=False)
            .head(top_n)
        )

        return [
            {"name": row["recipient"], "amount": float(row["debit"])}
            for _, row in top.iterrows()
        ]