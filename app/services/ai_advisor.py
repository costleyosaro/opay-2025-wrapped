# ================================================================
# ai_advisor.py — Groq-powered financial advisor (PRODUCTION FIX)
#
# FIX: Key is now checked at call-time, not just init-time.
#      Works reliably on Railway, Render, Heroku, etc.
# ================================================================

from groq import Groq
import json
import hashlib
import os

# Try loading .env for local dev (ignored in production)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed — that's fine in production

# ── Cache directory ──
CACHE_DIR = "ai_cache"
os.makedirs(CACHE_DIR, exist_ok=True)


def _cache_key(data: dict) -> str:
    raw = json.dumps(data, sort_keys=True, default=str)
    return hashlib.md5(raw.encode()).hexdigest()


class AIFinancialAdvisor:
    """
    AI financial advisor using Groq.
    Key is loaded fresh each time to handle Railway env var injection.
    """

    def __init__(self):
        self.client = None
        self._init_client()

    def _init_client(self):
        """Try to initialize the Groq client. Can be retried."""
        api_key = os.environ.get("GROQ_API_KEY", "").strip()

        if not api_key:
            print("⚠️  GROQ_API_KEY not found in environment at init time.")
            print(f"    Available env vars: {[k for k in os.environ.keys() if 'GROQ' in k.upper() or 'API' in k.upper()]}")
            self.client = None
            return

        try:
            self.client = Groq(api_key=api_key)
            print(f"✅ Groq client initialized. Key starts with: {api_key[:8]}...")
        except Exception as e:
            print(f"⚠️  Failed to init Groq: {e}")
            self.client = None

    def _ensure_client(self):
        """
        Lazy retry — if client wasn't ready at startup,
        try again now (Railway may have injected the var late).
        """
        if self.client is None:
            self._init_client()
        return self.client is not None

    # ────────────────────────────────────────────────────────────
    #  STATS EXTRACTION
    # ────────────────────────────────────────────────────────────

    @staticmethod
    def _extract_stat(d: dict, *keys, fallback="N/A"):
        for k in keys:
            val = d.get(k)
            if val is not None and str(val).strip() not in ("", "N/A", "None"):
                return val
        return fallback

    def _build_summary(self, stats: dict) -> str:
        total_income    = self._extract_stat(stats, "total_in",           "totalIncomeFormatted")
        total_spent     = self._extract_stat(stats, "total_out",          "totalExpenseFormatted")
        net_balance     = self._extract_stat(stats, "net",                "totalBalanceFormatted")
        data_spending   = self._extract_stat(stats, "data_airtime_spend", "dataAirtimeSpend")
        data_pct        = self._extract_stat(stats, "data_pct",           "dataPct")
        avg_transaction = self._extract_stat(stats, "avg_spend",          "avgSpend")
        weekend_pct     = self._extract_stat(stats, "weekend_pct",        "weekendPct")
        busiest_day     = self._extract_stat(stats, "busiest_day",        "busiestDay")
        busiest_amount  = self._extract_stat(stats, "busiest_day_amount", "busiestDayAmount")
        top_month       = self._extract_stat(stats, "top_spending_month", "topMonth")

        return (
            f"Total Income: {total_income}\n"
            f"Total Spent: {total_spent}\n"
            f"Net Balance: {net_balance}\n"
            f"Data & Airtime Spending: {data_spending} ({data_pct})\n"
            f"Average Transaction: {avg_transaction}\n"
            f"Weekend Spending Percentage: {weekend_pct}\n"
            f"Busiest Day: {busiest_day} ({busiest_amount})\n"
            f"Top Spending Month: {top_month}\n"
        )

    # ────────────────────────────────────────────────────────────
    #  MAIN GENERATE METHOD
    # ────────────────────────────────────────────────────────────

    def generate_advice(self, stats_dict: dict, user_message: str = None) -> str:
        """
        Generate financial advice or respond to a user question.
        """

        # ── Check cache for default advice ──
        cache_key  = _cache_key(stats_dict)
        cache_file = os.path.join(CACHE_DIR, f"{cache_key}.txt")

        if not user_message and os.path.exists(cache_file):
            with open(cache_file, "r", encoding="utf-8") as f:
                return f.read()

        # ── Ensure client is ready (lazy retry) ──
        if not self._ensure_client():
            # Last resort: try reading the key one more time
            print("❌ Final attempt to read GROQ_API_KEY...")
            print(f"   os.environ keys containing 'groq': {[k for k in os.environ if 'groq' in k.lower()]}")
            print(f"   os.getenv result: '{os.environ.get('GROQ_API_KEY', '<NOT SET>')}'")
            return (
                "⚠️ AI service is not configured. "
                "The GROQ_API_KEY environment variable was not found. "
                "Please check your Railway project variables."
            )

        # ── Build prompt ──
        stats_summary = self._build_summary(stats_dict)

        system_prompt = (
            "You are FinBuddy, a friendly virtual financial advisor. "
            "Your primary role is to analyze income, spending, and budgeting "
            "patterns and give actionable, concise guidance.\n\n"
            "Rules:\n"
            "- If the user greets you, respond naturally and warmly.\n"
            "- If the user asks something unrelated to finance, respond "
            "helpfully or redirect gently.\n"
            "- When giving financial advice, be specific and reference "
            "the numbers from the data.\n"
            "- Use emoji sparingly for friendliness.\n"
            "- Keep responses concise (3-5 short paragraphs max).\n"
            "- Format currency in Naira (₦).\n"
        )

        if user_message:
            user_content = (
                f"User question: {user_message}\n\n"
                f"Here is their financial data for context:\n{stats_summary}"
            )
        else:
            user_content = (
                "Based on the following financial summary, give a helpful "
                "analysis with specific advice.\n\n"
                "Focus on:\n"
                "- Key observations from the numbers\n"
                "- Saving opportunities\n"
                "- Spending habit insights\n"
                "- One actionable tip\n\n"
                f"Financial Summary:\n{stats_summary}"
            )

        # ── Call Groq API ──
        try:
            response = self.client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_content},
                ],
                temperature=0.7,
                max_tokens=1024,
            )

            advice = response.choices[0].message.content.strip()

            # Cache default advice only
            if not user_message:
                try:
                    with open(cache_file, "w", encoding="utf-8") as f:
                        f.write(advice)
                except OSError:
                    pass  # cache write failure is non-critical

            return advice

        except Exception as e:
            print(f"⚠️  Groq API error: {e}")
            return "⚠️ AI advice could not be generated right now. Please try again."