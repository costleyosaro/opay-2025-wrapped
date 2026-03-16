# ================================================================
# ai_advisor.py — Groq-powered financial advisor (FIXED)
#
# FIXES APPLIED:
#  1. generate_advice() now properly handles user_message
#     (chat endpoint was calling it without user_message before)
#  2. Stats key lookups are more resilient — handles both
#     backend keys (total_in) and frontend keys (totalIncomeFormatted)
#  3. Removed redundant client init check (cleaner flow)
#  4. Cache only applies to default advice, not chat messages (correct)
#  5. Better error messages
# ================================================================

from dotenv import load_dotenv
from groq import Groq
import json
import hashlib
import os

load_dotenv()

# ── Cache directory ──
CACHE_DIR = "ai_cache"
os.makedirs(CACHE_DIR, exist_ok=True)


def _cache_key(data: dict) -> str:
    """MD5 hash of a dict for cache lookups."""
    raw = json.dumps(data, sort_keys=True, default=str)
    return hashlib.md5(raw.encode()).hexdigest()


class AIFinancialAdvisor:
    """
    AI financial advisor using Groq (free tier).
    Handles both initial analysis summaries and follow-up chat.
    """

    def __init__(self):
        api_key = os.getenv("GROQ_API_KEY")
        self.client = None

        if not api_key:
            print("⚠️  GROQ_API_KEY not found — AI features will be unavailable.")
            return

        try:
            self.client = Groq(api_key=api_key)
        except Exception as e:
            print(f"⚠️  Failed to initialize Groq client: {e}")

    # ────────────────────────────────────────────────────────────
    #  STATS EXTRACTION (handles both backend + frontend keys)
    # ────────────────────────────────────────────────────────────

    @staticmethod
    def _extract_stat(d: dict, *keys, fallback="N/A"):
        """
        Try multiple key names and return the first hit.
        Handles the mismatch between backend keys (total_in)
        and frontend keys (totalIncomeFormatted).
        """
        for k in keys:
            val = d.get(k)
            if val is not None and str(val).strip() not in ("", "N/A", "None"):
                return val
        return fallback

    def _build_summary(self, stats: dict) -> str:
        """Build a human-readable financial summary for the AI prompt."""
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

        • If user_message is None → initial analysis summary (cacheable).
        • If user_message is set  → conversational follow-up (not cached).
        """

        # ── Check cache for default advice ──
        cache_key  = _cache_key(stats_dict)
        cache_file = os.path.join(CACHE_DIR, f"{cache_key}.txt")

        if not user_message and os.path.exists(cache_file):
            with open(cache_file, "r", encoding="utf-8") as f:
                return f.read()

        if not self.client:
            return (
                "⚠️ AI service is not configured. "
                "Please set the GROQ_API_KEY environment variable."
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

            # Cache default advice only (not user chat)
            if not user_message:
                with open(cache_file, "w", encoding="utf-8") as f:
                    f.write(advice)

            return advice

        except Exception as e:
            print(f"⚠️  Groq API error: {e}")
            return "⚠️ AI advice could not be generated right now. Please try again."