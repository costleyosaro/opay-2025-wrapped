# ================================================================
# ai_advisor.py — NUCLEAR FIX
# Exhaustive environment variable loading with full debug output
# ================================================================

import json
import hashlib
import os
import sys

# ── Try every possible way to load .env ──
def _load_env():
    """
    Tries multiple methods to find and load GROQ_API_KEY.
    """
    
    # Method 1: Already in environment (Railway, Docker, etc.)
    key = os.environ.get("GROQ_API_KEY", "").strip()
    if key:
        print(f"✅ GROQ_API_KEY found via os.environ (len={len(key)}, starts={key[:8]}...)")
        return key

    # Method 2: python-dotenv from current directory
    try:
        from dotenv import load_dotenv
        
        # Try current working directory
        cwd_env = os.path.join(os.getcwd(), ".env")
        if os.path.exists(cwd_env):
            print(f"📂 Found .env at: {cwd_env}")
            load_dotenv(cwd_env, override=True)
            key = os.environ.get("GROQ_API_KEY", "").strip()
            if key:
                print(f"✅ GROQ_API_KEY loaded from {cwd_env} (len={len(key)})")
                return key
        
        # Try relative to this file's directory
        this_dir = os.path.dirname(os.path.abspath(__file__))
        
        # backend/app/services/ → go up to backend/
        paths_to_try = [
            os.path.join(this_dir, ".env"),                          # services/.env
            os.path.join(this_dir, "..", ".env"),                     # app/.env
            os.path.join(this_dir, "..", "..", ".env"),               # backend/.env
            os.path.join(this_dir, "..", "..", "..", ".env"),         # project root/.env
        ]
        
        for env_path in paths_to_try:
            env_path = os.path.abspath(env_path)
            if os.path.exists(env_path):
                print(f"📂 Found .env at: {env_path}")
                load_dotenv(env_path, override=True)
                key = os.environ.get("GROQ_API_KEY", "").strip()
                if key:
                    print(f"✅ GROQ_API_KEY loaded from {env_path} (len={len(key)})")
                    return key
                else:
                    print(f"⚠️  .env found at {env_path} but GROQ_API_KEY not in it")
                    # Print contents (masked) for debugging
                    try:
                        with open(env_path, "r") as f:
                            lines = f.readlines()
                            for line in lines:
                                line = line.strip()
                                if line and not line.startswith("#"):
                                    k, _, v = line.partition("=")
                                    print(f"    {k.strip()} = {v.strip()[:5]}{'...' if len(v.strip()) > 5 else ''}")
                    except Exception:
                        pass
        
        # Try bare load_dotenv() as last resort
        load_dotenv(override=True)
        key = os.environ.get("GROQ_API_KEY", "").strip()
        if key:
            print(f"✅ GROQ_API_KEY loaded via bare load_dotenv() (len={len(key)})")
            return key
            
    except ImportError:
        print("ℹ️  python-dotenv not installed — skipping .env loading")
    
    # Nothing worked
    print("=" * 60)
    print("❌ GROQ_API_KEY NOT FOUND ANYWHERE")
    print(f"   Current working directory: {os.getcwd()}")
    print(f"   This file location:        {os.path.abspath(__file__)}")
    print(f"   sys.path:                   {sys.path[:3]}")
    print(f"   All env vars with 'KEY':    {[k for k in os.environ if 'KEY' in k.upper()]}")
    print(f"   All env vars with 'GROQ':   {[k for k in os.environ if 'GROQ' in k.upper()]}")
    print(f"   All env vars with 'API':    {[k for k in os.environ if 'API' in k.upper()]}")
    print("=" * 60)
    
    return ""


# Load key at module level
_GROQ_KEY = _load_env()

# Cache
CACHE_DIR = "ai_cache"
os.makedirs(CACHE_DIR, exist_ok=True)


def _cache_key(data: dict) -> str:
    raw = json.dumps(data, sort_keys=True, default=str)
    return hashlib.md5(raw.encode()).hexdigest()


class AIFinancialAdvisor:

    def __init__(self):
        self.client = None
        self._try_init()

    def _try_init(self):
        global _GROQ_KEY
        
        # Re-check environment in case it was set after module load
        if not _GROQ_KEY:
            _GROQ_KEY = os.environ.get("GROQ_API_KEY", "").strip()
        
        if not _GROQ_KEY:
            print("⚠️  AIFinancialAdvisor: No API key available")
            return

        try:
            from groq import Groq
            self.client = Groq(api_key=_GROQ_KEY)
            print(f"✅ Groq client created successfully")
        except ImportError:
            print("❌ groq package not installed! Run: pip install groq")
            self.client = None
        except Exception as e:
            print(f"❌ Groq client error: {e}")
            self.client = None

    def _ensure_client(self):
        if self.client is None:
            self._try_init()
        return self.client is not None

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

    def generate_advice(self, stats_dict: dict, user_message: str = None) -> str:

        cache_key  = _cache_key(stats_dict)
        cache_file = os.path.join(CACHE_DIR, f"{cache_key}.txt")

        if not user_message and os.path.exists(cache_file):
            with open(cache_file, "r", encoding="utf-8") as f:
                return f.read()

        if not self._ensure_client():
            return (
                "⚠️ AI service is not configured. "
                "The GROQ_API_KEY environment variable was not found. "
                "Please check your Railway project variables."
            )

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

            if not user_message:
                try:
                    with open(cache_file, "w", encoding="utf-8") as f:
                        f.write(advice)
                except OSError:
                    pass

            return advice

        except Exception as e:
            print(f"⚠️  Groq API call error: {e}")
            return "⚠️ AI advice could not be generated right now. Please try again."