from dotenv import load_dotenv
from groq import Groq
import json
import hashlib
import os

# Load environment variables
load_dotenv()

# ===============================
# AI RESPONSE CACHING
# ===============================
CACHE_DIR = "ai_cache"
os.makedirs(CACHE_DIR, exist_ok=True)


def get_cache_key(stats: dict) -> str:
    """
    Creates a unique hash for a stats dictionary
    so identical data reuses the same AI response.
    """
    raw = json.dumps(stats, sort_keys=True, default=str)
    return hashlib.md5(raw.encode()).hexdigest()


class AIFinancialAdvisor:
    """
    AI-powered financial advisor using Groq (FREE).
    Flexible enough for general conversation.
    """

    def __init__(self):
        api_key = os.getenv("GROQ_API_KEY")

        if not api_key:
            print("❌ GROQ_API_KEY not found in environment.")
            self.client = None
            return

        try:
            self.client = Groq(api_key=api_key)
            print("✅ Groq AI client initialized successfully.")
        except Exception as e:
            print(f"❌ Failed to initialize Groq client: {e}")
            self.client = None

    def generate_advice(self, stats_dict: dict, user_message: str = None) -> str:
        """
        Generates concise, actionable financial advice
        based on stats_dict. Can also respond to general
        user queries if user_message is provided.
        """

        # ---------- CHECK CACHE FIRST ----------
        cache_key = get_cache_key(stats_dict)
        cache_file = f"{CACHE_DIR}/{cache_key}.txt"

        if os.path.exists(cache_file) and not user_message:
            with open(cache_file, "r", encoding="utf-8") as f:
                return f.read()

        if not self.client:
            return "⚠️ AI service is not configured properly."

        # ---------- EXTRACT STATS SAFELY ----------
        total_income = stats_dict.get("total_in", "N/A")
        total_spent = stats_dict.get("total_out", "N/A")
        net_balance = stats_dict.get("net", "N/A")
        data_spending = stats_dict.get("data_airtime_spend", "N/A")
        data_pct = stats_dict.get("data_pct", "N/A")
        avg_transaction = stats_dict.get("avg_spend", "N/A")
        weekend_spending_pct = stats_dict.get("weekend_pct", "N/A")
        busiest_day = stats_dict.get("busiest_day", "N/A")
        busiest_day_amount = stats_dict.get("busiest_day_amount", "N/A")
        top_month = stats_dict.get("top_spending_month", "N/A")

        stats_summary = f"""
Total Income: {total_income}
Total Spent: {total_spent}
Net Balance: {net_balance}
Data & Airtime Spending: {data_spending} ({data_pct})
Average Transaction: {avg_transaction}
Weekend Spending Percentage: {weekend_spending_pct}
Busiest Day: {busiest_day} ({busiest_day_amount})
Top Spending Month: {top_month}
"""

        # ---------- CREATE SYSTEM PROMPT ----------
        system_prompt = (
            "You are FinBuddy, a virtual assistant whose primary role is to provide "
            "personal financial advice. Analyze income, spending, and budgeting patterns "
            "and give actionable guidance.\n\n"
            "However, you are friendly and flexible:\n"
            "- If the user greets you, respond naturally.\n"
            "- If the user asks general questions unrelated to finance, respond helpfully "
            "or conversationally.\n"
            "- Only provide financial advice when the query relates to money, expenses, "
            "budgeting, or investments.\n\n"
            "Maintain a professional but approachable tone, even in casual conversation."
        )

        # ---------- CREATE USER MESSAGE ----------
        if user_message:
            user_content = (
                f"{user_message}\n\nIf relevant, also consider the following financial data:\n"
                f"{stats_summary}"
            )
        else:
            user_content = (
                "Based on the following financial summary, give helpful advice.\n\n"
                "Focus on:\n"
                "- Saving opportunities\n"
                "- Spending habits\n"
                "- Budgeting tips\n"
                "- Specific observations from the data\n\n"
                "Keep it concise (3–4 short paragraphs).\n\n"
                f"Financial Summary:\n{stats_summary}"
            )

        try:
            response = self.client.chat.completions.create(
                model="llama-3.3-70b-versatile",  # Fast & Free
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content},
                ],
                temperature=0.7,
            )

            advice = response.choices[0].message.content.strip()

            # ---------- SAVE TO CACHE (ONLY FOR FINANCIAL DEFAULT REQUESTS) ----------
            if not user_message:
                with open(cache_file, "w", encoding="utf-8") as f:
                    f.write(advice)

            return advice

        except Exception as e:
            print("❌ Groq AI error:", e)
            return "⚠️ AI advice could not be generated at the moment."
