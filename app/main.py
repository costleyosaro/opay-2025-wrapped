# ================================================================
# main.py — FastAPI entry point (PRODUCTION READY)
# ================================================================

from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import shutil
import os
import traceback

from .services.analyzer import DataAnalyzer
from .services.ai_advisor import AIFinancialAdvisor

# ----------------------------------------------------------------
# APP SETUP
# ----------------------------------------------------------------
app = FastAPI(title="OPay Wrapped")

# Get the directory where main.py lives
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Mount static and templates using absolute paths (works on any server)
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# Global instances
analyzer = DataAnalyzer()
advisor  = AIFinancialAdvisor()

UPLOAD_DIR = os.path.join(BASE_DIR, "temp_uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)


# ----------------------------------------------------------------
# ROUTES
# ----------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/upload", response_class=JSONResponse)
async def upload_file(file: UploadFile = File(...)):
    file_path = None
    try:
        file_path = os.path.join(UPLOAD_DIR, file.filename)
        with open(file_path, "wb") as buf:
            shutil.copyfileobj(file.file, buf)

        success, msg = analyzer.process_data(file_path)
        if not success:
            return JSONResponse({"error": msg}, status_code=400)

        if analyzer.df is None or analyzer.df.empty:
            return JSONResponse(
                {"error": "No valid transactions found in the file."},
                status_code=400,
            )

        stats              = analyzer.get_fun_stats()
        metadata           = analyzer.get_account_info()
        top_recipients     = analyzer.get_top_recipients()
        weekday_breakdown  = stats.get("weekday_breakdown", [])
        categories         = analyzer.get_transaction_categories()
        monthly_breakdown  = analyzer.get_monthly_breakdown()
        recent_txns        = analyzer.get_recent_transactions()

        advice = advisor.generate_advice(stats)

        response_data = {
            "stats": {
                "totalBalance":          stats.get("net_raw", 0),
                "totalIncome":           stats.get("total_in_raw", 0),
                "totalExpense":          stats.get("total_out_raw", 0),
                "totalBalanceFormatted": stats.get("net", "₦0.00"),
                "totalIncomeFormatted":  stats.get("total_in", "₦0.00"),
                "totalExpenseFormatted": stats.get("total_out", "₦0.00"),
                "transactionCount":     stats.get("transaction_count", 0),
                "debitCount":           stats.get("debit_count", 0),
                "creditCount":          stats.get("credit_count", 0),
                "avgSpend":             stats.get("avg_spend", "₦0.00"),
                "avgIncome":            stats.get("avg_income", "₦0.00"),
                "weekendPct":           stats.get("weekend_pct", "0%"),
                "busiestDay":           stats.get("busiest_day", "N/A"),
                "busiestDayAmount":     stats.get("busiest_day_amount", "₦0.00"),
                "topMonth":             stats.get("top_spending_month", "N/A"),
                "topMonthAmount":       stats.get("top_month_amount", "₦0.00"),
                "dataAirtimeSpend":     stats.get("data_airtime_spend", "₦0.00"),
                "dataPct":              stats.get("data_pct", "0%"),
                "closingBalance":       metadata.get("closing_balance", "₦0.00"),
                "openingBalance":       metadata.get("opening_balance", "₦0.00"),
            },
            "categories":        categories,
            "topRecipients":     top_recipients,
            "monthlyBreakdown":  monthly_breakdown,
            "weekdayBreakdown":  weekday_breakdown,
            "recentTransactions": recent_txns,
            "advice":            advice,
        }

        return JSONResponse(response_data)

    except Exception as e:
        traceback.print_exc()
        return JSONResponse(
            {"error": str(e), "traceback": traceback.format_exc()},
            status_code=500,
        )
    finally:
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except OSError:
                pass


@app.post("/chat")
async def chat(request: Request):
    try:
        data         = await request.json()
        user_message = data.get("message", "").strip()

        if not user_message:
            return JSONResponse(
                {"response": "Please type a question and I'll help you out!"}
            )

        stats_for_ai = (
            analyzer.last_stats
            if analyzer.last_stats
            else data.get("stats", {})
        )

        advice = advisor.generate_advice(
            stats_dict=stats_for_ai,
            user_message=user_message,
        )

        return JSONResponse({"response": advice})

    except Exception as e:
        traceback.print_exc()
        return JSONResponse(
            {"error": str(e), "traceback": traceback.format_exc()},
            status_code=500,
        )

@app.get("/debug/env")
def debug_env():
    """
    Temporary endpoint to verify environment variables.
    DELETE THIS after confirming it works!
    """
    groq_key = os.environ.get("GROQ_API_KEY", "")
    return {
        "groq_key_set": bool(groq_key),
        "groq_key_length": len(groq_key),
        "groq_key_preview": groq_key[:8] + "..." if groq_key else "NOT SET",
        "all_env_with_api": [k for k in os.environ.keys() if "API" in k.upper() or "GROQ" in k.upper() or "KEY" in k.upper()],
    }