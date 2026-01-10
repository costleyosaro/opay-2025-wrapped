# main.py (rewritten)

from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import shutil
import os
import traceback

from .services.analyzer import DataAnalyzer
from .services.ai_advisor import AIFinancialAdvisor

app = FastAPI(title="OPay Wrapped Web")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Initialize analyzer globally (safe, non-blocking)
analyzer = DataAnalyzer()


@app.on_event("startup")
def startup_event():
    """
    Optional startup event for logging or future initializations.
    """
    print("FastAPI application starting up...")


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    """
    Render main frontend template.
    """
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/chat")
async def chat(request: Request):
    """
    Generate AI financial advice based on stats sent from frontend.
    """
    try:
        data = await request.json()
        stats_dict = data.get("stats", {})

        # Lazy initialization of AI advisor to avoid blocking startup
        advisor = AIFinancialAdvisor()
        advice = advisor.generate_advice(stats_dict)

        return JSONResponse({"response": advice})

    except Exception as e:
        print("Exception in /chat route:")
        traceback.print_exc()
        return JSONResponse({
            "error": str(e),
            "traceback": traceback.format_exc()
        }, status_code=500)


@app.post("/upload", response_class=JSONResponse)
async def upload_file(file: UploadFile = File(...)):
    """
    Handle file upload, analyze transactions, and return stats + AI advice.
    """
    try:
        # Save uploaded file
        upload_folder = "temp_uploads"
        os.makedirs(upload_folder, exist_ok=True)
        file_path = os.path.join(upload_folder, file.filename)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        print(f"Uploaded file saved to: {file_path}, size: {os.path.getsize(file_path)} bytes")

        # Process file with analyzer
        success, msg = analyzer.process_data(file_path)
        if not success:
            print(f"Process data failed: {msg}")
            return JSONResponse({"error": msg}, status_code=400)

        if analyzer.df is None or analyzer.df.empty:
            print("DataAnalyzer dataframe is empty after processing.")
            return JSONResponse({"error": "No valid transactions found."}, status_code=400)

        # Get analytics
        stats = analyzer.get_fun_stats()
        metadata = analyzer.get_account_info()
        top_recipients = analyzer.get_top_recipients()
        weekday_breakdown = stats.get('weekday_breakdown', [])

        transaction_categories = analyzer.get_transaction_categories()
        monthly_breakdown = analyzer.get_monthly_breakdown()
        recent_transactions = analyzer.get_recent_transactions()

        # Optional: Generate AI advice (lazy init)
        advisor = AIFinancialAdvisor()
        advice = advisor.generate_advice(stats)

        # Prepare frontend-ready JSON
        response_data = {
            "stats": {
                "totalBalance": stats.get("net_raw", 0),
                "totalBalanceFormatted": stats.get("net", "₦0.00"),
                "totalIncome": stats.get("total_in_raw", 0),
                "totalIncomeFormatted": stats.get("total_in", "₦0.00"),
                "totalExpense": stats.get("total_out_raw", 0),
                "totalExpenseFormatted": stats.get("total_out", "₦0.00"),
                "transactionCount": stats.get("transaction_count", 0),
                "debitCount": stats.get("debit_count", 0),
                "creditCount": stats.get("credit_count", 0),
                "avgSpend": stats.get("avg_spend", "₦0.00"),
                "avgIncome": stats.get("avg_income", "₦0.00"),
                "weekendPct": stats.get("weekend_pct", "0%"),
                "busiestDay": stats.get("busiest_day", "N/A"),
                "busiestDayAmount": stats.get("busiest_day_amount", "₦0.00"),
                "topMonth": stats.get("top_spending_month", "N/A"),
                "topMonthAmount": stats.get("top_month_amount", "₦0.00"),
                "dataAirtimeSpend": stats.get("data_airtime_spend", "₦0.00"),
                "dataPct": stats.get("data_pct", "0%"),                             
                "closingBalance": metadata.get("closing_balance", "₦0.00"),
                "openingBalance": metadata.get("opening_balance", "₦0.00"),
            },
            "categories": transaction_categories,
            "topRecipients": top_recipients,
            "monthlyBreakdown": monthly_breakdown,
            "weekdayBreakdown": weekday_breakdown, 
            "recentTransactions": recent_transactions,
            "advice": advice
        }

        return JSONResponse(response_data)

    except Exception as e:
        print("Exception occurred in /upload endpoint:")
        traceback.print_exc()
        return JSONResponse({
            "error": str(e),
            "traceback": traceback.format_exc()
        }, status_code=500)
