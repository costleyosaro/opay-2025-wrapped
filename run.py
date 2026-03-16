"""
Start the OPay Wrapped server.
Run from the backend/ folder:   python run.py
"""
import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",        # ← CHANGED from "backend.app.main:app"
        host="0.0.0.0",
        port=8000,
        reload=True,
    )