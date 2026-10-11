import os
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import FastAPI, Query, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
import duckdb
import pandas as pd
from huggingface_hub import HfFileSystem
import requests

app = FastAPI()

# --- Configuration & Credentials via Environment Variables ---
USERNAME = os.getenv("OXYLABS_USERNAME", "user-ddc_16512248")
PASSWORD = os.getenv("OXYLABS_PASSWORD", "8Boeb~60zRW3")
BUCKET_ID = os.getenv("BUCKET_ID", "CutehackX/hitek-data-bucket")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8637568132:AAGRnWPNXSuvvq-JCUjBMW5Z1_1mtO_jVFU")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

ENCODED_USER = urllib.parse.quote_plus(USERNAME)
ENCODED_PASS = urllib.parse.quote_plus(PASSWORD)
PROXY_URL = f"http://{ENCODED_USER}:{ENCODED_PASS}@ddc.oxylabs.io:8001"

def query_shard(uri: str, record_type: str, target_number: str) -> pd.DataFrame:
    """Worker function to query a single Parquet file in a thread-isolated connection."""
    os.environ["http_proxy"] = PROXY_URL
    os.environ["https_proxy"] = PROXY_URL
    
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.register_filesystem(HfFileSystem())
    
    col_name = "mobile" if record_type == "Main" else "alt"
    query = f"""
        SELECT *, '{record_type}' AS _record_type 
        FROM read_parquet('{uri}') 
        WHERE {col_name} = '{target_number}'
    """
    try:
        return con.execute(query).df()
    except Exception:
        return pd.DataFrame()

# --- Core Search Engine ---
def perform_search(number: str) -> dict:
    if not number or not number.isdigit() or len(number) < 10 or len(number) > 15:
        return {"status": "rejected", "message": "Invalid number format."}
    
    last_digit = number[-1]
    primary_uri = f"hf://buckets/{BUCKET_ID}/final_master_shard_{last_digit}.parquet"
    alt_uri = f"hf://buckets/{BUCKET_ID}/alt_master_shard_{last_digit}.parquet"
    
    tasks = [(primary_uri, "Main"), (alt_uri, "Alt")]
    all_results = []
    
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(query_shard, uri, r_type, number): r_type for uri, r_type in tasks}
            for future in as_completed(futures):
                df = future.result()
                if not df.empty:
                    all_results.append(df)
        
        if not all_results:
            return {"status": "not_found", "phone": number}
        
        combined_df = pd.concat(all_results, ignore_index=True)
        raw_results = combined_df.to_dict(orient="records")
        
        main_records, alt_records = [], []
        for row in raw_results:
            rec_type = row.pop('_record_type')
            if rec_type == 'Main':
                main_records.append(row)
            else:
                alt_records.append(row)
                
        return {
            "status": "success",
            "Data": {"Main_Records": main_records, "Alt_Records": alt_records}
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# --- FastAPI Web Endpoints ---
LANDING_PAGE_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Hitek Data Gateway - LIVE</title>
    <style>
        body { margin: 0; background-color: #050505; color: #00ffcc; font-family: monospace; display: flex; justify-content: center; align-items: center; height: 100vh; }
        .overlay { text-align: center; background: rgba(10,10,10,0.85); padding: 40px; border: 1px solid #00ffcc; border-radius: 12px; box-shadow: 0 0 20px rgba(0,255,204,0.3); }
        h1 { font-size: 3em; text-transform: uppercase; letter-spacing: 4px; }
    </style>
</head>
<body>
    <div class="overlay">
        <h1>SYSTEM ONLINE</h1>
        <p>FastAPI Gateway & Telegram Webhook Active</p>
    </div>
</body>
</html>
"""

@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    return JSONResponse(status_code=exc.status_code, content={"status": "rejected", "Developer": "@Maybechx"})

@app.get("/", response_class=HTMLResponse)
def root_landing_page():
    return HTMLResponse(content=LANDING_PAGE_HTML, status_code=200)

@app.get("/FetchData")
def fetch_data(Number: str = Query(None)):
    if not Number or not Number.isdigit() or len(Number) < 10 or len(Number) > 15:
        return JSONResponse(status_code=400, content={"status": "rejected", "message": "STRICTLY use /FetchData?Number=XXXXXXXXXX", "Developer": "@Maybechx"})
    
    result = perform_search(Number)
    if result.get("status") == "not_found":
        return JSONResponse(status_code=404, content={"status": "not_found", "phone": Number, "Developer": "@Maybechx"})
    elif result.get("status") == "error":
        return JSONResponse(status_code=500, content=result)
        
    return {"status": "success", "Data": result.get("Data"), "Developer": "@Maybechx"}

# --- Telegram Webhook Endpoint ---
@app.post("/webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    
    # Extract message from Telegram payload
    if "message" in data and "text" in data["message"]:
        chat_id = data["message"]["chat"]["id"]
        text = data["message"]["text"].strip()
        
        if text.isdigit():
            # Call the search engine logic directly
            search_res = perform_search(text)
            
            if search_res.get("status") == "success":
                records_data = search_res.get("Data", {})
                reply_text = f"MATCH FOUND FOR: {text}\n\nMain Records: {len(records_data.get('Main_Records', []))}\nAlt Records: {len(records_data.get('Alt_Records', []))}\n\nData: {str(records_data)[:3500]}"
            elif search_res.get("status") == "not_found":
                reply_text = f"Number {text} not found in database."
            else:
                reply_text = f"Error processing query."
        else:
            reply_text = "Please send a valid phone number digits only."
            
        # Send reply back to Telegram user via Telegram HTTP API
        requests.post(f"{TELEGRAM_API_URL}/sendMessage", json={
            "chat_id": chat_id,
            "text": reply_text
        })
        
    return Response(status_code=200)
  
