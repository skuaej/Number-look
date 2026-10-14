import os
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import FastAPI, Query, Request, Response, BackgroundTasks
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

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8860561051:AAGaPGzEwW5vYDjgfY5bKvs9D8ISBUYbEh8")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://number-look-production.up.railway.app")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

ENCODED_USER = urllib.parse.quote_plus(USERNAME)
ENCODED_PASS = urllib.parse.quote_plus(PASSWORD)
PROXY_URL = f"http://{ENCODED_USER}:{ENCODED_PASS}@ddc.oxylabs.io:8001"

# --- Auto-Configure Webhook on Startup ---
@app.on_event("startup")
def startup_event():
    try:
        target_webhook = f"{WEBHOOK_URL}/webhook"
        response = requests.get(f"{TELEGRAM_API_URL}/setWebhook?url={target_webhook}")
        print(f"Auto-Webhook Registration Response: {response.json()}")
    except Exception as e:
        print(f"Startup Webhook Error: {str(e)}")

def query_shard(uri: str, record_type: str, target_number: str) -> pd.DataFrame:
    """Worker function to query a single Parquet file with a safety row limit."""
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
        LIMIT 50
    """
    try:
        return con.execute(query).df()
    except Exception:
        return pd.DataFrame()

# --- Core Search Engine ---
def perform_search(number: str) -> dict:
    if not number or not number.isdigit() or len(number) < 10 or len(number) > 15:
        return {"status": "rejected", "message": "Invalid number format."}
    
    # Block placeholder/repeated numbers (e.g., 9999999999, 0000000000) to prevent overflow
    if len(set(number)) == 1:
        return {"status": "rejected", "message": "Placeholder numbers are not allowed."}
    
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
        body { margin: 0; overflow: hidden; background-color: #050505; color: #00ffcc; font-family: 'Courier New', Courier, monospace; }
        #canvas-container { position: absolute; top: 0; left: 0; width: 100%; height: 100%; z-index: -1; }
        .overlay { 
            position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); 
            text-align: center; background: rgba(10, 10, 10, 0.85); padding: 50px; 
            border: 1px solid #00ffcc; border-radius: 12px; box-shadow: 0 0 30px rgba(0, 255, 204, 0.3); 
            backdrop-filter: blur(5px);
        }
        h1 { margin: 0 0 15px 0; font-size: 3.5em; text-transform: uppercase; letter-spacing: 6px; text-shadow: 0 0 15px #00ffcc; }
        p { font-size: 1.2em; margin: 8px 0; color: #ccc; }
        .highlight { color: #00ffcc; font-weight: bold; }
        .status-box { 
            margin-top: 30px; font-weight: bold; padding: 15px; 
            border-radius: 8px; background: rgba(0, 255, 204, 0.1); 
            border: 1px solid rgba(0, 255, 204, 0.5);
            font-size: 1.1em;
        }
        .blinking { animation: blinker 1.5s linear infinite; display: inline-block; }
        @keyframes blinker { 50% { opacity: 0; } }
    </style>
</head>
<body>
    <div id="canvas-container"></div>
    <div class="overlay">
        <h1>SYSTEM ONLINE</h1>
        <p>API Gateway is <span class="highlight">Active & Secured</span></p>
        <p>Parquet Cloud Engine: <span class="highlight">High-Speed Parallel Stream</span></p>
        <div class="status-box">
            <span class="blinking" style="color: #00ffcc;">●</span> HTTP 200 OK - LISTENING FOR QUERIES
        </div>
    </div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
    <script>
        const scene = new THREE.Scene();
        const camera = new THREE.PerspectiveCamera(75, window.innerWidth / window.innerHeight, 0.1, 2000);
        const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
        
        renderer.setSize(window.innerWidth, window.innerHeight);
        document.getElementById('canvas-container').appendChild(renderer.domElement);

        const geometry = new THREE.BufferGeometry();
        const vertices = [];
        for (let i = 0; i < 8000; i++) {
            vertices.push(THREE.MathUtils.randFloatSpread(3000));
            vertices.push(THREE.MathUtils.randFloatSpread(3000));
            vertices.push(THREE.MathUtils.randFloatSpread(3000));
        }
        
        geometry.setAttribute('position', new THREE.Float32BufferAttribute(vertices, 3));
        const material = new THREE.PointsMaterial({ color: 0x00ffcc, size: 2.5, transparent: true, opacity: 0.8 });
        const points = new THREE.Points(geometry, material);
        scene.add(points);

        camera.position.z = 1200;

        function animate() {
            requestAnimationFrame(animate);
            points.rotation.x += 0.0005;
            points.rotation.y += 0.001;
            renderer.render(scene, camera);
        }
        animate();

        window.addEventListener('resize', () => {
            camera.aspect = window.innerWidth / window.innerHeight;
            camera.updateProjectionMatrix();
            renderer.setSize(window.innerWidth, window.innerHeight);
        });
    </script>
</body>
</html>
"""

@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        return JSONResponse(
            status_code=404,
            content={
                "status": "rejected",
                "message": "Invalid endpoint. STRICTLY use /FetchData?Number=XXXXXXXXXX",
                "Developer": "@jio_osint_bot"
            }
        )
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail, "Developer": "@jio_osint_bot"}
    )

@app.get("/", response_class=HTMLResponse)
def root_landing_page():
    return HTMLResponse(content=LANDING_PAGE_HTML, status_code=200)

@app.get("/FetchData")
def fetch_data(Number: str = Query(None)):
    if not Number or not Number.isdigit() or len(Number) < 10 or len(Number) > 15:
        return JSONResponse(
            status_code=400,
            content={
                "status": "rejected",
                "message": "STRICTLY use /FetchData?Number=XXXXXXXXXX",
                "Developer": "@jio_osint_bot"
            }
        )
    
    result = perform_search(Number)
    if result.get("status") == "not_found":
        return JSONResponse(
            status_code=404,
            content={
                "status": "not_found", 
                "phone": Number,
                "Developer": "@jio_osint_bot"
            }
        )
    elif result.get("status") == "error":
        return JSONResponse(status_code=500, content=result)
        
    return {"status": "success", "Data": result.get("Data"), "Developer": "@jio_osint_bot"}

# --- Background Processing for Telegram Webhook ---
async def process_telegram_update(data: dict):
    chat_id = None
    try:
        if "message" in data and "text" in data["message"]:
            chat_id = data["message"]["chat"]["id"]
            text = data["message"]["text"].strip()
            
            if text.lower().startswith("/start") or text.lower().startswith("/help"):
                reply_text = (
                    "👋 **Welcome to Hitek Data Gateway!**\n\n"
                    "Send any 10-digit mobile number here to instantly search the database via high-speed zero-download stream.\n\n"
                    "Developer: @jio_osint_bot"
                )
            elif text.isdigit():
                search_res = perform_search(text)
                
                if search_res.get("status") == "success":
                    records_data = search_res.get("Data", {})
                    reply_text = (
                        f"✅ **MATCH FOUND FOR: {text}**\n\n"
                        f"Main Records: {len(records_data.get('Main_Records', []))}\n"
                        f"Alt Records: {len(records_data.get('Alt_Records', []))}\n\n"
                        f"Data:\n{str(records_data)[:3500]}\n\n"
                        f"Developer: @jio_osint_bot"
                    )
                elif search_res.get("status") == "not_found":
                    reply_text = f"❌ Number `{text}` not found in the database.\n\nDeveloper: @jio_osint_bot"
                else:
                    reply_text = f"⚠️ Search Error: {search_res.get('message', 'Unknown database error')}\n\nDeveloper: @jio_osint_bot"
            else:
                reply_text = "⚠️ Please send a valid 10-digit phone number (digits only).\n\nDeveloper: @jio_osint_bot"
                
            requests.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                "chat_id": chat_id,
                "text": reply_text,
                "parse_mode": "Markdown"
            })
    except Exception as e:
        print(f"Background Processing Error: {str(e)}")
        if chat_id:
            requests.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                "chat_id": chat_id,
                "text": f"⚠️ Server Exception: {str(e)}"
            })

@app.post("/webhook")
async def telegram_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        data = await request.json()
        background_tasks.add_task(process_telegram_update, data)
    except Exception as e:
        print(f"Webhook Error: {str(e)}")
        
    return Response(status_code=200)
    
