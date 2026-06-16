"""
Shopee Indonesia Scraper — API + Queue System
==============================================
GET /shopee?keyword=mouse+gaming&pages=3
GET /status/{job_id}   → cek status job
GET /health

Queue: asyncio.Queue — FIFO, satu worker, request antri tidak ditolak.

Run:
    uvicorn shopee_scraper:app --host 0.0.0.0 --port 8000
"""

import asyncio
import json
import re
import random
import urllib.parse
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from playwright.async_api import async_playwright, BrowserContext

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
SHOPEE_BASE    = "https://shopee.co.id"
SEARCH_API     = "https://shopee.co.id/api/v4/search/search_items"
ITEMS_PER_PAGE = 20
REQUEST_DELAY  = (2.0, 3.5)
MAX_RETRIES    = 3
MAX_QUEUE_SIZE = 20   # tolak request kalau antrian > 20

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.112 Safari/537.36",
]

VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1366, "height": 768},
    {"width": 1536, "height": 864},
]

CHROMIUM_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-extensions",
    "--disable-background-networking",
    "--disable-sync",
    "--disable-translate",
    "--disable-default-apps",
    "--mute-audio",
    "--no-first-run",
    "--no-zygote",
    "--single-process",
    "--disable-accelerated-2d-canvas",
    "--disable-webgl",
    "--disable-features=TranslateUI,BlinkGenPropertyTrees",
    "--js-flags=--max-old-space-size=256",
]

STEALTH_SCRIPT = """
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
    Object.defineProperty(navigator, 'languages', {get: () => ['id-ID','id','en-US','en']});
    Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
    Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 4});
    Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
    window.chrome = {runtime:{}, loadTimes:function(){}, csi:function(){}, app:{}};
    const _q = window.navigator.permissions.query;
    window.navigator.permissions.query = (p) =>
        p.name === 'notifications'
            ? Promise.resolve({state: Notification.permission})
            : _q(p);
"""

# ─────────────────────────────────────────────
# JOB STATE
# ─────────────────────────────────────────────
# Status: pending → running → done | error

jobs: dict[str, dict] = {}
# {
#   job_id: {
#     "status"    : "pending" | "running" | "done" | "error",
#     "keyword"   : str,
#     "pages"     : int,
#     "queue_pos" : int,          # posisi antrian (0 = sedang jalan)
#     "created_at": str,
#     "started_at": str | None,
#     "done_at"   : str | None,
#     "result"    : dict | None,
#     "error"     : str | None,
#     "event"     : asyncio.Event  # di-await oleh HTTP request
#   }
# }

scrape_queue: asyncio.Queue = asyncio.Queue()

# ─────────────────────────────────────────────
# BROWSER POOL
# ─────────────────────────────────────────────
class BrowserPool:
    def __init__(self):
        self._playwright = None
        self._browser    = None

    async def start(self):
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=CHROMIUM_ARGS,
        )
        print("[BrowserPool] Chromium launched ✓")

    async def stop(self):
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        print("[BrowserPool] Chromium stopped")

    async def new_context(self) -> BrowserContext:
        ctx = await self._browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport=random.choice(VIEWPORTS),
            locale="id-ID",
            timezone_id="Asia/Jakarta",
            ignore_https_errors=True,
        )
        await ctx.route(
            re.compile(r"\.(png|jpg|jpeg|gif|webp|svg|ico|woff2?|ttf|mp4|mp3)(\?.*)?$"),
            lambda route: route.abort()
        )
        await ctx.route(
            re.compile(r"(google-analytics|googletagmanager|facebook\.net|hotjar|doubleclick)"),
            lambda route: route.abort()
        )
        return ctx


pool = BrowserPool()

# ─────────────────────────────────────────────
# QUEUE WORKER — loop tak berenti, proses 1 job per kali
# ─────────────────────────────────────────────
async def queue_worker():
    """
    Single worker yang jalan selamanya di background.
    Ambil job dari queue → scrape → simpan hasil → signal event.
    Karena hanya 1 worker, request otomatis antri FIFO.
    """
    print("[Worker] Queue worker started")
    while True:
        job_id = await scrape_queue.get()

        job = jobs.get(job_id)
        if not job:
            scrape_queue.task_done()
            continue

        # Update posisi antrian semua job pending
        _update_queue_positions()

        job["status"]     = "running"
        job["queue_pos"]  = 0
        job["started_at"] = _now()
        print(f"[Worker] Running job {job_id} — keyword={job['keyword']}")

        try:
            result = await _scrape(
                keyword   = job["keyword"],
                max_pages = job["pages"],
            )
            job["status"]  = "done"
            job["result"]  = result
            job["done_at"] = _now()
            print(f"[Worker] Done job {job_id} — {result['total_scraped']} produk")

        except Exception as e:
            job["status"]  = "error"
            job["error"]   = str(e)
            job["done_at"] = _now()
            print(f"[Worker] Error job {job_id}: {e}")

        finally:
            job["event"].set()   # sinyal ke HTTP request yang lagi nunggu
            scrape_queue.task_done()
            _update_queue_positions()


def _update_queue_positions():
    """Hitung ulang posisi antrian tiap job pending."""
    pos = 1
    for jid, j in jobs.items():
        if j["status"] == "pending":
            j["queue_pos"] = pos
            pos += 1


def _now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

# ─────────────────────────────────────────────
# SCRAPE LOGIC
# ─────────────────────────────────────────────
def _build_api_url(keyword: str, newest: int) -> str:
    extra = urllib.parse.quote(json.dumps({
        "global_search_session_id": f"gs-{uuid.uuid4()}",
        "search_session_id"       : f"ss-{uuid.uuid4()}",
    }))
    return (
        f"{SEARCH_API}"
        f"?by=relevancy"
        f"&extra_params={extra}"
        f"&keyword={urllib.parse.quote(keyword)}"
        f"&limit={ITEMS_PER_PAGE}"
        f"&newest={newest}"
        f"&order=desc"
        f"&page_type=search"
        f"&scenario=PAGE_GLOBAL_SEARCH"
        f"&source=SRP"
        f"&version=2"
        f"&view_session_id={uuid.uuid4()}"
    )


def _parse_product(ib: dict) -> dict | None:
    try:
        item_id = ib.get("itemid", 0)
        shop_id = ib.get("shopid", 0)
        if not item_id:
            return None

        def rp(val):
            return int(val / 100000) if val and val > 0 else 0

        r_obj  = ib.get("item_rating", {}) or {}
        r_cnt  = r_obj.get("rating_count", [])
        s_obj  = ib.get("item_card_display_sold_count", {}) or {}
        tier   = ib.get("tier_variations", []) or []
        variasi = [
            {"nama": t["name"], "opsi": t["options"]}
            for t in tier
            if t.get("name") and t.get("options") and t["options"] != [""]
        ]
        img = ib.get("image", "")

        return {
            "item_id"        : item_id,
            "shop_id"        : shop_id,
            "nama"           : ib.get("name", "").strip(),
            "harga"          : rp(ib.get("price")),
            "harga_min"      : rp(ib.get("price_min")),
            "harga_max"      : rp(ib.get("price_max")),
            "harga_asli"     : rp(ib.get("price_before_discount")),
            "diskon"         : ib.get("discount") or "",
            "stok"           : ib.get("stock", 0),
            "terjual_bulan"  : s_obj.get("rounded_local_monthly_sold_count", 0),
            "terjual_total"  : s_obj.get("rounded_display_sold_count", 0),
            "rating"         : round(r_obj.get("rating_star", 0), 2),
            "total_ulasan"   : sum(r_cnt) if isinstance(r_cnt, list) else 0,
            "total_komentar" : ib.get("cmt_count", 0),
            "disukai"        : ib.get("liked_count", 0),
            "brand"          : ib.get("brand", ""),
            "lokasi_toko"    : ib.get("shop_location", ""),
            "nama_toko"      : ib.get("shop_name", ""),
            "official_mall"  : ib.get("is_official_shop", False),
            "shopee_verified": ib.get("shopee_verified", False),
            "cod"            : ib.get("can_use_cod", False),
            "variasi"        : variasi,
            "gambar"         : f"https://cf.shopee.co.id/file/{img}" if img else "",
            "url"            : f"{SHOPEE_BASE}/product/{shop_id}/{item_id}",
        }
    except Exception:
        return None


async def _scrape(keyword: str, max_pages: int) -> dict:
    ctx  = await pool.new_context()
    page = await ctx.new_page()
    await page.add_init_script(STEALTH_SCRIPT)

    products    = []
    total_count = 0
    token_event = asyncio.Event()

    async def on_request(req):
        if "shopee.co.id/api/v4/search/" in req.url and not token_event.is_set():
            if req.headers.get("sz-token"):
                token_event.set()

    page.on("request", on_request)

    # Warm-up
    try:
        await page.goto(
            f"{SHOPEE_BASE}/search?keyword={urllib.parse.quote(keyword)}",
            wait_until="domcontentloaded",
            timeout=30000,
        )
        await asyncio.wait_for(token_event.wait(), timeout=12.0)
    except Exception as e:
        print(f"[scrape] warmup warn: {e}")

    # Loop halaman
    stop = False
    for page_num in range(max_pages):
        if stop:
            break

        newest  = page_num * ITEMS_PER_PAGE
        api_url = _build_api_url(keyword, newest)

        for attempt in range(MAX_RETRIES):
            try:
                data = await page.evaluate("""
                    async (url) => {
                        try {
                            const r = await fetch(url, {
                                credentials: 'include',
                                headers: {
                                    'Accept': 'application/json',
                                    'X-Shopee-Language': 'id',
                                    'X-Requested-With': 'XMLHttpRequest',
                                    'X-API-SOURCE': 'rweb',
                                }
                            });
                            if (!r.ok) return {_err: r.status};
                            return await r.json();
                        } catch(e) {
                            return {_err: e.toString()};
                        }
                    }
                """, api_url)

                if not data or "_err" in data:
                    raise ValueError(str(data.get("_err", "empty response")))
                if data.get("error"):
                    raise ValueError(f"API: {data.get('error_msg')}")

                items_raw   = data.get("items", [])
                total_count = data.get("total_count", 0)

                for entry in items_raw:
                    ib = entry.get("item_basic") or entry
                    p  = _parse_product(ib)
                    if p:
                        products.append(p)

                print(f"[scrape] page {page_num+1} → {len(items_raw)} items")

                if data.get("nomore"):
                    stop = True

                break  # sukses, keluar retry

            except Exception as e:
                print(f"[scrape] page {page_num+1} attempt {attempt+1}: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(2)

        if not stop and page_num < max_pages - 1:
            await asyncio.sleep(random.uniform(*REQUEST_DELAY))

    await page.close()
    await ctx.close()

    # Deduplikasi
    seen, unique = set(), []
    for p in products:
        if p["item_id"] not in seen:
            seen.add(p["item_id"])
            unique.append(p)

    return {
        "keyword"      : keyword,
        "total_shopee" : total_count,
        "total_scraped": len(unique),
        "pages"        : max_pages,
        "produk"       : unique,
    }

# ─────────────────────────────────────────────
# LIFESPAN
# ─────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await pool.start()
    worker_task = asyncio.create_task(queue_worker())
    yield
    worker_task.cancel()
    await pool.stop()


app = FastAPI(
    title="Shopee Scraper API",
    version="3.0.0",
    lifespan=lifespan,
)

# ─────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "service": "Shopee Scraper API",
        "version": "3.0.0",
        "endpoints": {
            "scrape" : "GET /shopee?keyword=mouse&pages=3",
            "status" : "GET /status/{job_id}",
            "queue"  : "GET /queue",
            "health" : "GET /health",
        }
    }


@app.get("/shopee")
async def shopee_search(
    keyword: str = Query(..., description="Kata kunci pencarian"),
    pages  : int = Query(default=3, ge=1, le=10, description="Jumlah halaman (max 10)"),
    wait   : bool = Query(default=True, description="True=tunggu hasil, False=langsung dapat job_id"),
):
    keyword = keyword.strip()
    if not keyword:
        raise HTTPException(status_code=400, detail="keyword tidak boleh kosong")

    # Tolak kalau antrian sudah penuh
    if scrape_queue.qsize() >= MAX_QUEUE_SIZE:
        raise HTTPException(
            status_code=503,
            detail=f"Antrian penuh ({MAX_QUEUE_SIZE} job). Coba lagi nanti."
        )

    # Buat job baru
    job_id = str(uuid.uuid4())
    event  = asyncio.Event()
    pos    = scrape_queue.qsize() + 1

    jobs[job_id] = {
        "status"    : "pending",
        "keyword"   : keyword,
        "pages"     : pages,
        "queue_pos" : pos,
        "created_at": _now(),
        "started_at": None,
        "done_at"   : None,
        "result"    : None,
        "error"     : None,
        "event"     : event,
    }

    await scrape_queue.put(job_id)
    print(f"[Queue] Job {job_id} masuk antrian pos={pos}")

    # Mode async: langsung return job_id, client poll /status
    if not wait:
        return JSONResponse({
            "job_id"   : job_id,
            "status"   : "pending",
            "queue_pos": pos,
            "poll_url" : f"/status/{job_id}",
            "message"  : f"Job antri di posisi {pos}. Poll /status/{job_id} untuk hasil.",
        })

    # Mode sync: tunggu sampai selesai (default)
    await event.wait()

    job = jobs[job_id]
    if job["status"] == "error":
        raise HTTPException(status_code=500, detail=job["error"])

    return JSONResponse({
        "job_id"    : job_id,
        "started_at": job["started_at"],
        "done_at"   : job["done_at"],
        **job["result"],
    })


@app.get("/status/{job_id}")
async def job_status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job tidak ditemukan")

    base = {
        "job_id"    : job_id,
        "status"    : job["status"],
        "keyword"   : job["keyword"],
        "pages"     : job["pages"],
        "created_at": job["created_at"],
        "started_at": job["started_at"],
        "done_at"   : job["done_at"],
    }

    if job["status"] == "pending":
        return JSONResponse({**base, "queue_pos": job["queue_pos"]})

    if job["status"] == "running":
        return JSONResponse({**base, "queue_pos": 0})

    if job["status"] == "error":
        return JSONResponse({**base, "error": job["error"]}, status_code=500)

    # done
    return JSONResponse({**base, **job["result"]})


@app.get("/queue")
async def queue_info():
    """Lihat semua job dan status antrian."""
    summary = []
    for jid, j in jobs.items():
        summary.append({
            "job_id"    : jid,
            "status"    : j["status"],
            "keyword"   : j["keyword"],
            "pages"     : j["pages"],
            "queue_pos" : j["queue_pos"] if j["status"] == "pending" else None,
            "created_at": j["created_at"],
            "started_at": j["started_at"],
            "done_at"   : j["done_at"],
        })

    pending = sum(1 for j in jobs.values() if j["status"] == "pending")
    running = sum(1 for j in jobs.values() if j["status"] == "running")
    done    = sum(1 for j in jobs.values() if j["status"] == "done")
    error   = sum(1 for j in jobs.values() if j["status"] == "error")

    return JSONResponse({
        "queue_size": scrape_queue.qsize(),
        "summary"   : {"pending": pending, "running": running, "done": done, "error": error},
        "jobs"      : summary,
    })


@app.get("/health")
async def health():
    return {
        "status"    : "ok",
        "queue_size": scrape_queue.qsize(),
        "total_jobs": len(jobs),
}
