"""
Nifty 500 Live Signal Backend v7
Render.com | uvicorn main:app --host 0.0.0.0 --port $PORT

New in v7:
  - Real-time SMA/EMA Confluence (±1% of Average MA) signal engine
  - 5-Year High Breakout detection module
  - Sector-wise filter endpoints for Buy/Sell/Confluence
  - WebSocket endpoint for push-based signal delivery
  - Duplicate-trigger suppression (enter/exit zone tracking)
"""
import os, time, logging, sqlite3, json, asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from collections import deque
import pytz, pandas as pd, requests, yfinance as yf, numpy as np
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")
MARKET_OPEN = (9, 15); MARKET_CLOSE = (15, 30)
DB_PATH = "signals.db"; PRICE_HISTORY_LEN = 30


# ─────────────────────────────────────────────
# STOCK UNIVERSE
# ─────────────────────────────────────────────
STOCKS = [
    {"symbol":"HDFCBANK","name":"HDFC Bank","sector":"Banking"},
    {"symbol":"ICICIBANK","name":"ICICI Bank","sector":"Banking"},
    {"symbol":"SBIN","name":"State Bank of India","sector":"Banking"},
    {"symbol":"KOTAKBANK","name":"Kotak Mahindra Bank","sector":"Banking"},
    {"symbol":"AXISBANK","name":"Axis Bank","sector":"Banking"},
    {"symbol":"INDUSINDBK","name":"IndusInd Bank","sector":"Banking"},
    {"symbol":"BANKBARODA","name":"Bank of Baroda","sector":"Banking"},
    {"symbol":"PNB","name":"Punjab National Bank","sector":"Banking"},
    {"symbol":"CANBK","name":"Canara Bank","sector":"Banking"},
    {"symbol":"UNIONBANK","name":"Union Bank of India","sector":"Banking"},
    {"symbol":"FEDERALBNK","name":"Federal Bank","sector":"Banking"},
    {"symbol":"IDFCFIRSTB","name":"IDFC First Bank","sector":"Banking"},
    {"symbol":"BANDHANBNK","name":"Bandhan Bank","sector":"Banking"},
    {"symbol":"AUBANK","name":"AU Small Finance Bank","sector":"Banking"},
    {"symbol":"YESBANK","name":"Yes Bank","sector":"Banking"},
    {"symbol":"INDIANB","name":"Indian Bank","sector":"Banking"},
    {"symbol":"BANKINDIA","name":"Bank of India","sector":"Banking"},
    {"symbol":"MAHABANK","name":"Bank of Maharashtra","sector":"Banking"},
    {"symbol":"RBLBANK","name":"RBL Bank","sector":"Banking"},
    {"symbol":"KARURVYSYA","name":"Karur Vysya Bank","sector":"Banking"},
    {"symbol":"UJJIVANSFB","name":"Ujjivan Small Finance Bank","sector":"Banking"},
    {"symbol":"EQUITASBNK","name":"Equitas Small Finance Bank","sector":"Banking"},
    {"symbol":"TCS","name":"Tata Consultancy Services","sector":"IT"},
    {"symbol":"INFY","name":"Infosys","sector":"IT"},
    {"symbol":"HCLTECH","name":"HCL Technologies","sector":"IT"},
    {"symbol":"WIPRO","name":"Wipro","sector":"IT"},
    {"symbol":"TECHM","name":"Tech Mahindra","sector":"IT"},
    {"symbol":"LTIM","name":"LTIMindtree","sector":"IT"},
    {"symbol":"COFORGE","name":"Coforge","sector":"IT"},
    {"symbol":"MPHASIS","name":"Mphasis","sector":"IT"},
    {"symbol":"PERSISTENT","name":"Persistent Systems","sector":"IT"},
    {"symbol":"LTTS","name":"L&T Technology Services","sector":"IT"},
    {"symbol":"KPITTECH","name":"KPIT Technologies","sector":"IT"},
    {"symbol":"TATAELXSI","name":"Tata Elxsi","sector":"IT"},
    {"symbol":"MASTEK","name":"Mastek","sector":"IT"},
    {"symbol":"ZENSAR","name":"Zensar Technologies","sector":"IT"},
    {"symbol":"SONATSOFTW","name":"Sonata Software","sector":"IT"},
    {"symbol":"CYIENT","name":"Cyient","sector":"IT"},
    {"symbol":"TANLA","name":"Tanla Platforms","sector":"IT"},
    {"symbol":"CAMS","name":"CAMS","sector":"IT"},
    {"symbol":"KFINTECH","name":"KFin Technologies","sector":"IT"},
    {"symbol":"RELIANCE","name":"Reliance Industries","sector":"Energy"},
    {"symbol":"ONGC","name":"ONGC","sector":"Energy"},
    {"symbol":"BPCL","name":"Bharat Petroleum","sector":"Energy"},
    {"symbol":"IOC","name":"Indian Oil Corp","sector":"Energy"},
    {"symbol":"HINDPETRO","name":"Hindustan Petroleum","sector":"Energy"},
    {"symbol":"GAIL","name":"GAIL India","sector":"Energy"},
    {"symbol":"PETRONET","name":"Petronet LNG","sector":"Energy"},
    {"symbol":"MRPL","name":"MRPL","sector":"Energy"},
    {"symbol":"IGL","name":"Indraprastha Gas","sector":"Energy"},
    {"symbol":"MGL","name":"Mahanagar Gas","sector":"Energy"},
    {"symbol":"GUJGASLTD","name":"Gujarat Gas","sector":"Energy"},
    {"symbol":"NTPC","name":"NTPC","sector":"Power"},
    {"symbol":"POWERGRID","name":"Power Grid Corp","sector":"Power"},
    {"symbol":"TATAPOWER","name":"Tata Power","sector":"Power"},
    {"symbol":"ADANIGREEN","name":"Adani Green Energy","sector":"Power"},
    {"symbol":"TORNTPOWER","name":"Torrent Power","sector":"Power"},
    {"symbol":"JSWENERGY","name":"JSW Energy","sector":"Power"},
    {"symbol":"NHPC","name":"NHPC","sector":"Power"},
    {"symbol":"SJVN","name":"SJVN","sector":"Power"},
    {"symbol":"CESC","name":"CESC","sector":"Power"},
    {"symbol":"ADANIPOWER","name":"Adani Power","sector":"Power"},
    {"symbol":"KEC","name":"KEC International","sector":"Power"},
    {"symbol":"KALPATPOWR","name":"Kalpataru Power","sector":"Power"},
    {"symbol":"HINDUNILVR","name":"Hindustan Unilever","sector":"FMCG"},
    {"symbol":"ITC","name":"ITC","sector":"FMCG"},
    {"symbol":"NESTLEIND","name":"Nestle India","sector":"FMCG"},
    {"symbol":"BRITANNIA","name":"Britannia Industries","sector":"FMCG"},
    {"symbol":"DABUR","name":"Dabur India","sector":"FMCG"},
    {"symbol":"MARICO","name":"Marico","sector":"FMCG"},
    {"symbol":"GODREJCP","name":"Godrej Consumer Products","sector":"FMCG"},
    {"symbol":"COLPAL","name":"Colgate-Palmolive India","sector":"FMCG"},
    {"symbol":"EMAMILTD","name":"Emami","sector":"FMCG"},
    {"symbol":"TATACONSUM","name":"Tata Consumer Products","sector":"FMCG"},
    {"symbol":"MARUTI","name":"Maruti Suzuki","sector":"Auto"},
    {"symbol":"TATAMOTORS","name":"Tata Motors","sector":"Auto"},
    {"symbol":"BAJAJ-AUTO","name":"Bajaj Auto","sector":"Auto"},
    {"symbol":"HEROMOTOCO","name":"Hero MotoCorp","sector":"Auto"},
    {"symbol":"M&M","name":"Mahindra & Mahindra","sector":"Auto"},
    {"symbol":"EICHERMOT","name":"Eicher Motors","sector":"Auto"},
    {"symbol":"TVSMOTORS","name":"TVS Motor Company","sector":"Auto"},
    {"symbol":"ASHOKLEY","name":"Ashok Leyland","sector":"Auto"},
    {"symbol":"ESCORTS","name":"Escorts Kubota","sector":"Auto"},
    {"symbol":"MOTHERSON","name":"Samvardhana Motherson","sector":"Auto Anc"},
    {"symbol":"BALKRISIND","name":"Balkrishna Industries","sector":"Auto Anc"},
    {"symbol":"MRF","name":"MRF","sector":"Auto Anc"},
    {"symbol":"APOLLOTYRE","name":"Apollo Tyres","sector":"Auto Anc"},
    {"symbol":"BHARATFORG","name":"Bharat Forge","sector":"Auto Anc"},
    {"symbol":"EXIDEIND","name":"Exide Industries","sector":"Auto Anc"},
    {"symbol":"UNOMINDA","name":"UNO Minda","sector":"Auto Anc"},
    {"symbol":"BOSCHLTD","name":"Bosch","sector":"Auto Anc"},
    {"symbol":"TIINDIA","name":"Tube Investments of India","sector":"Auto Anc"},
    {"symbol":"ENDURANCE","name":"Endurance Technologies","sector":"Auto Anc"},
    {"symbol":"BAJFINANCE","name":"Bajaj Finance","sector":"Finance"},
    {"symbol":"BAJAJFINSV","name":"Bajaj Finserv","sector":"Finance"},
    {"symbol":"CHOLAFIN","name":"Cholamandalam Finance","sector":"Finance"},
    {"symbol":"MUTHOOTFIN","name":"Muthoot Finance","sector":"Finance"},
    {"symbol":"SBICARD","name":"SBI Card","sector":"Finance"},
    {"symbol":"PFC","name":"Power Finance Corp","sector":"Finance"},
    {"symbol":"RECLTD","name":"REC Limited","sector":"Finance"},
    {"symbol":"IREDA","name":"IREDA","sector":"Finance"},
    {"symbol":"MANAPPURAM","name":"Manappuram Finance","sector":"Finance"},
    {"symbol":"M&MFIN","name":"M&M Financial Services","sector":"Finance"},
    {"symbol":"SHRIRAMFIN","name":"Shriram Finance","sector":"Finance"},
    {"symbol":"LTFH","name":"L&T Finance","sector":"Finance"},
    {"symbol":"ANGELONE","name":"Angel One","sector":"Finance"},
    {"symbol":"CDSL","name":"CDSL","sector":"Finance"},
    {"symbol":"BSE","name":"BSE Limited","sector":"Finance"},
    {"symbol":"MCX","name":"MCX India","sector":"Finance"},
    {"symbol":"IRFC","name":"IRFC","sector":"Finance"},
    {"symbol":"HUDCO","name":"HUDCO","sector":"Finance"},
    {"symbol":"LICHOUSING","name":"LIC Housing Finance","sector":"Finance"},
    {"symbol":"HDFCLIFE","name":"HDFC Life Insurance","sector":"Insurance"},
    {"symbol":"SBILIFE","name":"SBI Life Insurance","sector":"Insurance"},
    {"symbol":"ICICIGI","name":"ICICI Lombard GIC","sector":"Insurance"},
    {"symbol":"ICICIPRULI","name":"ICICI Prudential Life","sector":"Insurance"},
    {"symbol":"LICI","name":"LIC of India","sector":"Insurance"},
    {"symbol":"STARHEALTH","name":"Star Health Insurance","sector":"Insurance"},
    {"symbol":"SUNPHARMA","name":"Sun Pharmaceutical","sector":"Pharma"},
    {"symbol":"CIPLA","name":"Cipla","sector":"Pharma"},
    {"symbol":"DRREDDY","name":"Dr. Reddy's Laboratories","sector":"Pharma"},
    {"symbol":"DIVISLAB","name":"Divi's Laboratories","sector":"Pharma"},
    {"symbol":"ZYDUSLIFE","name":"Zydus Lifesciences","sector":"Pharma"},
    {"symbol":"AUROPHARMA","name":"Aurobindo Pharma","sector":"Pharma"},
    {"symbol":"LUPIN","name":"Lupin","sector":"Pharma"},
    {"symbol":"TORNTPHARM","name":"Torrent Pharma","sector":"Pharma"},
    {"symbol":"ALKEM","name":"Alkem Laboratories","sector":"Pharma"},
    {"symbol":"GLENMARK","name":"Glenmark Pharma","sector":"Pharma"},
    {"symbol":"GRANULES","name":"Granules India","sector":"Pharma"},
    {"symbol":"LAURUSLABS","name":"Laurus Labs","sector":"Pharma"},
    {"symbol":"IPCALAB","name":"IPCA Laboratories","sector":"Pharma"},
    {"symbol":"ABBOTINDIA","name":"Abbott India","sector":"Pharma"},
    {"symbol":"NATCOPHARM","name":"Natco Pharma","sector":"Pharma"},
    {"symbol":"AJANTPHARM","name":"Ajanta Pharma","sector":"Pharma"},
    {"symbol":"APOLLOHOSP","name":"Apollo Hospitals","sector":"Healthcare"},
    {"symbol":"LALPATHLAB","name":"Dr Lal PathLabs","sector":"Healthcare"},
    {"symbol":"MAXHEALTH","name":"Max Healthcare","sector":"Healthcare"},
    {"symbol":"FORTIS","name":"Fortis Healthcare","sector":"Healthcare"},
    {"symbol":"METROPOLIS","name":"Metropolis Healthcare","sector":"Healthcare"},
    {"symbol":"KIMS","name":"Krishna Institute of Medical Sciences","sector":"Healthcare"},
    {"symbol":"NH","name":"Narayana Hrudayalaya","sector":"Healthcare"},
    {"symbol":"JSWSTEEL","name":"JSW Steel","sector":"Metals"},
    {"symbol":"TATASTEEL","name":"Tata Steel","sector":"Metals"},
    {"symbol":"SAIL","name":"SAIL","sector":"Metals"},
    {"symbol":"HINDZINC","name":"Hindustan Zinc","sector":"Metals"},
    {"symbol":"VEDL","name":"Vedanta","sector":"Metals"},
    {"symbol":"NATIONALUM","name":"National Aluminium","sector":"Metals"},
    {"symbol":"HINDCOPPER","name":"Hindustan Copper","sector":"Metals"},
    {"symbol":"APLAPOLLO","name":"APL Apollo Tubes","sector":"Metals"},
    {"symbol":"RATNAMANI","name":"Ratnamani Metals","sector":"Metals"},
    {"symbol":"NMDC","name":"NMDC","sector":"Mining"},
    {"symbol":"COALINDIA","name":"Coal India","sector":"Mining"},
    {"symbol":"JINDALSTEL","name":"Jindal Steel & Power","sector":"Metals"},
    {"symbol":"ULTRACEMCO","name":"UltraTech Cement","sector":"Cement"},
    {"symbol":"GRASIM","name":"Grasim Industries","sector":"Cement"},
    {"symbol":"AMBUJACEM","name":"Ambuja Cements","sector":"Cement"},
    {"symbol":"ACC","name":"ACC","sector":"Cement"},
    {"symbol":"SHREECEM","name":"Shree Cement","sector":"Cement"},
    {"symbol":"JKCEMENT","name":"JK Cement","sector":"Cement"},
    {"symbol":"RAMCOCEM","name":"The Ramco Cements","sector":"Cement"},
    {"symbol":"KAJARIACER","name":"Kajaria Ceramics","sector":"Building"},
    {"symbol":"ASTRAL","name":"Astral","sector":"Building"},
    {"symbol":"SUPREMEIND","name":"Supreme Industries","sector":"Building"},
    {"symbol":"CENTURYPLY","name":"Century Plyboards","sector":"Building"},
    {"symbol":"ASIANPAINT","name":"Asian Paints","sector":"Paints"},
    {"symbol":"BERGEPAINT","name":"Berger Paints","sector":"Paints"},
    {"symbol":"KANSAINER","name":"Kansai Nerolac Paints","sector":"Paints"},
    {"symbol":"PIDILITIND","name":"Pidilite Industries","sector":"Chemicals"},
    {"symbol":"SRF","name":"SRF","sector":"Chemicals"},
    {"symbol":"DEEPAKNTR","name":"Deepak Nitrite","sector":"Chemicals"},
    {"symbol":"NAVINFLUOR","name":"Navin Fluorine","sector":"Chemicals"},
    {"symbol":"AARTIIND","name":"Aarti Industries","sector":"Chemicals"},
    {"symbol":"VINATI","name":"Vinati Organics","sector":"Chemicals"},
    {"symbol":"TATACHEM","name":"Tata Chemicals","sector":"Chemicals"},
    {"symbol":"ALKYLAMINE","name":"Alkyl Amines Chemicals","sector":"Chemicals"},
    {"symbol":"LT","name":"Larsen & Toubro","sector":"Engineering"},
    {"symbol":"SIEMENS","name":"Siemens India","sector":"Engineering"},
    {"symbol":"ABB","name":"ABB India","sector":"Engineering"},
    {"symbol":"BHEL","name":"Bharat Heavy Electricals","sector":"Engineering"},
    {"symbol":"THERMAX","name":"Thermax","sector":"Engineering"},
    {"symbol":"CUMMINSIND","name":"Cummins India","sector":"Engineering"},
    {"symbol":"GRINDWELL","name":"Grindwell Norton","sector":"Engineering"},
    {"symbol":"AIAENG","name":"AIA Engineering","sector":"Engineering"},
    {"symbol":"PRAJ","name":"Praj Industries","sector":"Engineering"},
    {"symbol":"BEL","name":"Bharat Electronics","sector":"Defence"},
    {"symbol":"HAL","name":"Hindustan Aeronautics","sector":"Defence"},
    {"symbol":"MAZDOCK","name":"Mazagon Dock","sector":"Defence"},
    {"symbol":"COCHINSHIP","name":"Cochin Shipyard","sector":"Defence"},
    {"symbol":"GRSE","name":"Garden Reach Shipbuilders","sector":"Defence"},
    {"symbol":"BDL","name":"Bharat Dynamics","sector":"Defence"},
    {"symbol":"ADANIPORTS","name":"Adani Ports","sector":"Infrastructure"},
    {"symbol":"ADANIENT","name":"Adani Enterprises","sector":"Diversified"},
    {"symbol":"DLF","name":"DLF","sector":"Real Estate"},
    {"symbol":"GODREJPROP","name":"Godrej Properties","sector":"Real Estate"},
    {"symbol":"OBEROIRLTY","name":"Oberoi Realty","sector":"Real Estate"},
    {"symbol":"PRESTIGE","name":"Prestige Estates","sector":"Real Estate"},
    {"symbol":"BRIGADE","name":"Brigade Enterprises","sector":"Real Estate"},
    {"symbol":"PHOENIXLTD","name":"Phoenix Mills","sector":"Real Estate"},
    {"symbol":"LODHA","name":"Macrotech Developers","sector":"Real Estate"},
    {"symbol":"TITAN","name":"Titan Company","sector":"Consumer"},
    {"symbol":"TRENT","name":"Trent","sector":"Retail"},
    {"symbol":"DMART","name":"Avenue Supermarts","sector":"Retail"},
    {"symbol":"BATAINDIA","name":"Bata India","sector":"Retail"},
    {"symbol":"NYKAA","name":"Nykaa","sector":"E-Commerce"},
    {"symbol":"KALYANKJIL","name":"Kalyan Jewellers","sector":"Jewellery"},
    {"symbol":"HAVELLS","name":"Havells India","sector":"Electronics"},
    {"symbol":"VOLTAS","name":"Voltas","sector":"Electronics"},
    {"symbol":"POLYCAB","name":"Polycab India","sector":"Electronics"},
    {"symbol":"DIXON","name":"Dixon Technologies","sector":"Electronics"},
    {"symbol":"AMBER","name":"Amber Enterprises","sector":"Electronics"},
    {"symbol":"VGUARD","name":"V-Guard Industries","sector":"Electronics"},
    {"symbol":"KEI","name":"KEI Industries","sector":"Electronics"},
    {"symbol":"BLUESTAR","name":"Blue Star","sector":"Electronics"},
    {"symbol":"BHARTIARTL","name":"Bharti Airtel","sector":"Telecom"},
    {"symbol":"HFCL","name":"HFCL","sector":"Telecom"},
    {"symbol":"TATACOMM","name":"Tata Communications","sector":"Telecom"},
    {"symbol":"INDUS","name":"Indus Towers","sector":"Telecom"},
    {"symbol":"JUBLFOOD","name":"Jubilant FoodWorks","sector":"QSR"},
    {"symbol":"WESTLIFE","name":"Westlife Foodworld","sector":"QSR"},
    {"symbol":"DEVYANI","name":"Devyani International","sector":"QSR"},
    {"symbol":"INDHOTEL","name":"Indian Hotels (Taj)","sector":"Hospitality"},
    {"symbol":"EIH","name":"EIH (Oberoi Hotels)","sector":"Hospitality"},
    {"symbol":"LEMONTRE","name":"Lemon Tree Hotels","sector":"Hospitality"},
    {"symbol":"IRCTC","name":"IRCTC","sector":"Travel"},
    {"symbol":"INTERGLOBE","name":"IndiGo (InterGlobe)","sector":"Aviation"},
    {"symbol":"VBL","name":"Varun Beverages","sector":"Beverages"},
    {"symbol":"UBL","name":"United Breweries","sector":"Beverages"},
    {"symbol":"RADICO","name":"Radico Khaitan","sector":"Beverages"},
    {"symbol":"ZOMATO","name":"Zomato","sector":"New Age Tech"},
    {"symbol":"PAYTM","name":"Paytm","sector":"Fintech"},
    {"symbol":"DELHIVERY","name":"Delhivery","sector":"Logistics"},
    {"symbol":"POLICYBZR","name":"PB Fintech","sector":"Fintech"},
    {"symbol":"CHAMBAL","name":"Chambal Fertilisers","sector":"Fertilizers"},
    {"symbol":"COROMANDEL","name":"Coromandel International","sector":"Fertilizers"},
    {"symbol":"PIIND","name":"PI Industries","sector":"Agro Chemicals"},
    {"symbol":"RALLIS","name":"Rallis India","sector":"Agro Chemicals"},
    {"symbol":"WELSPUNLIV","name":"Welspun Living","sector":"Textile"},
    {"symbol":"RAYMOND","name":"Raymond","sector":"Textile"},
    {"symbol":"KPRMILL","name":"KPR Mill","sector":"Textile"},
    {"symbol":"TRIDENT","name":"Trident","sector":"Textile"},
    {"symbol":"ZEEL","name":"Zee Entertainment","sector":"Media"},
    {"symbol":"SUNTV","name":"Sun TV Network","sector":"Media"},
    {"symbol":"PVRINOX","name":"PVR INOX","sector":"Media"},
    {"symbol":"SAREGAMA","name":"Saregama India","sector":"Media"},
    {"symbol":"BLUEDART","name":"Blue Dart Express","sector":"Logistics"},
    {"symbol":"CONCOR","name":"Container Corp of India","sector":"Logistics"},
    {"symbol":"GODREJIND","name":"Godrej Industries","sector":"Diversified"},
    {"symbol":"BALRAMCHIN","name":"Balrampur Chini Mills","sector":"Sugar"},
    {"symbol":"PAGEIND","name":"Page Industries","sector":"Textile"},
    {"symbol":"MOFSL","name":"Motilal Oswal Financial","sector":"Finance"},
]

_seen = set(); _dd = []
for s in STOCKS:
    if s["symbol"] not in _seen: _seen.add(s["symbol"]); _dd.append(s)
STOCKS = _dd
STOCK_MAP = {s["symbol"]: s for s in STOCKS}

INDICES_LIST = [
    {"symbol":"^NSEI",      "name":"NIFTY 50",       "yf":"^NSEI",       "category":"Broad"},
    {"symbol":"^NSEBANK",   "name":"NIFTY BANK",      "yf":"^NSEBANK",    "category":"Broad"},
    {"symbol":"^CNX100",    "name":"NIFTY 100",       "yf":"^CNX100",     "category":"Broad"},
    {"symbol":"^CNX200",    "name":"NIFTY 200",       "yf":"^CNX200",     "category":"Broad"},
    {"symbol":"^CRSLDX",    "name":"NIFTY 500",       "yf":"^CRSLDX",     "category":"Broad"},
    {"symbol":"^NSMIDCP",   "name":"NIFTY MIDCAP 50", "yf":"^NSMIDCP",    "category":"MidSmall"},
    {"symbol":"^CNXSC",     "name":"NIFTY SMALLCAP",  "yf":"^CNXSC",      "category":"MidSmall"},
    {"symbol":"^CNXIT",     "name":"NIFTY IT",        "yf":"^CNXIT",      "category":"Sectoral"},
    {"symbol":"^CNXPHARMA", "name":"NIFTY PHARMA",    "yf":"^CNXPHARMA",  "category":"Sectoral"},
    {"symbol":"^CNXAUTO",   "name":"NIFTY AUTO",      "yf":"^CNXAUTO",    "category":"Sectoral"},
    {"symbol":"^CNXFMCG",   "name":"NIFTY FMCG",      "yf":"^CNXFMCG",    "category":"Sectoral"},
    {"symbol":"^CNXMETAL",  "name":"NIFTY METAL",     "yf":"^CNXMETAL",   "category":"Sectoral"},
    {"symbol":"^CNXREALTY", "name":"NIFTY REALTY",    "yf":"^CNXREALTY",  "category":"Sectoral"},
    {"symbol":"^CNXENERGY", "name":"NIFTY ENERGY",    "yf":"^CNXENERGY",  "category":"Sectoral"},
    {"symbol":"^CNXINFRA",  "name":"NIFTY INFRA",     "yf":"^CNXINFRA",   "category":"Sectoral"},
    {"symbol":"^CNXPSE",    "name":"NIFTY PSE",       "yf":"^CNXPSE",     "category":"Sectoral"},
]

store = {
    "last_update": "Not yet updated",
    "is_market_open": False,
    "initialized": False,
    "stocks": {},
    "ohlcv": {},
    "weekly_ohlcv": {},
    "indices": {},
    "price_history": {},
    "notifications": deque(maxlen=100),
    "rsi_store": {},
    # ── NEW v7 ──
    # confluence_signals: {sym: {signal_data}} active in-zone stocks
    "confluence_signals": {},
    # confluence_zone_state: {sym: bool} True = currently inside zone
    "confluence_zone_state": {},
    # fivey_breakouts: {sym: {breakout_data}} stocks above 5Y high
    "fivey_breakouts": {},
    # fivey_highs: {sym: float} pre-computed 5Y highs (ex current session)
    "fivey_highs": {},
    # Today's highs cache for 5Y breakout detection
    "today_highs": {},
}

PERIOD_MAP = {
    "1H" : {"period": "1d",  "interval": "5m"},
    "1D" : {"period": "5d",  "interval": "30m"},
    "5D" : {"period": "5d",  "interval": "1d"},
    "1W" : {"period": "5d",  "interval": "60m"},
    "1M" : {"period": "1mo", "interval": "1d"},
    "3M" : {"period": "3mo", "interval": "1d"},
    "6M" : {"period": "6mo", "interval": "1d"},
    "1Y" : {"period": "1y",  "interval": "1d"},
    "5Y" : {"period": "5y",  "interval": "1wk"},
    "ALL": {"period": "max", "interval": "1mo"},
}

# ─────────────────────────────────────────────
# WEBSOCKET MANAGER
# ─────────────────────────────────────────────
class WSManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        self.active = [c for c in self.active if c != ws]

    async def broadcast(self, data: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

ws_manager = WSManager()


# ─────────────────────────────────────────────
# NOTIFICATIONS
# ─────────────────────────────────────────────
def add_notification(ntype, symbol, name, price, detail=""):
    ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    store["notifications"].appendleft({
        "id": int(time.time() * 1000), "type": ntype, "symbol": symbol,
        "name": name, "price": price, "detail": detail, "timestamp": ts, "read": False
    })


# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────
def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS signal_history(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, signal TEXT, date TEXT, price REAL, ts TEXT)""")
    # New table: confluence signal log
    con.execute("""CREATE TABLE IF NOT EXISTS confluence_history(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, name TEXT, sector TEXT,
        price REAL, sma5 REAL, ema13 REAL, ema26 REAL,
        avg_ma REAL, dist_pct REAL,
        trigger_time TEXT, date TEXT)""")
    # New table: 5Y breakout log
    con.execute("""CREATE TABLE IF NOT EXISTS breakout_history(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, name TEXT, sector TEXT,
        breakout_price REAL, fivey_high REAL, pct_above REAL,
        volume INTEGER, avg_volume REAL,
        breakout_date TEXT, breakout_time TEXT)""")
    con.commit(); con.close()


def save_signal(symbol, signal, date, price):
    ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    con = sqlite3.connect(DB_PATH)
    con.execute("INSERT INTO signal_history VALUES(NULL,?,?,?,?,?)",
                (symbol, signal, date, price, ts))
    con.commit(); con.close()


def save_confluence(sym, entry):
    con = sqlite3.connect(DB_PATH)
    con.execute("""INSERT INTO confluence_history
        VALUES(NULL,?,?,?,?,?,?,?,?,?,?,?)""", (
        sym, entry["name"], entry["sector"],
        entry["current_price"], entry["sma5"], entry["ema13"], entry["ema26"],
        entry["avg_ma"], entry["dist_pct"],
        entry["trigger_time"], entry["trigger_date"]
    ))
    con.commit(); con.close()


def save_breakout(sym, entry):
    con = sqlite3.connect(DB_PATH)
    con.execute("""INSERT INTO breakout_history
        VALUES(NULL,?,?,?,?,?,?,?,?,?,?)""", (
        sym, entry["name"], entry["sector"],
        entry["breakout_price"], entry["fivey_high"], entry["pct_above"],
        entry.get("volume", 0), entry.get("avg_volume", 0),
        entry["breakout_date"], entry["breakout_time"]
    ))
    con.commit(); con.close()


def get_history(limit=500):
    con = sqlite3.connect(DB_PATH)
    cur = con.execute("SELECT symbol,signal,date,price,ts FROM signal_history ORDER BY id DESC LIMIT ?", (limit,))
    rows = cur.fetchall(); con.close()
    result = []
    for r in rows:
        sym = r[0]; e = STOCK_MAP.get(sym, {})
        cp = store["stocks"].get(sym, {}).get("current_price", r[3])
        pct = round((cp - r[3]) / r[3] * 100, 2) if r[3] else 0
        result.append({"symbol": sym, "name": e.get("name", sym), "sector": e.get("sector", ""),
                       "signal": r[1], "signal_date": r[2], "signal_price": r[3],
                       "current_price": cp, "pct_change": pct, "timestamp": r[4]})
    return result


# ─────────────────────────────────────────────
# INDICATORS
# ─────────────────────────────────────────────
def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, 1e-10)
    return (100 - (100 / (1 + rs))).round(2)


def compute_indicators(df):
    df = df.copy(); df.sort_index(inplace=True)
    df["SMA5"]    = df["Close"].rolling(5).mean().round(2)
    df["EMA13"]   = df["Close"].ewm(span=13, adjust=False).mean().round(2)
    df["EMA26"]   = df["Close"].ewm(span=26, adjust=False).mean().round(2)
    df["VolSMA20"] = df["Volume"].rolling(20).mean().round(0)
    df["RSI14"]   = compute_rsi(df["Close"], 14)
    df["MaxInd"]  = df[["SMA5", "EMA13", "EMA26"]].max(axis=1)
    df["MinInd"]  = df[["SMA5", "EMA13", "EMA26"]].min(axis=1)
    df["Conjunction"] = (df["MaxInd"] * 0.99) <= (df["MinInd"] * 1.01)
    df["VolConfirm"]  = df["Volume"] > (df["VolSMA20"] * 1.5)
    df["SMA5_Rising"] = df["SMA5"] > df["SMA5"].shift(2)
    df["SMA5_Falling"] = df["SMA5"] < df["SMA5"].shift(2)
    df["BuySignal"]  = df["Conjunction"] & df["VolConfirm"] & (df["Close"] > df["SMA5"]) & df["SMA5_Rising"]
    df["SellSignal"] = df["Conjunction"] & df["VolConfirm"] & (df["Close"] < df["SMA5"]) & df["SMA5_Falling"]
    return df


def compute_weekly_rsi(df_daily):
    try:
        df_w = df_daily["Close"].resample("W").last().dropna()
        if len(df_w) < 15: return None, None
        rsi = compute_rsi(df_w, 14)
        return float(rsi.iloc[-1]), float(rsi.iloc[-2]) if len(rsi) > 1 else None
    except:
        return None, None


def detect_signal(df):
    result = {"signal": "HOLD", "signal_date": None, "signal_price": None}
    for i in range(len(df) - 1, -1, -1):
        row = df.iloc[i]
        if row.get("BuySignal", False):
            result = {"signal": "BUY", "signal_date": df.index[i].strftime("%Y-%m-%d"),
                      "signal_price": float(row["Close"])}; break
        if row.get("SellSignal", False):
            result = {"signal": "SELL", "signal_date": df.index[i].strftime("%Y-%m-%d"),
                      "signal_price": float(row["Close"])}; break
    return result


# ─────────────────────────────────────────────
# NSE PRICE FETCHER
# ─────────────────────────────────────────────
_nse_session = None; _nse_ts = 0.0


def get_nse_session():
    global _nse_session, _nse_ts
    if _nse_session is None or (time.time() - _nse_ts) > 300:
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
                          "Referer": "https://www.nseindia.com", "Accept": "*/*"})
        try: s.get("https://www.nseindia.com", timeout=15); time.sleep(0.5)
        except: pass
        _nse_session = s; _nse_ts = time.time()
    return _nse_session


def fetch_nse_prices():
    s = get_nse_session(); prices = {}
    for index in ["NIFTY%20200", "NIFTY%20MIDCAP%20150", "NIFTY%20SMALLCAP%20250"]:
        try:
            r = s.get(f"https://www.nseindia.com/api/equity-stockIndices?index={index}", timeout=15)
            r.raise_for_status()
            for item in r.json().get("data", []):
                sym = item.get("symbol", ""); ltp = item.get("lastPrice", 0)
                dh = item.get("dayHigh", ltp)   # today's high from NSE
                if sym and ltp:
                    prices[sym] = {"ltp": float(str(ltp).replace(",", "")),
                                   "day_high": float(str(dh).replace(",", "")),
                                   "volume": int(str(item.get("totalTradedVolume", 0)).replace(",", "") or 0)}
        except Exception as e:
            log.warning(f"NSE {index}: {e}")
    return prices


# ─────────────────────────────────────────────
# HISTORICAL DATA LOAD
# ─────────────────────────────────────────────
def load_historical_data():
    log.info(f"Loading {len(STOCKS)} stocks...")
    symbols = [s["symbol"] for s in STOCKS]
    yf_syms = [f"{sym}.NS" for sym in symbols]
    for b in range(0, len(yf_syms), 50):
        byf = yf_syms[b:b + 50]; bsym = symbols[b:b + 50]
        log.info(f"Batch {b // 50 + 1}/{(len(yf_syms) + 49) // 50}")
        try:
            raw = yf.download(byf, period="90d", interval="1d", group_by="ticker",
                              auto_adjust=True, progress=False, threads=True)
            for sym, yfs in zip(bsym, byf):
                try:
                    df = raw.copy() if len(byf) == 1 else (
                        raw[yfs].copy() if yfs in raw.columns.get_level_values(0) else None)
                    if df is None: continue
                    df = df.dropna(subset=["Close"])
                    if df.empty: continue
                    df = compute_indicators(df); sig = detect_signal(df); last = df.iloc[-1]
                    def v(x): return round(float(x), 2) if not pd.isna(x) else None
                    rsi_cur, rsi_prev = compute_weekly_rsi(df)
                    store["rsi_store"][sym] = {"rsi": rsi_cur, "rsi_prev": rsi_prev}
                    store["stocks"][sym] = {
                        "symbol": sym, "name": STOCK_MAP[sym]["name"], "sector": STOCK_MAP[sym]["sector"],
                        "current_price": v(last["Close"]), "signal": sig["signal"],
                        "signal_date": sig["signal_date"], "signal_price": sig["signal_price"],
                        "pct_change": 0.0, "sma5": v(last["SMA5"]), "ema13": v(last["EMA13"]),
                        "ema26": v(last["EMA26"]), "volsma20": v(last["VolSMA20"]),
                        "conjunction": bool(last.get("Conjunction", False)),
                        "vol_confirm": bool(last.get("VolConfirm", False)),
                        "rsi": rsi_cur, "rsi_prev": rsi_prev,
                        "rsi14": v(last.get("RSI14")),
                    }
                    store["ohlcv"][sym] = df
                    closes = df["Close"].dropna().tolist()[-PRICE_HISTORY_LEN:]
                    store["price_history"][sym] = deque(closes, maxlen=PRICE_HISTORY_LEN)
                    if sig["signal_price"] and v(last["Close"]):
                        sp = sig["signal_price"]; cp = v(last["Close"])
                        store["stocks"][sym]["pct_change"] = round((cp - sp) / sp * 100, 2)
                except Exception as ex:
                    log.warning(f"{sym}: {ex}")
        except Exception as e:
            log.error(f"Batch: {e}")
        time.sleep(1)
    log.info(f"Done. {len(store['stocks'])} stocks loaded.")
    # After loading history, compute 5Y highs
    load_5y_highs()


def load_5y_highs():
    """Pre-compute 5-year highs (excluding current session) for all stocks."""
    log.info("Loading 5-year highs...")
    symbols = [s["symbol"] for s in STOCKS]
    yf_syms = [f"{sym}.NS" for sym in symbols]
    today_str = datetime.now(IST).strftime("%Y-%m-%d")

    for b in range(0, len(yf_syms), 30):
        byf = yf_syms[b:b + 30]; bsym = symbols[b:b + 30]
        try:
            raw = yf.download(byf, period="5y", interval="1d", group_by="ticker",
                              auto_adjust=True, progress=False, threads=True)
            for sym, yfs in zip(bsym, byf):
                try:
                    df = raw.copy() if len(byf) == 1 else (
                        raw[yfs].copy() if yfs in raw.columns.get_level_values(0) else None)
                    if df is None or df.empty: continue
                    df = df.dropna(subset=["Close"])
                    # Exclude today's session to get "previous 5Y high"
                    df_ex = df[df.index.strftime("%Y-%m-%d") < today_str]
                    if df_ex.empty: continue
                    high_5y = float(df_ex["High"].max())
                    store["fivey_highs"][sym] = high_5y
                except Exception as ex:
                    log.warning(f"5Y {sym}: {ex}")
        except Exception as e:
            log.error(f"5Y batch: {e}")
        time.sleep(1)
    log.info(f"5Y highs loaded for {len(store['fivey_highs'])} stocks.")


def load_index_data():
    try:
        for idx in INDICES_LIST:
            ticker = yf.Ticker(idx["yf"]); hist = ticker.history(period="2d")
            if len(hist) >= 1:
                price = float(hist["Close"].iloc[-1])
                prev = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else price
                pct = round((price - prev) / prev * 100, 2) if prev else 0
                day_open = float(hist["Open"].iloc[-1])
                day_high = float(hist["High"].iloc[-1])
                day_low  = float(hist["Low"].iloc[-1])
                store["indices"][idx["symbol"]] = {
                    "symbol": idx["symbol"], "name": idx["name"],
                    "category": idx.get("category", "Broad"),
                    "price": round(price, 2), "prev_close": round(prev, 2), "pct_change": pct,
                    "day_open": round(day_open, 2), "day_high": round(day_high, 2), "day_low": round(day_low, 2),
                    "change": round(price - prev, 2),
                }
    except Exception as e:
        log.warning(f"Index: {e}")


def is_market_hours():
    now = datetime.now(IST)
    if now.weekday() >= 5: return False
    return MARKET_OPEN <= (now.hour, now.minute) <= MARKET_CLOSE


# ─────────────────────────────────────────────
# ── NEW v7: CONFLUENCE SIGNAL ENGINE ──────────
# ─────────────────────────────────────────────
def check_confluence(sym, entry, ltp):
    """
    Check if current price is within ±1% of the average of SMA5, EMA13, EMA26.
    Fires signal on first entry into zone. Clears on exit.
    Returns (entered_zone: bool, signal_data: dict | None)
    """
    sma5  = entry.get("sma5")
    ema13 = entry.get("ema13")
    ema26 = entry.get("ema26")
    if not all([sma5, ema13, ema26, ltp]):
        return False, None

    avg_ma    = round((sma5 + ema13 + ema26) / 3, 2)
    upper     = round(avg_ma * 1.01, 2)
    lower     = round(avg_ma * 0.99, 2)
    in_zone   = lower <= ltp <= upper
    dist_pct  = round((ltp - avg_ma) / avg_ma * 100, 4)
    was_in    = store["confluence_zone_state"].get(sym, False)

    if in_zone:
        store["confluence_zone_state"][sym] = True
        if not was_in:
            # First entry — fire signal
            now_ist = datetime.now(IST)
            sig = {
                "symbol":       sym,
                "name":         entry["name"],
                "sector":       entry["sector"],
                "current_price": ltp,
                "sma5":         sma5,
                "ema13":        ema13,
                "ema26":        ema26,
                "avg_ma":       avg_ma,
                "upper_limit":  upper,
                "lower_limit":  lower,
                "dist_pct":     dist_pct,
                "signal_type":  "CONFLUENCE",
                "trigger_time": now_ist.strftime("%H:%M:%S"),
                "trigger_date": now_ist.strftime("%Y-%m-%d"),
                "timestamp":    now_ist.strftime("%Y-%m-%d %H:%M:%S"),
                "new":          True,
            }
            store["confluence_signals"][sym] = sig
            save_confluence(sym, sig)
            add_notification("CONFLUENCE", sym, entry["name"], ltp,
                             f"Price ₹{ltp} entered ±1% MA zone (Avg MA: ₹{avg_ma})")
            return True, sig
        else:
            # Already in zone — update price only
            if sym in store["confluence_signals"]:
                store["confluence_signals"][sym]["current_price"] = ltp
                store["confluence_signals"][sym]["dist_pct"] = dist_pct
                store["confluence_signals"][sym]["new"] = False
            return True, None
    else:
        # Exited zone — reset so next entry fires again
        store["confluence_zone_state"][sym] = False
        return False, None


# ─────────────────────────────────────────────
# ── NEW v7: 5-YEAR BREAKOUT ENGINE ────────────
# ─────────────────────────────────────────────
def check_5y_breakout(sym, entry, ltp, day_high, volume):
    """
    Check if current price OR today's high exceeds the pre-computed 5Y high.
    Fires once per session per stock (re-fires if price retreats and breaks out again
    only if it previously exited — kept simple: fires once per day).
    """
    fivey_high = store["fivey_highs"].get(sym)
    if not fivey_high or not ltp:
        return False, None

    triggered = (ltp > fivey_high) or (day_high and day_high > fivey_high)
    if not triggered:
        # Remove from active breakouts if price dropped back below
        if sym in store["fivey_breakouts"]:
            del store["fivey_breakouts"][sym]
        return False, None

    # Already tracked today → just update price
    if sym in store["fivey_breakouts"]:
        store["fivey_breakouts"][sym]["current_price"] = ltp
        store["fivey_breakouts"][sym]["pct_above"] = round((ltp - fivey_high) / fivey_high * 100, 2)
        store["fivey_breakouts"][sym]["volume"] = volume
        store["fivey_breakouts"][sym]["new"] = False
        return True, None

    # New breakout
    now_ist = datetime.now(IST)
    df = store["ohlcv"].get(sym)
    avg_vol = 0
    if df is not None and "VolSMA20" in df.columns:
        v20 = df["VolSMA20"].iloc[-1]
        avg_vol = round(float(v20), 0) if not pd.isna(v20) else 0

    breakout_price = max(ltp, day_high or ltp)
    pct_above = round((breakout_price - fivey_high) / fivey_high * 100, 2)

    bo = {
        "symbol":        sym,
        "name":          entry["name"],
        "sector":        entry["sector"],
        "current_price": ltp,
        "fivey_high":    round(fivey_high, 2),
        "breakout_price": round(breakout_price, 2),
        "pct_above":     pct_above,
        "breakout_date": now_ist.strftime("%Y-%m-%d"),
        "breakout_time": now_ist.strftime("%H:%M:%S"),
        "timestamp":     now_ist.strftime("%Y-%m-%d %H:%M:%S"),
        "volume":        volume,
        "avg_volume":    avg_vol,
        "vol_ratio":     round(volume / avg_vol, 2) if avg_vol else None,
        "new":           True,
    }
    store["fivey_breakouts"][sym] = bo
    save_breakout(sym, bo)
    add_notification("5Y_BREAKOUT", sym, entry["name"], ltp,
                     f"5Y High Breakout! ₹{ltp} > Previous 5Y High ₹{fivey_high:.2f} (+{pct_above}%)")
    log.info(f"5Y BREAKOUT {sym}@{ltp} above 5Y high {fivey_high}")
    return True, bo


# ─────────────────────────────────────────────
# PRICE UPDATE LOOP (runs every 1 min)
# ─────────────────────────────────────────────
def update_prices():
    store["is_market_open"] = is_market_hours()
    if not store["is_market_open"]: return
    try:
        raw_prices = fetch_nse_prices()
        log.info(f"NSE: {len(raw_prices)} prices")
    except Exception as e:
        log.error(f"NSE: {e}"); return

    now_ist = datetime.now(IST); today = now_ist.strftime("%Y-%m-%d")
    new_confluence = []; new_breakouts = []

    for sym, entry in store["stocks"].items():
        price_data = raw_prices.get(sym)
        if price_data is None: continue
        ltp       = price_data["ltp"]
        day_high  = price_data["day_high"]
        volume    = price_data["volume"]

        entry["current_price"] = ltp
        store["today_highs"][sym] = day_high
        h = store["price_history"].get(sym)
        if h: h.append(ltp)
        sp = entry.get("signal_price")
        if sp: entry["pct_change"] = round((ltp - sp) / sp * 100, 2)

        df = store["ohlcv"].get(sym)
        if df is None or df.empty: continue
        if today in df.index.strftime("%Y-%m-%d").tolist():
            li = df.index[-1]; df.at[li, "Close"] = ltp
            df.at[li, "High"] = max(df.at[li, "High"], ltp)
            df.at[li, "Low"]  = min(df.at[li, "Low"], ltp)
            df.at[li, "Volume"] = volume if volume else df.at[li, "Volume"]
        else:
            nr = pd.DataFrame(
                {"Open": [ltp], "High": [ltp], "Low": [ltp], "Close": [ltp], "Volume": [volume or 0]},
                index=[pd.Timestamp(today)])
            df = pd.concat([df, nr])
        df = compute_indicators(df); store["ohlcv"][sym] = df; last = df.iloc[-1]
        def v(x): return round(float(x), 2) if not pd.isna(x) else None
        prev_rsi = entry.get("rsi14")
        entry["sma5"]        = v(last["SMA5"])
        entry["ema13"]       = v(last["EMA13"])
        entry["ema26"]       = v(last["EMA26"])
        entry["rsi14"]       = v(last.get("RSI14"))
        entry["conjunction"] = bool(last.get("Conjunction", False))
        entry["vol_confirm"] = bool(last.get("VolConfirm", False))
        cur_rsi = entry["rsi14"]
        if prev_rsi is not None and cur_rsi is not None:
            if prev_rsi < 49 and cur_rsi >= 49:
                add_notification("RSI_CROSS", sym, entry["name"], ltp,
                                 f"Weekly RSI crossed above 49 — now at {cur_rsi}")
        prev_sig = entry.get("signal")
        if df["BuySignal"].iloc[-1] and prev_sig != "BUY":
            entry.update({"signal": "BUY", "signal_date": today, "signal_price": ltp, "pct_change": 0.0})
            save_signal(sym, "BUY", today, ltp)
            add_notification("BUY", sym, entry["name"], ltp,
                             f"Triple Confluence BUY signal fired at ₹{ltp}")
            log.info(f"BUY {sym}@{ltp}")
        elif df["SellSignal"].iloc[-1] and prev_sig != "SELL":
            entry.update({"signal": "SELL", "signal_date": today, "signal_price": ltp, "pct_change": 0.0})
            save_signal(sym, "SELL", today, ltp)
            add_notification("SELL", sym, entry["name"], ltp,
                             f"Triple Confluence SELL signal fired at ₹{ltp}")
            log.info(f"SELL {sym}@{ltp}")

        # ── v7: Confluence zone check
        _, conf_sig = check_confluence(sym, entry, ltp)
        if conf_sig:
            new_confluence.append(conf_sig)
            log.info(f"CONFLUENCE {sym}@{ltp} dist={conf_sig['dist_pct']}%")

        # ── v7: 5Y Breakout check
        _, bo_sig = check_5y_breakout(sym, entry, ltp, day_high, volume)
        if bo_sig:
            new_breakouts.append(bo_sig)

    store["last_update"] = now_ist.strftime("%Y-%m-%d %H:%M:%S IST")

    # Broadcast via WebSocket if any new signals
    if new_confluence or new_breakouts:
        asyncio.create_task(_broadcast_signals(new_confluence, new_breakouts))


async def _broadcast_signals(confluence, breakouts):
    if confluence:
        await ws_manager.broadcast({"type": "confluence", "signals": confluence})
    if breakouts:
        await ws_manager.broadcast({"type": "breakout",   "signals": breakouts})


def index_update_job():
    if is_market_hours(): load_index_data()


# ─────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────
app = FastAPI(title="Nifty500 Signals v7", version="7.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False,
                  allow_methods=["*"], allow_headers=["*"])


# ── WebSocket endpoint ──────────────────────
@app.websocket("/ws/signals")
async def ws_signals(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()   # keep connection alive
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)


# ── Health ──────────────────────────────────
@app.api_route("/api/health", methods=["GET", "HEAD"])
def health():
    return {"status": "ok", "initialized": store["initialized"],
            "last_update": store["last_update"],
            "is_market_open": store["is_market_open"],
            "total_stocks": len(store["stocks"])}


# ── Signals (with optional sector filter) ───
@app.get("/api/signals")
def get_signals(sector: Optional[str] = Query(None)):
    order = {"BUY": 0, "SELL": 1, "HOLD": 2}
    sl = list(store["stocks"].values())
    if sector:
        sl = [s for s in sl if s.get("sector", "").lower() == sector.lower()]
    sl.sort(key=lambda x: (order.get(x.get("signal", "HOLD"), 2),
                           -(pd.Timestamp(x["signal_date"]).timestamp()
                             if x.get("signal_date") else 0)))
    for s in sl:
        h = store["price_history"].get(s["symbol"])
        s["price_history"] = list(h) if h else []
    return {"last_update": store["last_update"],
            "is_market_open": store["is_market_open"],
            "total": len(sl), "stocks": sl}


@app.get("/api/history")
def get_history_ep(): return {"history": get_history(500)}


@app.get("/api/indices")
def get_indices(): return {"indices": list(store["indices"].values())}


@app.get("/api/notifications")
def get_notifications():
    return {"notifications": list(store["notifications"])}


@app.post("/api/notifications/read")
def mark_read():
    for n in store["notifications"]: n["read"] = True
    return {"ok": True}


@app.get("/api/rsi-screener")
def rsi_screener(min_rsi: float = 45, max_rsi: float = 60, sector: Optional[str] = Query(None)):
    result = []
    for sym, entry in store["stocks"].items():
        rsi = entry.get("rsi14")
        if rsi is None: continue
        if not (min_rsi <= rsi <= max_rsi): continue
        if sector and entry.get("sector", "").lower() != sector.lower(): continue
        prev_rsi = store["rsi_store"].get(sym, {}).get("rsi_prev")
        rising = prev_rsi is not None and rsi > prev_rsi
        h = store["price_history"].get(sym)
        result.append({
            "symbol": sym, "name": entry["name"], "sector": entry["sector"],
            "current_price": entry["current_price"], "rsi": rsi, "rsi_prev": prev_rsi,
            "rsi_rising": rising, "signal": entry["signal"],
            "pct_change": entry.get("pct_change", 0),
            "price_history": list(h) if h else [],
        })
    result.sort(key=lambda x: x["rsi"], reverse=True)
    return {"stocks": result, "count": len(result)}


# ── NEW v7: Confluence signals endpoint ─────
@app.get("/api/confluence")
def get_confluence(sector: Optional[str] = Query(None)):
    """
    Returns all stocks currently inside the ±1% Average MA zone,
    sorted by most recently triggered, with optional sector filter.
    """
    sigs = list(store["confluence_signals"].values())
    if sector:
        sigs = [s for s in sigs if s.get("sector", "").lower() == sector.lower()]
    sigs.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    return {"count": len(sigs), "signals": sigs,
            "last_update": store["last_update"],
            "is_market_open": store["is_market_open"]}


@app.get("/api/confluence/history")
def get_confluence_history(limit: int = 200, sector: Optional[str] = Query(None)):
    con = sqlite3.connect(DB_PATH)
    if sector:
        cur = con.execute(
            "SELECT * FROM confluence_history WHERE sector=? ORDER BY id DESC LIMIT ?",
            (sector, limit))
    else:
        cur = con.execute(
            "SELECT * FROM confluence_history ORDER BY id DESC LIMIT ?", (limit,))
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    con.close()
    return {"history": rows, "count": len(rows)}


# ── NEW v7: 5-Year High Breakouts endpoint ──
@app.get("/api/breakouts/5y")
def get_5y_breakouts(
    sector: Optional[str] = Query(None),
    min_pct: float = Query(0.0, description="Minimum % above 5Y high"),
    sort_by: str = Query("pct", description="pct | time | volume | name")
):
    """
    Returns all stocks that have broken above their previous 5Y high today,
    with optional sector, strength, and sort filters.
    """
    bos = list(store["fivey_breakouts"].values())
    if sector:
        bos = [b for b in bos if b.get("sector", "").lower() == sector.lower()]
    bos = [b for b in bos if b.get("pct_above", 0) >= min_pct]
    if sort_by == "time":
        bos.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    elif sort_by == "volume":
        bos.sort(key=lambda x: (x.get("vol_ratio") or 0), reverse=True)
    elif sort_by == "name":
        bos.sort(key=lambda x: x.get("symbol", ""))
    else:  # pct default
        bos.sort(key=lambda x: x.get("pct_above", 0), reverse=True)
    return {"count": len(bos), "breakouts": bos,
            "last_update": store["last_update"],
            "is_market_open": store["is_market_open"]}


@app.get("/api/breakouts/5y/history")
def get_5y_breakout_history(limit: int = 200, sector: Optional[str] = Query(None)):
    con = sqlite3.connect(DB_PATH)
    if sector:
        cur = con.execute(
            "SELECT * FROM breakout_history WHERE sector=? ORDER BY id DESC LIMIT ?",
            (sector, limit))
    else:
        cur = con.execute(
            "SELECT * FROM breakout_history ORDER BY id DESC LIMIT ?", (limit,))
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    con.close()
    return {"history": rows, "count": len(rows)}


# ── Sectors list ─────────────────────────────
@app.get("/api/sectors")
def get_sectors():
    sectors = sorted(set(s["sector"] for s in STOCKS if s.get("sector")))
    return {"sectors": sectors}


# ── Chart data ───────────────────────────────
@app.get("/api/chart-data/{symbol}/{period}")
def get_chart_data(symbol: str, period: str):
    symbol = symbol.upper()
    info = STOCK_MAP.get(symbol)
    if not info: raise HTTPException(status_code=404, detail="Symbol not found")
    cfg = PERIOD_MAP.get(period, PERIOD_MAP["3M"])
    try:
        ticker = yf.Ticker(f"{symbol}.NS")
        df = ticker.history(period=cfg["period"], interval=cfg["interval"], auto_adjust=True)
        if df.empty: raise HTTPException(status_code=404, detail="No data")
        df.sort_index(inplace=True)
        if hasattr(df.index, "tz") and df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        if len(df) > 5:
            df["SMA5"]  = df["Close"].rolling(5).mean()
            df["EMA13"] = df["Close"].ewm(span=13, adjust=False).mean()
            df["EMA26"] = df["Close"].ewm(span=26, adjust=False).mean()
            df["RSI14"] = compute_rsi(df["Close"], 14)
        def v(x):
            try: return None if pd.isna(x) else round(float(x), 2)
            except: return None
        rows = []
        for dt, row in df.iterrows():
            rows.append({
                "date": dt.strftime("%Y-%m-%d %H:%M") if cfg["interval"] in ["5m", "30m", "60m"] else dt.strftime("%Y-%m-%d"),
                "open": v(row.get("Open")), "high": v(row.get("High")),
                "low": v(row.get("Low")), "close": v(row.get("Close")),
                "volume": int(row["Volume"]) if "Volume" in row and not pd.isna(row["Volume"]) else 0,
                "sma5": v(row.get("SMA5")), "ema13": v(row.get("EMA13")),
                "ema26": v(row.get("EMA26")), "rsi14": v(row.get("RSI14")),
            })
        return {"symbol": symbol, "period": period, "data": rows}
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"chart-data {symbol}/{period}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/index-history/{symbol}/{period}")
def get_index_history(symbol: str, period: str):
    cfg = PERIOD_MAP.get(period, PERIOD_MAP["3M"])
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=cfg["period"], interval=cfg["interval"], auto_adjust=True)
        if df.empty: raise HTTPException(status_code=404, detail="No data")
        if hasattr(df.index, "tz") and df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        def v(x):
            try: return None if pd.isna(x) else round(float(x), 2)
            except: return None
        rows = []
        for dt, row in df.iterrows():
            rows.append({
                "date": dt.strftime("%Y-%m-%d %H:%M") if cfg["interval"] in ["5m", "30m", "60m"] else dt.strftime("%Y-%m-%d"),
                "close": v(row.get("Close")), "high": v(row.get("High")),
                "low": v(row.get("Low")), "open": v(row.get("Open")),
            })
        return {"symbol": symbol, "period": period, "data": rows}
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"index-history {symbol}/{period}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    symbol = symbol.upper()
    if symbol not in store["ohlcv"]: raise HTTPException(status_code=404, detail="Symbol not found")
    df = store["ohlcv"][symbol].copy().tail(90)
    rows = []
    for dt, row in df.iterrows():
        def v(x): return None if pd.isna(x) else round(float(x), 2)
        rows.append({
            "date": dt.strftime("%Y-%m-%d"), "open": v(row["Open"]), "high": v(row["High"]),
            "low": v(row["Low"]), "close": v(row["Close"]),
            "volume": int(row["Volume"]) if not pd.isna(row["Volume"]) else 0,
            "sma5": v(row.get("SMA5")), "ema13": v(row.get("EMA13")),
            "ema26": v(row.get("EMA26")), "rsi14": v(row.get("RSI14")),
            "volsma20": v(row.get("VolSMA20")),
            "conjunction": bool(row.get("Conjunction", False)),
            "vol_confirm": bool(row.get("VolConfirm", False)),
            "buy_signal": bool(row.get("BuySignal", False)),
            "sell_signal": bool(row.get("SellSignal", False)),
        })
    info = store["stocks"].get(symbol, STOCK_MAP.get(symbol, {})).copy()
    h = store["price_history"].get(symbol)
    info["price_history"] = list(h) if h else []
    # Attach 5Y high if available
    info["fivey_high"] = store["fivey_highs"].get(symbol)
    return {"symbol": symbol, "info": info, "ohlcv": rows}


# ─────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────
@app.on_event("startup")
def startup():
    init_db()
    load_historical_data()   # includes load_5y_highs()
    load_index_data()
    store["is_market_open"] = is_market_hours()
    store["initialized"] = True
    scheduler = BackgroundScheduler(timezone=IST)
    scheduler.add_job(update_prices,       "interval", minutes=1,  id="price_update")
    scheduler.add_job(index_update_job,    "interval", minutes=5,  id="index_update")
    scheduler.add_job(load_historical_data,"cron", hour=9, minute=0, day_of_week="mon-fri", id="daily_reload")
    # Reload 5Y highs every morning (before market open) to refresh the baseline
    scheduler.add_job(load_5y_highs,       "cron", hour=8, minute=50, day_of_week="mon-fri", id="fivey_reload")
    # Reset today's breakout tracker every morning
    scheduler.add_job(lambda: store["fivey_breakouts"].clear(), "cron",
                      hour=9, minute=10, day_of_week="mon-fri", id="breakout_reset")
    scheduler.start()
    log.info("NiftySignals v7 server ready.")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
