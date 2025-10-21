"""
NSE Gold & Silver ETF Scraper Module
Complete scraping logic extracted from app.py

This module contains:
- ETF database (all 20 ETFs)
- NSE scraping with Selenium
- AMC website fallback scrapers
- Parallel batch processing
"""

import time
import re
import os
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
try:
    from .mcx_scraper import get_mcx_spot_prices  # For package import
except ImportError:
    from mcx_scraper import get_mcx_spot_prices   # For direct execution


# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# STATIC DATA CACHE CONFIGURATION
# ============================================================================

STATIC_CACHE_FILE = 'etf_static_cache.csv'
STATIC_CACHE_FILE_DATA = 'data/etf_static_cache.csv'
STATIC_CACHE_TTL = 1 * 60 * 60  # 4 hours in seconds

def load_static_cache():
    """Load static data from cache file"""
    try:
        if not os.path.exists(STATIC_CACHE_FILE_DATA):
            logger.info("📂 No static cache found")
            return {}
        
        df = pd.read_csv(STATIC_CACHE_FILE_DATA)
        
        if 'timestamp' in df.columns and len(df) > 0:
            cache_time = datetime.fromisoformat(df['timestamp'].iloc[0])
            age_seconds = (datetime.now() - cache_time).total_seconds()
            
            if age_seconds > STATIC_CACHE_TTL:
                logger.info("⚠️ Static cache expired (>4 hours old)")
                return {}
        
        static_data = {}
        for _, row in df.iterrows():
            static_data[row['symbol']] = row.to_dict()
        
        logger.info(f"✅ Loaded {len(static_data)} ETFs from static cache")
        return static_data
    except Exception as e:
        logger.error(f"❌ Failed to load static cache: {e}")
        return {}

def save_static_cache(static_data_list):
    """Save static data to cache file"""
    try:
        timestamp = datetime.now().isoformat()
        for entry in static_data_list:
            entry['timestamp'] = timestamp
        
        df = pd.DataFrame(static_data_list)
        df.to_csv(STATIC_CACHE_FILE_DATA, index=False)
        logger.info(f"💾 Saved {len(static_data_list)} ETFs to static cache")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to save static cache: {e}")
        return False

def should_refresh_static_cache(static_cache):
    """Check if static cache needs refresh"""
    if not static_cache:
        return True
    
    try:
        df = pd.read_csv(STATIC_CACHE_FILE_DATA)
        if 'timestamp' in df.columns and len(df) > 0:
            cache_time = datetime.fromisoformat(df['timestamp'].iloc[0])
            age_seconds = (datetime.now() - cache_time).total_seconds()
            if age_seconds > STATIC_CACHE_TTL:
                return True
        return False
    except:
        return True
# ============================================================================
# ETF DATABASE - ALL 20 ETFs
# ============================================================================

ETF_DATABASE = {
    "gold_etfs": [
        {"symbol": "GOLDBEES", "name": "Nippon India Gold BeES", "type": "gold", "isin": "INF204KB17I5","gold_per_unit": 0.00869},
        {"symbol": "HDFCGOLD", "name": "HDFC Gold ETF", "type": "gold", "isin": "INF179KB1AK5","gold_per_unit": 0.0083},
        {"symbol": "SETFGOLD", "name": "SBI Gold ETF", "type": "gold", "isin": "INF200K01LS1", "gold_per_unit": 0.00855},
        {"symbol": "GOLDIETF", "name": "ICICI Prudential Gold ETF", "type": "gold", "isin": "INF109KC16G0", "gold_per_unit": 0.00866},
        {"symbol": "GOLD1", "name": "Kotak Gold ETF", "type": "gold", "isin": "INF174KA13E3", "gold_per_unit": 0.00836},
        {"symbol": "AXISGOLD", "name": "Axis Gold ETF", "type": "gold", "isin": "INF846K01EW8", "gold_per_unit": 0.00844},
        {"symbol": "GOLDSHARE", "name": "UTI Gold ETF", "type": "gold", "isin": "INF789FA1016", "gold_per_unit": 0.00836},
        {"symbol": "BSLGOLDETF", "name": "Aditya Birla Gold ETF", "type": "gold", "isin": "INF209KB11H1", "gold_per_unit": 0.00881},
        {"symbol": "TATAGOLD", "name": "Tata Gold ETF", "type": "gold", "isin": "INF277K01EN5", "gold_per_unit": 0.00096},
        {"symbol": "GOLD360", "name": "360 ONE Gold ETF", "type": "gold", "isin": "INF966L01019", "gold_per_unit": 0.00959},
        {"symbol": "GOLDCASE", "name": "Zerodha Gold ETF", "type": "gold", "isin": "INF174K01021", "gold_per_unit": 0.0016}
    ],
    "silver_etfs": [
        {"symbol": "SILVERBEES", "name": "Nippon India Silver ETF", "type": "silver", "isin": "INF204KB18I3", "silver_per_unit": 1},
        {"symbol": "SILVERIETF", "name": "ICICI Prudential Silver ETF", "type": "silver", "isin": "INF109KC17M6", "silver_per_unit": 1},
        {"symbol": "HDFCSILVER", "name": "HDFC Silver ETF", "type": "silver", "isin": "INF179KB1AL3", "silver_per_unit": 1},
        {"symbol": "SILVER1", "name": "Kotak Silver ETF", "type": "silver", "isin": "INF174KA14E1", "silver_per_unit": 1},
        {"symbol": "SBISILVER", "name": "SBI Silver ETF", "type": "silver", "isin": "INF200K01639", "silver_per_unit": 1},
        {"symbol": "SILVER", "name": "Aditya Birla Silver ETF", "type": "silver", "isin": "INF209KB12H9", "silver_per_unit": 1},
        {"symbol": "AXISILVER", "name": "Axis Silver ETF", "type": "silver", "isin": "INF846K01FW6", "silver_per_unit": 1},
        {"symbol": "TATSILV", "name": "Tata Silver ETF", "type": "silver", "isin": "INF277K01FN2", "silver_per_unit": 0.1},
        {"symbol": "SILVERCASE", "name": "Zerodha Silver ETF", "type": "silver", "isin": "INF174K01039", "silver_per_unit": 0.1}
    ]
}

# Flatten for easy access
ETF_LIST = ETF_DATABASE['gold_etfs'] + ETF_DATABASE['silver_etfs']

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def safe_float(value, default=0.0):
    """Safely convert value to float"""
    try:
        if value is None or value == '':
            return default
        cleaned = re.sub(r'[^0-9.-]', '', str(value).strip())
        return float(cleaned) if cleaned else default
    except:
        return default

def safe_int(value, default=0):
    """Safely convert value to int"""
    try:
        if value is None or value == '':
            return default
        cleaned = re.sub(r'[^0-9.-]', '', str(value).strip())
        return int(float(cleaned)) if cleaned else default
    except:
        return default

# ============================================================================
# AMC WEBSITE iNAV SCRAPERS
# ============================================================================

def scrape_360one_inav(symbol):
    """Scrape from 360 ONE archive site"""
    try:
        if symbol == "GOLD360":
            url = "https://archive.iiflmf.com/our-funds/etf/360-one-gold-etf"
        else:
            url = "https://archive.iiflmf.com/our-funds/etf/360-one-silver-etf"

        response = requests.get(url, timeout=10)
        response.raise_for_status()

        match = re.search(r'iNAV.*?₹.*?\-\s*([0-9]{2,3}\.[0-9]{2})', response.text, re.IGNORECASE)
        if match:
            inav = safe_float(match.group(1))
            logger.info(f"🌐 360ONE {symbol}: iNAV = ₹{inav}")
            return inav

        logger.warning(f"⚠️ 360ONE {symbol}: iNAV pattern not found")
        return 0.0
    except Exception as e:
        logger.error(f"❌ 360ONE {symbol} scraping failed: {str(e)}")
        return 0.0

def scrape_sbi_inav(driver, symbol):
    """Scrape from SBI ETF Portal"""
    try:
        logger.info(f"🏦 SBI: Attempting to scrape {symbol}...")
        driver.get("https://etf.sbimf.com/Home/inav")
        time.sleep(4)

        search_text = "Gold ETF" if symbol == "SETFGOLD" else "Silver ETF"

        try:
            rows = driver.find_elements(By.CSS_SELECTOR, "#navTable tr")
            for row in rows:
                if search_text.lower() in row.text.lower():
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 2:
                        inav_text = cells[1].text.strip()
                        inav = safe_float(inav_text)
                        if inav > 0:
                            logger.info(f"🏦 SBI {symbol}: iNAV = ₹{inav}")
                            return inav
        except Exception as e:
            logger.warning(f"⚠️ SBI {symbol} failed: {str(e)}")

        return 0.0
    except Exception as e:
        logger.error(f"❌ SBI {symbol} scraping failed: {str(e)}")
        return 0.0

def scrape_uti_inav(driver, symbol):
    """Scrape from UTI MF nav-dividend page"""
    try:
        logger.info(f"🏛️ UTI: Attempting to scrape {symbol}...")
        driver.get("https://www.utimf.com/mutual-funds/nav-dividend")
        time.sleep(5)

        try:
            # Direct ID lookup
            gold_etf_cell = driver.find_element(By.ID, "myDiv9")
            row = gold_etf_cell.find_element(By.XPATH, "..")
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) >= 2:
                inav_text = cells[1].text.strip()
                inav_text = inav_text.replace('₹', '').replace(',', '').strip()
                inav = safe_float(inav_text)
                if inav > 0:
                    logger.info(f"🏛️ UTI {symbol}: iNAV = ₹{inav}")
                    return inav
        except Exception as e:
            logger.warning(f"⚠️ UTI {symbol}: Direct ID lookup failed, trying fallback...")

            # Fallback method
            rows = driver.find_elements(By.TAG_NAME, "tr")
            for row in rows:
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 2:
                        name_cell = cells[0].text.strip().lower()
                        if "gold exchange traded fund" in name_cell or "gold etf" in name_cell:
                            inav_text = cells[1].text.strip()
                            inav_text = inav_text.replace('₹', '').replace(',', '').strip()
                            inav = safe_float(inav_text)
                            if inav > 0:
                                logger.info(f"🏛️ UTI {symbol}: iNAV = ₹{inav} (fallback)")
                                return inav
                except:
                    continue

        logger.warning(f"⚠️ UTI {symbol}: Could not find iNAV")
        return 0.0
    except Exception as e:
        logger.error(f"❌ UTI {symbol} scraping failed: {str(e)}")
        return 0.0

def scrape_hdfc_inav(driver, symbol):
    """Scrape from HDFC MF website"""
    try:
        logger.info(f"🏦 HDFC: Attempting to scrape {symbol}...")
        driver.get("https://www.hdfcfund.com/explore/mutual-funds/hdfc-silver-etf/regular")

        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "p.style_description__kIUXb"))
            )
            time.sleep(3)
        except TimeoutException:
            logger.warning(f"⚠️ HDFC {symbol}: Timeout waiting for iNAV element")
            return 0.0

        try:
            inav_elements = driver.find_elements(By.CSS_SELECTOR, "p.style_description__kIUXb")
            for elem in inav_elements:
                text = elem.text.strip()
                if '₹' in text:
                    inav_text = text.replace('₹', '').replace(',', '').strip()
                    inav = safe_float(inav_text)
                    if 100 < inav < 200:
                        logger.info(f"🏦 HDFC {symbol}: iNAV = ₹{inav}")
                        return inav
        except Exception as e:
            logger.warning(f"⚠️ HDFC {symbol}: CSS selector failed - {str(e)}")

        # Fallback: Regex search
        try:
            page_source = driver.page_source
            match = re.search(r'iNAV.*?₹\s*([0-9]{2,3}\.[0-9]{2})', page_source, re.IGNORECASE | re.DOTALL)
            if match:
                inav = safe_float(match.group(1))
                if 100 < inav < 200:
                    logger.info(f"🏦 HDFC {symbol}: iNAV = ₹{inav} (regex)")
                    return inav
        except Exception as e:
            logger.warning(f"⚠️ HDFC {symbol}: Regex search failed - {str(e)}")

        logger.warning(f"⚠️ HDFC {symbol}: Could not find iNAV")
        return 0.0
    except Exception as e:
        logger.error(f"❌ HDFC {symbol} scraping failed: {str(e)}")
        return 0.0

def scrape_etfjunction_inav(driver, symbol):
    """Scrape from ETF Junction DataTable"""
    try:
        logger.info(f"📊 ETFJunction: Attempting to scrape {symbol}...")
        driver.get("https://etfjunction.com/inav.php")

        try:
            WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.CLASS_NAME, "etftable_ab")))
            table = driver.find_element(By.CLASS_NAME, "etftable_ab")
            logger.info(f"📊 ETFJunction {symbol}: Table loaded successfully")
        except TimeoutException:
            logger.warning(f"⚠️ ETFJunction {symbol}: Table failed to load within 15s")
            return 0.0

        time.sleep(2)

        try:
            table = driver.find_element(By.CLASS_NAME, "etftable_ab")
            rows = table.find_elements(By.TAG_NAME, "tr")
            logger.info(f"📊 ETFJunction {symbol}: Found {len(rows)} rows in table")

            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 4:
                    exchange_symbol = cells[1].text.strip()
                    if exchange_symbol == symbol:
                        inav_text = cells[3].text.strip()
                        inav = safe_float(inav_text)
                        if inav > 0:
                            logger.info(f"📊 ETFJunction {symbol}: iNAV = ₹{inav}")
                            return inav

            logger.warning(f"⚠️ ETFJunction {symbol}: Symbol not found in {len(rows)} rows")
            return 0.0
        except Exception as e:
            logger.warning(f"⚠️ ETFJunction {symbol}: Table parsing failed - {str(e)}")
            return 0.0
    except Exception as e:
        logger.error(f"❌ ETFJunction {symbol} scraping failed: {str(e)}")
        return 0.0

# ============================================================================
# CHROME DRIVER SETUP
# ============================================================================

def create_optimized_driver():
    """
    Create headless Chrome driver with optimized settings
    Optimized for Intel i3-6006U (dual-core) with 8GB RAM
    """
    chrome_options = Options()

    # ═══════════════════════════════════════════════════════════════
    # EXISTING FLAGS (KEEP THESE)
    # ═══════════════════════════════════════════════════════════════
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-software-rasterizer')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--log-level=3')
    chrome_options.add_argument('--silent')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

    # ═══════════════════════════════════════════════════════════════
    # NEW MEMORY-SAVING FLAGS (ADD THESE)
    # ═══════════════════════════════════════════════════════════════
    # Disable unnecessary background processes
    chrome_options.add_argument('--disable-background-networking')
    chrome_options.add_argument('--disable-default-apps')
    chrome_options.add_argument('--disable-sync')
    chrome_options.add_argument('--disable-background-timer-throttling')
    chrome_options.add_argument('--disable-backgrounding-occluded-windows')
    chrome_options.add_argument('--disable-renderer-backgrounding')

    # Disable media & audio (not needed for scraping)
    chrome_options.add_argument('--mute-audio')

    # Disable DevTools overhead
    chrome_options.add_argument('--disable-dev-tools')

    # Reduce telemetry
    chrome_options.add_argument('--metrics-recording-only')
    chrome_options.add_argument('--no-first-run')

    # ⭐ CRITICAL: Memory pressure management for 8GB RAM
    chrome_options.add_argument('--memory-pressure-off')

    # ⭐ CRITICAL: Limit V8 JavaScript heap size (prevents OOM)
    chrome_options.add_argument('--max-old-space-size=512')  # 512 MB limit

    # Additional GPU optimizations
    chrome_options.add_argument('--disable-accelerated-2d-canvas')
    chrome_options.add_argument('--disable-webgl')

    # ═══════════════════════════════════════════════════════════════
    # PREFERENCES (UPDATED)
    # ═══════════════════════════════════════════════════════════════
    prefs = {
        'profile.default_content_setting_values': {
            'images': 2,        # Block images (already there)
            'javascript': 1,    # Allow JavaScript (needed for dynamic content)
            'notifications': 2, # Block notifications ⭐ NEW
            'media_stream': 2,  # Block media streams (camera/mic) ⭐ NEW
        }
    }
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.page_load_strategy = 'eager'  # Already there, keep it!

    # ═══════════════════════════════════════════════════════════════
    # CREATE DRIVER (SAME AS BEFORE)
    # ═══════════════════════════════════════════════════════════════
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(15)
    driver.implicitly_wait(2)

    return driver

# ============================================================================
# CORE SCRAPING FUNCTIONS
# ============================================================================

#original
def scrape_etf_fast(driver, symbol, isin="", retry_count=0):
    """
    Scrape single ETF from NSE with AMC fallback
    """
    max_retries = 2

    try:
        url = f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}"
        driver.get(url)

        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, 'quoteLtp')))
            time.sleep(2)
        except TimeoutException:
            if retry_count < max_retries:
                time.sleep(2)
                return scrape_etf_fast(driver, symbol, isin, retry_count + 1)
            return None

        # Extract price
        price = safe_float(driver.find_element(By.ID, 'quoteLtp').text)
        if price <= 0:
            if retry_count < max_retries:
                time.sleep(2)
                return scrape_etf_fast(driver, symbol, isin, retry_count + 1)
            return None

        # Try NSE iNAV
        inav = 0.0
        try:
            inav = safe_float(driver.find_element(By.ID, 'iNavValue').text)
        except:
            pass

        # Extract other NSE data
        logger.info(f"🔍 {symbol}: Extracting NSE fields... price={price}, inav={inav}")

        prev_close = 0.0
        try:
            elem = driver.find_element(By.ID, 'stockPreviousClose')
            prev_close = safe_float(elem.text)
        except Exception as e:
            logger.warning(f"⚠️ {symbol}: prev_close failed - {str(e)}")

        open_price = 0.0
        try:
            elem = driver.find_element(By.ID, 'stockOpenPrice')
            open_price = safe_float(elem.text)
        except:
            pass

        day_high = 0.0
        try:
            day_high = safe_float(driver.find_element(By.ID, 'stockHigh').text)
        except:
            pass

        day_low = 0.0
        try:
            day_low = safe_float(driver.find_element(By.ID, 'stockLow').text)
        except:
            pass

        volume = 0
        try:
            elem = driver.find_element(By.ID, 'orderBookTradeVol')
            vol_lakhs = safe_float(elem.text)
            volume = int(vol_lakhs * 100000) if vol_lakhs > 0 else 0
        except:
            pass

        week52_high = 0.0
        try:
            week52_high = safe_float(driver.find_element(By.ID, 'week52highVal').text)
        except:
            pass

        week52_high_date = ""
        try:
            week52_high_date = driver.find_element(By.ID, 'week52HighDate').text.strip().replace('(', '').replace(')', '')
        except:
            pass

        delivery_percent = 0.0
        try:
            delivery_text = driver.find_element(By.ID, 'orderBookDeliveryTradedQty').text.strip()
            delivery_percent = safe_float(delivery_text.replace('%', ''))
        except:
            pass

        # AMC Fallback (if iNAV not found on NSE)
        if inav == 0:
            try:
                logger.info(f"🌐 {symbol}: iNAV not on NSE, trying AMC website...")

                if symbol == "GOLD360":
                    inav = scrape_360one_inav(symbol)
                elif symbol in ["BSLGOLDETF", "SILVER"]:
                    etfj_driver = create_optimized_driver()
                    try:
                        inav = scrape_etfjunction_inav(etfj_driver, symbol)
                    finally:
                        etfj_driver.quit()
                else:
                    amc_driver = create_optimized_driver()
                    try:
                        if symbol == "SETFGOLD":
                            inav = scrape_sbi_inav(amc_driver, symbol)
                        elif symbol == "SBISILVER":
                            inav = scrape_sbi_inav(amc_driver, symbol)
                        elif symbol == "GOLDSHARE":
                            inav = scrape_uti_inav(amc_driver, symbol)
                        elif symbol == "HDFCSILVER":
                            inav = scrape_hdfc_inav(amc_driver, symbol)
                    finally:
                        amc_driver.quit()

                if inav > 0:
                    logger.info(f"✅ {symbol}: Got iNAV from AMC = ₹{inav}")
            except Exception as amc_error:
                logger.warning(f"⚠️ {symbol}: AMC scraping failed: {str(amc_error)}")
                inav = 0.0

        # Calculate metrics
        change = price - prev_close if prev_close > 0 else 0.0
        change_percent = (change / prev_close * 100) if prev_close > 0 else 0.0
        discount = ((price - inav) / inav * 100) if inav > 0 else 0.0        
        result = {
            'symbol': symbol,
            'isin': isin,
            'price': round(price, 2),
            'inav': round(inav, 2),
            'discount': round(discount, 2),
            'change': round(change, 2),
            'changePercent': round(change_percent, 2),
            'open': round(open_price, 2),
            'dayHigh': round(day_high, 2),
            'dayLow': round(day_low, 2),
            'week52High': round(week52_high, 2),
            'week52HighDate': week52_high_date,
            'prevClose': round(prev_close, 2),
            'deliveryPercent': round(delivery_percent, 2),
            'vwap': round(price, 2),
            'volume': volume,
            'lastUpdate': datetime.now().isoformat(),
            'status': 'live',
            'dataAge': 'live'
        }
        logger.info(f"✅ {symbol}: ₹{price:.2f} | iNAV: ₹{inav:.2f} | Discount: {discount:.2f}%")
        return result

    except Exception as e:
        if retry_count < max_retries:
            time.sleep(2)
            return scrape_etf_fast(driver, symbol, isin, retry_count + 1)
        else:
            logger.error(f"❌ {symbol}: Failed - {str(e)}")
            return None

def scrape_static_fields(driver, symbol, isin=""):
    """Scrape STATIC fields only: prevClose, open, week52High, week52HighDate, vwap"""
    try:
        url = f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}"
        driver.get(url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, 'quoteLtp')))
        time.sleep(2)
        logger.error(f"✅ {symbol}: Static scraping done")
        return {
            'symbol': symbol,
            'prevClose': round(safe_float(driver.find_element(By.ID, 'stockPreviousClose').text), 2),
            'open': round(safe_float(driver.find_element(By.ID, 'stockOpenPrice').text), 2),
            'week52High': round(safe_float(driver.find_element(By.ID, 'week52highVal').text), 2),
            'week52HighDate': driver.find_element(By.ID, 'week52HighDate').text.strip().replace('(', '').replace(')', ''),
            'vwap': round(safe_float(driver.find_element(By.ID, 'quoteLtp').text), 2)
        }
        
    except Exception as e:
        logger.error(f"❌ {symbol}: Static scraping failed")
        return None

def scrape_dynamic_fields(driver, symbol, isin="", retry_count=0):
    """Scrape DYNAMIC fields: price, dayHigh, dayLow, deliveryPercent, inav, volume"""
    max_retries = 2
    try:
        url = f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}"
        driver.get(url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, 'quoteLtp')))
        time.sleep(2)
        
        price = safe_float(driver.find_element(By.ID, 'quoteLtp').text)
        if price <= 0:
            if retry_count < max_retries:
                return scrape_dynamic_fields(driver, symbol, isin, retry_count + 1)
            return None
        
        # NSE iNAV
        inav = 0.0
        try:
            inav = safe_float(driver.find_element(By.ID, 'iNavValue').text)
        except:
            pass
        
        # AMC Fallback (COPY YOUR EXISTING AMC LOGIC HERE)
        if inav == 0:
            try:
                logger.info(f"🌐 {symbol}: Trying AMC for iNAV...")
                if symbol == "GOLD360":
                    inav = scrape_360one_inav(symbol)
                elif symbol in ["BSLGOLDETF", "SILVER"]:
                    etfj_driver = create_optimized_driver()
                    try:
                        inav = scrape_etfjunction_inav(etfj_driver, symbol)
                    finally:
                        etfj_driver.quit()
                # Add other AMC logic here...
                else:  # ← ADD THIS BLOCK!
                    amc_driver = create_optimized_driver()
                    try:
                        if symbol == "SETFGOLD":
                            inav = scrape_sbi_inav(amc_driver, symbol)
                        elif symbol == "SBISILVER":
                            inav = scrape_sbi_inav(amc_driver, symbol)
                        elif symbol == "GOLDSHARE":  # ← FIXES GOLDSHARE!
                            inav = scrape_uti_inav(amc_driver, symbol)
                        elif symbol == "HDFCSILVER":  # ← FIXES HDFCSILVER!
                            inav = scrape_hdfc_inav(amc_driver, symbol)
                    finally:
                        amc_driver.quit()
                    
                if inav > 0:
                    logger.info(f"✅ {symbol}: Got iNAV from AMC = ₹{inav}")
            
            except Exception as amc_error:
                logger.warning(f"⚠️ {symbol}: AMC scraping failed: {str(amc_error)}")
                inav = 0.0
        
        return {
            'symbol': symbol,
            'price': round(price, 2),
            'dayHigh': round(safe_float(driver.find_element(By.ID, 'stockHigh').text), 2),
            'dayLow': round(safe_float(driver.find_element(By.ID, 'stockLow').text), 2),
            'deliveryPercent': round(safe_float(driver.find_element(By.ID, 'orderBookDeliveryTradedQty').text.replace('%', '')), 2),
            'inav': round(inav, 2),
            'volume': int(safe_float(driver.find_element(By.ID, 'orderBookTradeVol').text) * 100000)
        }
    except Exception as e:
        if retry_count < max_retries:
            return scrape_dynamic_fields(driver, symbol, isin, retry_count + 1)
        return None

def scrape_etf_batch_parallel(etf_batch, batch_id):
    """Process a batch of ETFs with a single driver"""
    driver = None
    results = []

    try:
        driver = create_optimized_driver()
        logger.info(f"🚀 Batch-{batch_id}: Processing {len(etf_batch)} ETFs")

        for etf in etf_batch:
            nse_data = scrape_etf_fast(driver, etf['symbol'], etf.get('isin', ''))

            if nse_data and nse_data.get('price', 0) > 0:
                etf_data = {**etf, **nse_data}
            else:
                etf_data = {
                    **etf,
                    'status': 'error',
                    'dataAge': 'error',
                    'lastUpdate': datetime.now().isoformat()
                }

            results.append(etf_data)
            time.sleep(0.5)

        return results

    except Exception as e:
        logger.error(f"❌ Batch-{batch_id}: {str(e)}")
        return []

    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

def calculate_mcx_fields(etf, mcx_gold, mcx_silver):
    """Calculate effective price per gram and IBJA comparison (FIXED)"""
    
    # Determine metal type
    etf_type = etf.get('type', '')
    metal_per_unit = float(etf.get('gold_per_unit', 0)) if etf_type == 'gold' else float(etf.get('silver_per_unit', 0))
    price = float(etf.get('price', 0))
    
    # Get correct IBJA spot price
    ibja_spot = mcx_gold if etf_type == 'gold' else mcx_silver
    if (ibja_spot>0):
        # Calculate effective price per gram
        #logger.info(f"{etf.get('symbol', '')} : {metal_per_unit} ")
        effective_price = (price / metal_per_unit) if metal_per_unit > 0 else 0
        
        # Calculate premium/discount vs IBJA
        vs_ibja = ((effective_price - ibja_spot) / ibja_spot) * 100
        #logger.info(f"{etf.get('symbol', '')} : {vs_ibja} ")
        
        # Add to ETF dict
        etf['effective_price_per_gram'] = round(effective_price, 2)
        etf['mcx_spot_per_gram'] = round(ibja_spot, 2)
        etf['discount_vs_mcx'] = round(vs_ibja, 2)
    else:
        effective_price=0
        vs_ibja=0
        etf['effective_price_per_gram'] = round(effective_price, 2)
        etf['mcx_spot_per_gram'] = round(ibja_spot, 2)
        etf['discount_vs_mcx'] = round(vs_ibja, 2)
    #logger.info(f"🔍 {etf.get('symbol', '')}: SAVED effective={etf['effective_price_per_gram']}, vs_ibja={etf['discount_vs_mcx']}")
    return etf

def scrape_all_etfs_parallel():
    """OPTIMIZED TWO-TIER scraping with IBJA integration"""
    logger.info("⚡ Starting OPTIMIZED scraping with IBJA integration...")
    start_time = time.time()
    
    # Get IBJA prices
    mcx_prices = get_mcx_spot_prices()
    if mcx_prices is None:
        logger.error("❌ MCX prices is None, using fallback")
        mcx_prices = {
            'gold_per_gram': 0.0,
            'silver_per_gram': 0.0,
            'timestamp': datetime.now().isoformat(),
            'source': 'ERROR'
        }
    mcx_gold = mcx_prices.get('gold_per_gram', 0.0)
    mcx_silver = mcx_prices.get('silver_per_gram', 0.0)
    logger.info(f"💰 {mcx_prices.get('source')}: Gold=₹{mcx_gold:.2f}/g, Silver=₹{mcx_silver:.2f}/g")
    
    # Load/check static cache
    static_cache = load_static_cache()
    needs_static_refresh = should_refresh_static_cache(static_cache)
    
    # Scrape static if needed (SLOW, every 4 hours)
    if needs_static_refresh:
        logger.info("🏦 Scraping STATIC fields...")
        driver = create_optimized_driver()
        static_data_list = []
        try:
            for etf in ETF_LIST:
                static = scrape_static_fields(driver, etf['symbol'], etf.get('isin', ''))
                if static:
                    static_data_list.append(static)
                time.sleep(1)
            if static_data_list:
                save_static_cache(static_data_list)
                static_cache = {s['symbol']: s for s in static_data_list}
        finally:
            driver.quit()
    
    # Scrape dynamic (FAST, every call)
    logger.info("⚡ Scraping DYNAMIC fields...")
    # Scrape dynamic (PARALLEL, FAST!)
    logger.info("⚡ Scraping DYNAMIC fields in PARALLEL...")
    BATCH_SIZE = 5  # Process 5 ETFs concurrently
    MAX_WORKERS = 4  # Use 4 parallel threads

    batches = [ETF_LIST[i:i + BATCH_SIZE] for i in range(0, len(ETF_LIST), BATCH_SIZE)]
    all_results = []
    success_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_batch = {
            executor.submit(scrape_etf_batch_parallel, batch, idx): idx
            for idx, batch in enumerate(batches, 1)
        }
        
        for future in as_completed(future_to_batch):
            batch_results = future.result()
            for etf_data in batch_results:
                static = static_cache.get(etf_data['symbol'], {})
                
                if etf_data.get('price', 0) > 0:
                    price = etf_data.get('price', 0)
                    prev_close = safe_float(static.get('prevClose', 0))
                    inav = etf_data.get('inav', 0)
                    
                    change = price - prev_close if prev_close > 0 else 0.0
                    change_percent = (change / prev_close * 100) if prev_close > 0 else 0.0
                    discount = ((price - inav) / inav * 100) if inav > 0 else 0.0
                    
                    combined = {
                        **etf_data,
                        **static,
                        'change': round(change, 2),
                        'changePercent': round(change_percent, 2),
                        'discount': round(discount, 2),
                        'gold_per_gram': mcx_gold,
                        'silver_per_gram': mcx_silver,
                        'status': 'live',
                        'lastUpdate': datetime.now().isoformat(),
                        'dataAge': 'live'
                    }
                    all_results.append(combined)
                    success_count += 1

    
    # Calculate IBJA fields
    for etf in all_results:
        calculate_mcx_fields(etf, mcx_gold, mcx_silver)
    
    gold_etfs = [e for e in all_results if e['type'] == 'gold']
    silver_etfs = [e for e in all_results if e['type'] == 'silver']
    
    results = {
        'gold_etfs': gold_etfs,
        'silver_etfs': silver_etfs,
        'mcx_spot_prices': mcx_prices,
        'timestamp': datetime.now().isoformat(),
        'success_count': success_count,
        'total_count': len(ETF_LIST)
    }
    
    total_time = time.time() - start_time
    logger.info(f"🎉 Scraping complete: {success_count}/{len(ETF_LIST)} in {total_time:.1f}s")
    return results

# ============================================================================
# TEST FUNCTION (Optional)
# ============================================================================

if __name__ == "__main__":
    print("🧪 Testing ETF Scraper...")
    print("="*60)
    
    # ✅ Run scraper (internally calls get_mcx_spot_prices() once)
    results = scrape_all_etfs_parallel()
    
    # Extract MCX prices from results
    mcx_prices = results.get('mcx_spot_prices', {})
    print(f"\nMCX/IBJA Prices:")
    print(f"  Gold: {mcx_prices.get('gold_per_gram', 0):.2f}/gram")
    print(f"  Silver: {mcx_prices.get('silver_per_gram', 0):.2f}/gram")
    print(f"  Source: {mcx_prices.get('source', 'Unknown')}")
    
    # Print scraping results
    print(f"\nScraping Results:")
    print(f"  Scraped: {results['success_count']}/{results['total_count']} ETFs")
    print(f"  Gold ETFs: {len(results['gold_etfs'])}")
    print(f"  Silver ETFs: {len(results['silver_etfs'])}")
