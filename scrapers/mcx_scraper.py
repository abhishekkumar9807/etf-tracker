"""
MCX Gold & Silver Price Scraper - PRODUCTION VERSION
═══════════════════════════════════════════════════════════════════════════

STABLE SCRAPING LOGIC + TIME-BASED OPTIMIZATION
• Uses your proven IBJA scraping code (www.ibjarates.com)
• Uses your proven MCX scraping code  
• Adds intelligent time-window routing
• Includes cache preservation & age tracking

Time Windows:
• 7:00 AM - 12:30 PM: IBJA requests-only (skip Selenium)
• 12:30 PM - 12:40 PM: Cache-only (dead zone)
• 12:40 PM - 10:00 PM: IBJA requests → IBJA Selenium → MCX fallback
• 5:00 PM - 7:00 AM: MCX active (overnight trading)

═══════════════════════════════════════════════════════════════════════════
"""

import time
import json
import os
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import logging
import requests
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cache file
CACHE_FILE = 'mcx_cache.json'
CACHE_VALIDITY_HOURS = 2


# ============================================================================
# TIME WINDOW DETECTION FUNCTIONS
# ============================================================================

def is_ibja_requests_only_window():
    """Check if we're in IBJA requests-only window (7 AM - 12:30 PM)"""
    now = datetime.now()
    hour = now.hour
    minute = now.minute

    if (hour == 7 and minute >= 0) or (hour >= 8 and hour < 12) or (hour == 12 and minute < 30):
        return True
    return False


def is_ibja_active_window():
    """Check if IBJA is active (12:40 PM - 10:00 PM)"""
    now = datetime.now()
    hour = now.hour
    minute = now.minute
    if now.weekday() >= 5:  # 0=Monday, 4=Friday
        return False
    if (hour == 12 and minute >= 40) or (hour >= 13 and hour < 22):
        return True
    return False


def is_mcx_active_window():
    """Check if MCX Spot is active (5:00 PM - 7:00 AM next day)"""
    now = datetime.now()
    hour = now.hour
    if now.weekday() >= 5:
        return True
    if hour >= 17 or hour < 7:
        if now.weekday() < 5:  # 0=Monday, 4=Friday
            return True
    return False


def is_dead_zone():
    """Check if we're in the dead zone (12:30 PM - 12:40 PM)"""
    now = datetime.now()
    hour = now.hour
    minute = now.minute

    if hour == 12 and 30 <= minute < 40:
        return True
    return False


# ============================================================================
# CACHE FUNCTIONS
# ============================================================================

def calculate_cache_age(cache_timestamp):
    """Calculate how old the cache is in hours"""
    try:
        cache_time = datetime.fromisoformat(cache_timestamp)
        now = datetime.now()
        age = (now - cache_time).total_seconds() / 3600
        return round(age, 1)
    except:
        return None


def load_cache():
    """Load cached prices WITHOUT overwriting the file"""
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, 'r') as f:
                cache = json.load(f)

            cache_age = calculate_cache_age(cache.get('timestamp'))

            if cache_age is not None:
                cache['cache_age_hours'] = cache_age

                if cache_age < 1:
                    age_str = f"{int(cache_age * 60)} minutes"
                else:
                    age_str = f"{cache_age:.1f} hours"

                cache['note'] = f"⚠️ Using cached data ({age_str} old). Last updated: {cache.get('timestamp_display', 'unknown')}"

                if cache_age > 24:
                    cache['warning'] = f"🔴 Cache is {cache_age:.0f} hours old (>24 hours)!"
                elif cache_age > 12:
                    cache['warning'] = f"🟡 Cache is {cache_age:.1f} hours old"

            logger.info(f"📦 Loaded cache from {cache.get('timestamp_display', 'unknown')} (age: {cache_age:.1f}h)")
            return cache

    except Exception as e:
        logger.error(f"❌ Cache load failed: {str(e)}")

    logger.warning("⚠️ No valid cache found, returning zero fallback")
    return {
        'gold_per_gram': 0,
        'silver_per_gram': 0,
        'source': 'Fallback',
        'timestamp': datetime.now().isoformat(),
        'timestamp_display': datetime.now().strftime('%Y-%m-%d %I:%M %p IST'),
        'note': '⚠️ No data available (cache missing)',
        'cache_age_hours': None
    }

def is_cache_fresh():
    """Check if cache is valid (< 2 hours old)"""
    try:
        if not os.path.exists(CACHE_FILE):
            return False
        
        with open(CACHE_FILE, 'r') as f:
            cache = json.load(f)
        
        cache_age = calculate_cache_age(cache.get('timestamp'))
        
        if cache_age is None:
            return False
        
        is_fresh = cache_age < CACHE_VALIDITY_HOURS
        
        if is_fresh:
            logger.info(f"✅ Cache is FRESH ({cache_age:.1f}h old)")
        else:
            logger.info(f"⏰ Cache is STALE ({cache_age:.1f}h old)")
        
        return is_fresh
    
    except Exception as e:
        logger.error(f"❌ Cache freshness check failed: {str(e)}")
        return False

def save_cache(data):
    """Save FRESH data to cache (only called when scraping succeeds)"""
    try:
        data_to_save = data.copy()
        data_to_save.pop('note', None)
        data_to_save.pop('cache_age_hours', None)
        data_to_save.pop('warning', None)

        with open(CACHE_FILE, 'w') as f:
            json.dump(data_to_save, f, indent=2)

        logger.info(f"💾 Cache saved: {data.get('source', 'unknown')} at {data.get('timestamp_display', 'unknown')}")
    except Exception as e:
        logger.error(f"❌ Cache save failed: {str(e)}")


# ============================================================================
# IBJA SCRAPING FUNCTIONS (YOUR STABLE CODE)
# ============================================================================

def scrape_ibja_with_requests():
    """Scrape IBJA rates using requests (faster, primary method)"""
    try:
        logger.info("🔍 [IBJA Method 1] Trying requests + BeautifulSoup...")

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        }

        response = requests.get(
            "https://www.ibjarates.com/",
            headers=headers,
            timeout=15,
            verify=False
        )

        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        table = soup.find('table', {'id': 'TodayRatesTableDataYes'})
        if not table:
            logger.warning("⚠️ Could not find IBJA rates table")
            return None

        # Extract values
        def extract_value(span_id):
            try:
                span = soup.find('span', {'id': span_id})
                if span and span.text.strip():
                    return int(span.text.strip())
                return None
            except:
                return None

        gold_995_am = extract_value('lblGold995_AM')
        gold_995_pm = extract_value('lblGold995_PM')
        silver_999_am = extract_value('lblSilver999_AM')
        silver_999_pm = extract_value('lblSilver999_PM')

        if gold_995_am and silver_999_am:
            # Convert to per gram
            gold_per_gram = round(gold_995_am / 10, 2)
            silver_per_gram = round(silver_999_am / 1000, 2)

            rates = {
                'gold_per_gram': gold_per_gram,
                'silver_per_gram': silver_per_gram,
                'source': 'IBJA (Requests)',
                'timestamp': datetime.now().isoformat(),
                'timestamp_display': datetime.now().strftime('%Y-%m-%d %I:%M %p IST'),
                'cache_age_hours': 0
            }

            logger.info(f"✅ [IBJA Method 1] Success - Gold: ₹{gold_per_gram}/g, Silver: ₹{silver_per_gram}/g")
            return rates

        logger.warning("⚠️ [IBJA Method 1] No data found in table")
        return None

    except Exception as e:
        logger.warning(f"⚠️ [IBJA Method 1] Failed: {str(e)[:100]}")
        return None


def scrape_ibja_with_selenium():
    """Scrape IBJA rates using Selenium (fallback, more reliable)"""
    driver = None
    try:
        logger.info("🔍 [IBJA Method 2] Trying Selenium + Chrome...")

        # Setup Chrome options
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--ignore-ssl-errors')
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')

        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(30)
        driver.get("https://www.ibjarates.com/")

        # Wait for table to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "TodayRatesTableDataYes"))
        )

        # Extract values
        def extract_value(span_id):
            try:
                element = driver.find_element(By.ID, span_id)
                text = element.text.strip()
                if text:
                    return int(text)
                return None
            except:
                return None

        gold_995_am = extract_value('lblGold995_AM')
        gold_995_pm = extract_value('lblGold995_PM')
        silver_999_am = extract_value('lblSilver999_AM')
        silver_999_pm = extract_value('lblSilver999_PM')

        if gold_995_am and silver_999_am:
            # Convert to per gram
            gold_per_gram = round(gold_995_am / 10, 2)
            silver_per_gram = round(silver_999_am / 1000, 2)

            rates = {
                'gold_per_gram': gold_per_gram,
                'silver_per_gram': silver_per_gram,
                'source': 'IBJA (Selenium)',
                'timestamp': datetime.now().isoformat(),
                'timestamp_display': datetime.now().strftime('%Y-%m-%d %I:%M %p IST'),
                'cache_age_hours': 0
            }

            logger.info(f"✅ [IBJA Method 2] Success - Gold: ₹{gold_per_gram}/g, Silver: ₹{silver_per_gram}/g")
            return rates

        logger.warning("⚠️ [IBJA Method 2] No data found")
        return None

    except Exception as e:
        logger.error(f"❌ [IBJA Method 2] Failed: {str(e)[:100]}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass


# ============================================================================
# MCX SCRAPING FUNCTION (YOUR STABLE CODE)
# ============================================================================

def scrape_mcx_official():
    """
    Scrape MCX Spot from official MCX India website
    Available 24/7 including weekends!
    """
    driver = None
    try:
        logger.info("🏦 [MCX Spot] Scraping from MCX India official...")

        # ✅ FIX #3: Create driver FIRST
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        import time as time_module

        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        driver = webdriver.Chrome(options=options)  # ← MUST create here!
        driver.set_page_load_timeout(30)

        # ============ SCRAPE GOLD ============
        url_gold = "https://www.mcxindia.com/market-data/spot-market-price/gold"
        logger.info("🔍 Scraping MCX Gold...")
        driver.get(url_gold)
        
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        time_module.sleep(7)

        # ✅ FIX #1: Use soup_gold (not soup)
        soup_gold = BeautifulSoup(driver.page_source, 'html.parser')
        table_gold = soup_gold.find('table', {'id': 'tblSMP'})

        gold_per_gram = 0.0
        if not table_gold:
            # ✅ FIX #2: Use "Gold" string (not metal variable)
            logger.warning("⚠️ MCX Gold: Table #tblSMP not found")
        else:
            tbody = table_gold.find('tbody')
            if tbody:
                row = tbody.find('tr')
                if row:
                    cells = row.find_all('td')
                    logger.info(f"📊 Gold: Found {len(cells)} cells")
                    
                    if len(cells) >= 4:
                        # ✅ FIX #4: Use cells[3] for price (not cells[0])
                        spot_price_text = cells[3].get_text(strip=True)
                        price_raw = float(spot_price_text.replace(',', ''))
                        gold_per_gram = round(price_raw / 10, 2)
                        logger.info(f"✅ MCX Gold: ₹{gold_per_gram}/g")

        # ============ SCRAPE SILVER ============
        url_silver = "https://www.mcxindia.com/market-data/spot-market-price/silver"
        logger.info("🔍 Scraping MCX Silver...")
        driver.get(url_silver)
        
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        # ✅ FIX #5: Use time_module (not time)
        time_module.sleep(7)

        soup_silver = BeautifulSoup(driver.page_source, 'html.parser')
        table_silver = soup_silver.find('table', {'id': 'tblSMP'})

        silver_per_gram = 0.0
        if table_silver:
            tbody = table_silver.find('tbody')
            if tbody:
                row = tbody.find('tr')
                if row:
                    cells = row.find_all('td')
                    if len(cells) >= 4:
                        spot_price_text = cells[3].get_text(strip=True)
                        price_raw = float(spot_price_text.replace(',', ''))
                        silver_per_gram = round(price_raw / 1000, 2)
                        logger.info(f"✅ MCX Silver: ₹{silver_per_gram}/g")

        # ============ RETURN RESULTS ============
        if gold_per_gram > 0 and silver_per_gram > 0:
            return {
                'gold_per_gram': gold_per_gram,
                'silver_per_gram': silver_per_gram,
                'source': 'MCX_Spot',
                'timestamp': datetime.now().isoformat(),
                'timestamp_display': datetime.now().strftime('%Y-%m-%d %I:%M %p IST'),
                'cache_age_hours': 0
            }

        logger.warning("⚠️ MCX scraping returned zero values")
        return None

    except Exception as e:
        logger.error(f"❌ MCX scraping failed: {str(e)[:100]}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass


# ============================================================================
# MAIN FUNCTION WITH INTELLIGENT TIME-BASED ROUTING
# ============================================================================

def get_mcx_spot_prices():
    """
    MAIN FUNCTION: Intelligent scraping with time-based optimization

    Uses your proven stable code + adds smart time-window routing
    """
    logger.info(f"🕐 Current time: {datetime.now().strftime('%Y-%m-%d %I:%M %p IST')}")
    # ✅ NEW: Check cache FIRST (before any scraping!)
    if is_cache_fresh():
        logger.info("📦 Returning fresh cache (no scraping needed)")
        return load_cache()
    
    logger.info("🔄 Cache is stale or missing → Proceeding with scraping")

    # CASE 1: Dead zone (12:30 PM - 12:40 PM) → Load cache only
    if is_dead_zone():
        logger.info("⏸️  Dead zone detected (12:30-12:40 PM) → Loading cache")
        return load_cache()

    # CASE 2: IBJA requests-only window (7:00 AM - 12:30 PM)
    if is_ibja_requests_only_window():
        logger.info("🌅 IBJA requests-only window (7:00 AM - 12:30 PM)")

        result = scrape_ibja_with_requests()
        if result:
            save_cache(result)
            return result

        logger.info("⏩ IBJA requests failed → Loading cache (skipping Selenium to save time)")
        return load_cache()

    # CASE 3: IBJA active window (12:40 PM - 10:00 PM)
    if is_ibja_active_window():
        logger.info("☀️ IBJA active window (12:40 PM - 10:00 PM)")

        # Try requests first
        result = scrape_ibja_with_requests()
        if result:
            save_cache(result)
            return result

        # Requests failed → Try Selenium
        logger.info("🔄 IBJA requests failed → Trying Selenium")
        result = scrape_ibja_with_selenium()
        if result:
            save_cache(result)
            return result

        # Both IBJA methods failed → Try MCX if active (5 PM-10 PM overlap)
        if is_mcx_active_window():
            logger.info("🔄 IBJA Selenium failed → Trying MCX fallback (dual-coverage window)")
            result = scrape_mcx_official()
            if result:
                save_cache(result)
                return result

        # All failed → Load cache
        logger.info("⏩ All scraping methods failed → Loading cache")
        return load_cache()

    # CASE 4: MCX active window (5:00 PM - 7:00 AM next day)
    if is_mcx_active_window():
        logger.info("🌙 MCX active window (5:00 PM - 7:00 AM)")

        result = scrape_mcx_official()
        if result:
            save_cache(result)
            return result

        logger.info("⏩ MCX scraping failed → Loading cache")
        return load_cache()

    # CASE 5: All sources offline → Load cache
    logger.info("⏸️  All sources offline → Loading cache")
    return load_cache()


# Test function
if __name__ == '__main__':
    print("Testing PRODUCTION MCX scraper...")
    prices = get_mcx_spot_prices()
    print(f"\nResult:\n{json.dumps(prices, indent=2)}")
