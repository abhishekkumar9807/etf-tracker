import sys
import os
import csv
import json
import logging
import shutil
from pathlib import Path
from datetime import datetime

sys.path.append('.')
from scrapers.etf_scraper_mcx import scrape_all_etfs_sequential

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def save_etf_cache(results):
    """Save complete ETF data to etf_cache.csv"""
    try:
        gold_etfs = results.get('gold_etfs', [])
        silver_etfs = results.get('silver_etfs', [])
        all_etfs = gold_etfs + silver_etfs

        if not all_etfs:
            logger.warning("⚠️ No ETF data to save!")
            return

        os.makedirs('data', exist_ok=True)

        fieldnames = set()
        for etf in all_etfs:
            fieldnames.update(etf.keys())
        fieldnames = sorted(list(fieldnames))

        with open('data/etf_cache.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_etfs)

        logger.info(f"✅ Saved {len(all_etfs)} ETFs to data/etf_cache.csv")
        logger.info(f"   Gold: {len(gold_etfs)}, Silver: {len(silver_etfs)}")

    except Exception as e:
        logger.error(f"❌ Failed to save etf_cache.csv: {e}")

def save_mcx_cache(results):
    """Save MCX spot prices to mcx_cache.json"""
    try:
        mcx_prices = results.get('mcx_spot_prices', {})

        os.makedirs('data', exist_ok=True)

        with open('data/mcx_cache.json', 'w') as f:
            json.dump(mcx_prices, f, indent=2)

        logger.info(f"✅ Saved MCX prices to data/mcx_cache.json")
        logger.info(f"   Gold: ₹{mcx_prices.get('gold_per_gram', 0)}/g")
        logger.info(f"   Silver: ₹{mcx_prices.get('silver_per_gram', 0)}/g")

    except Exception as e:
        logger.error(f"❌ Failed to save mcx_cache.json: {e}")

'''def copy_static_cache():
    """Copy etf_static_cache.csv from project root to data/ folder"""
    try:
        src = Path('etf_static_cache.csv')
        dst = Path('data/etf_static_cache.csv')

        if src.exists():
            os.makedirs('data', exist_ok=True)
            shutil.copy(src, dst)
            logger.info(f"✅ Copied etf_static_cache.csv to data/")
        else:
            logger.warning("⚠️ etf_static_cache.csv not found in project root")
    except Exception as e:
        logger.error(f"❌ Failed to copy static cache: {e}")
'''
def update_last_updated():
    """Update last_updated.txt timestamp"""
    try:
        os.makedirs('data', exist_ok=True)
        '''with open('data/last_updated.txt', 'w') as f:
            f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S IST'))'''
        timestamp = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
        with open('data/last_updated.txt', 'w') as f:
            f.write(timestamp)
        logger.info(f"✓ Updated timestamp: {timestamp}")
    except Exception as e:
        logger.error(f"❌ Failed to update timestamp: {e}")

def main():
    logger.info("⏳ Starting ETF scraper...")
    results = scrape_all_etfs_sequential()

    # Save files that app_fastapi_mcx.py expects
    save_etf_cache(results)      # ← Creates data/etf_cache.csv
    save_mcx_cache(results)      # ← Creates data/mcx_cache.json

    # Copy internal files to data/ folder (for consistency)
    #copy_static_cache()          # ← Copies etf_static_cache.csv to data/
    update_last_updated()        # ← Updates last_updated.txt

    logger.info("✅ Done!")

if __name__ == "__main__":
    main()
