import sys
import os
import csv
import json
import logging

sys.path.append('.')
from scrapers.etf_scraper_mcx import scrape_all_etfs_parallel

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def save_etf_cache(results):
    """Save complete ETF data to etf_cache.csv"""
    try:
        gold_etfs = results.get('gold_etfs', [])      # ✅ FIXED: snake_case
        silver_etfs = results.get('silver_etfs', [])  # ✅ FIXED: snake_case
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
        mcx_prices = results.get('mcx_spot_prices', {})  # ✅ FIXED: snake_case

        os.makedirs('data', exist_ok=True)

        with open('data/mcx_cache.json', 'w') as f:
            json.dump(mcx_prices, f, indent=2)

        logger.info(f"✅ Saved MCX prices to data/mcx_cache.json")
        logger.info(f"   Gold: ₹{mcx_prices.get('gold_per_gram', 0)}/g")
        logger.info(f"   Silver: ₹{mcx_prices.get('silver_per_gram', 0)}/g")

    except Exception as e:
        logger.error(f"❌ Failed to save mcx_cache.json: {e}")

def main():
    logger.info("⏳ Starting ETF scraper...")
    results = scrape_all_etfs_parallel()

    # etf_scraper_mcx.py already saved etf_static_cache.csv internally ✅
    # Now save the files that app_fastapi_mcx.py expects:
    save_etf_cache(results)   # ← Creates etf_cache.csv
    save_mcx_cache(results)   # ← Creates mcx_cache.json

    logger.info("✅ Done!")

if __name__ == "__main__":
    main()
