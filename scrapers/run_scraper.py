#!/usr/bin/env python3
"""
GitHub Actions Scraper - Works with existing CSV+JSON format
No changes needed to your scrapers!
"""

import sys
import os
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def main():
    print("=" * 80)
    print("ETF + MCX Scraper for GitHub Actions")
    print("=" * 80)
    print()

    # Change to project root
    os.chdir(project_root)

    # Import scrapers (after changing directory)
    from scrapers.mcx_scraper import get_mcx_spot_prices
    from scrapers.etf_scraper_mcx import scrape_all_etfs_parallel

    # Create data directory
    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)

    # ========================================================================
    # STEP 1: Scrape MCX/IBJA Spot Prices
    # ========================================================================
    print("🏦 Scraping MCX/IBJA spot prices...")
    try:
        mcx_data = get_mcx_spot_prices()

        if mcx_data and mcx_data.get('gold_per_gram', 0) > 0:
            print(f"✅ Gold: ₹{mcx_data['gold_per_gram']:.2f}/g")
            print(f"✅ Silver: ₹{mcx_data['silver_per_gram']:.2f}/g")
            print(f"✅ Source: {mcx_data.get('source', 'Unknown')}")

            # Your scraper already saves mcx_cache.json in project root
            # Copy to data/ directory
            import shutil
            if Path('mcx_cache.json').exists():
                shutil.copy('mcx_cache.json', data_dir / 'mcx_cache.json')
                print(f"✅ Copied: mcx_cache.json → data/mcx_cache.json")
        else:
            print("⚠️ MCX scraping returned no data")

    except Exception as e:
        print(f"❌ MCX scraping failed: {e}")
        import traceback
        traceback.print_exc()

    print()

    # ========================================================================
    # STEP 2: Scrape ETF Data
    # ========================================================================
    print("📊 Scraping ETF data...")
    try:
        etf_results = scrape_all_etfs_parallel()

        if etf_results:
            gold_count = len(etf_results.get('gold_etfs', []))
            silver_count = len(etf_results.get('silver_etfs', []))

            print(f"✅ Scraped {gold_count} gold ETFs")
            print(f"✅ Scraped {silver_count} silver ETFs")

            # Your scraper saves to etf_cache.csv + etf_static_cache.csv in project root
            # Copy to data/ directory
            import shutil
            if Path('etf_cache.csv').exists():
                shutil.copy('etf_cache.csv', data_dir / 'etf_cache.csv')
                print(f"✅ Copied: etf_cache.csv → data/etf_cache.csv")

            if Path('etf_static_cache.csv').exists():
                shutil.copy('etf_static_cache.csv', data_dir / 'etf_static_cache.csv')
                print(f"✅ Copied: etf_static_cache.csv → data/etf_static_cache.csv")
        else:
            print("⚠️ ETF scraping returned no data")

    except Exception as e:
        print(f"❌ ETF scraping failed: {e}")
        import traceback
        traceback.print_exc()

    print()

    # ========================================================================
    # STEP 3: Update Timestamp
    # ========================================================================
    timestamp_file = data_dir / 'last_updated.txt'
    timestamp_file.write_text(datetime.now().strftime('%Y-%m-%d %H:%M:%S IST'))
    print(f"✅ Updated: data/last_updated.txt")

    print()
    print("=" * 80)
    print("✅ Scraping complete!")
    print("=" * 80)
    print()

    # Show what's in data/ directory
    print("📂 Files in data/ directory:")
    for file in sorted(data_dir.glob('*')):
        size = file.stat().st_size
        print(f"   {file.name} ({size:,} bytes)")

if __name__ == '__main__':
    main()
