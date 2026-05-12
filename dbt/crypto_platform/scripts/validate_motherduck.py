#!/usr/bin/env python3
"""
Validates MotherDuck schema and connection.
Usage: python scripts/validate_motherduck.py
"""
import os
import sys
import duckdb

def validate_motherduck():
    token = os.getenv('MOTHERDUCK_TOKEN')
    if not token:
        print("❌ MOTHERDUCK_TOKEN not set")
        sys.exit(1)

    try:
        conn = duckdb.connect(f"md:crypto_platform?motherduck_token={token}")
        
        # Test queries
        checks = [
            ("Bronze schema", "SELECT COUNT(*) FROM bronze_raw.raw_trades"),
            ("Silver schema", "SELECT COUNT(*) FROM silver_curated.ohlcv_1min"),
            ("Gold schema", "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'gold_analytics'"),
        ]
        
        print("🔍 MotherDuck Connection Validation\n")
        for name, query in checks:
            try:
                result = conn.execute(query).fetchone()
                print(f"✅ {name}: {result}")
            except Exception as e:
                print(f"⚠️  {name}: {e}")
        
        conn.close()
        print("\n✅ MotherDuck connection validated")
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    validate_motherduck()