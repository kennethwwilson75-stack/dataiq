import duckdb

conn = duckdb.connect()
conn.execute("CREATE TABLE trips AS SELECT * FROM read_parquet('data/raw/yellow_tripdata_2024_01.parquet')")

result = conn.execute("""
    SELECT 
        count(*) as rows,
        min(tpep_pickup_datetime) as earliest,
        max(tpep_pickup_datetime) as latest
    FROM trips
""").fetchone()

print(f"Rows:     {result[0]:,}")
print(f"Earliest: {result[1]}")
print(f"Latest:   {result[2]}")

cols = conn.execute("DESCRIBE trips").fetchall()
print(f"\nColumns ({len(cols)} total):")
for c in cols:
    print(f"  {c[0]}: {c[1]}")