#!/usr/bin/env python3
"""Build dashboard-ready aggregates from the cleaned Amazon Beauty reviews Parquet."""
import json
import os
import duckdb

PARQUET = os.environ.get("PARQUET_PATH", "beauty_cleaned.parquet")
OUT = os.environ.get("OUT_DIR", "dashboard-data")
os.makedirs(OUT, exist_ok=True)

con = duckdb.connect()
con.execute("SET memory_limit='6GB'")
con.execute("SET threads=2")
safe_path = PARQUET.replace("'", "''")
con.execute(f"CREATE TEMP VIEW reviews AS SELECT * FROM read_parquet('{safe_path}')")

total_rows = con.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]
valid_rows = con.execute("SELECT COUNT(*) FROM reviews WHERE rating_valid BETWEEN 1 AND 5").fetchone()[0]
print(f"Rows: {total_rows:,}; valid ratings: {valid_rows:,}")

products = con.execute("""
WITH base AS (
 SELECT
  COALESCE(NULLIF(parent_asin,''), NULLIF(asin,'')) AS product_id,
  COALESCE(NULLIF(product_title_clean,''), NULLIF(product_title,''), 'Untitled product') AS product,
  COALESCE(NULLIF(store,''), 'Unknown brand') AS brand,
  price_numeric, rating_valid AS rating, helpful_vote_valid AS helpful_votes,
  verified_purchase_valid AS verified, review_time_utc,
  LOWER(COALESCE(review_title_clean,'') || ' ' || COALESCE(text_clean,'')) AS review_text
 FROM reviews
 WHERE rating_valid BETWEEN 1 AND 5
  AND COALESCE(NULLIF(parent_asin,''), NULLIF(asin,'')) IS NOT NULL
),
tagged AS (
 SELECT *,
 CASE
  WHEN regexp_matches(review_text,'smell|scent|odor|fragrance|perfume') THEN 'Scent / odor'
  WHEN regexp_matches(review_text,'leak|broken|packag|spill|damag|arriv') THEN 'Packaging / leak'
  WHEN regexp_matches(review_text,'price|expensive|worth|value|size|small|tiny') THEN 'Value / size'
  WHEN regexp_matches(review_text,'work|effective|result|doesn.t|ineffective') THEN 'Effectiveness'
  WHEN regexp_matches(review_text,'last|long.?lasting|wear|fade|durab') THEN 'Longevity / wear'
  WHEN regexp_matches(review_text,'itch|rash|burn|irritat|reaction|breakout') THEN 'Skin reaction'
  WHEN regexp_matches(review_text,'texture|feel|sticky|greasy|thick|watery') THEN 'Texture / feel'
  ELSE 'Other' END AS issue
 FROM base
)
SELECT product_id, any_value(product) AS product, any_value(brand) AS brand,
 count(*) AS review_count, avg(rating) AS avg_rating,
 count(*) FILTER(WHERE rating<=2)::DOUBLE/count(*) AS negative_rate,
 count(*) FILTER(WHERE verified IS TRUE)::DOUBLE/NULLIF(count(*) FILTER(WHERE verified IS NOT NULL),0) AS verified_rate,
 avg(price_numeric) FILTER(WHERE price_numeric>0 AND price_numeric<=1000) AS price,
 sum(COALESCE(helpful_votes,0)) AS helpful_votes,
 count(*) FILTER(WHERE rating<=2 AND issue<>'Other') AS tagged_negative_reviews,
 mode() WITHIN GROUP(ORDER BY issue) FILTER(WHERE rating<=2) AS main_issue,
 avg(rating) FILTER(WHERE year(CAST(review_time_utc AS TIMESTAMP))=2023) AS rating_2023,
 avg(rating) FILTER(WHERE year(CAST(review_time_utc AS TIMESTAMP))=2022) AS rating_2022
FROM tagged GROUP BY product_id ORDER BY review_count DESC
""").fetchdf()

def clean(records):
 out=[]
 for row in records:
  item={}
  for k,v in row.items():
   if hasattr(v,"item"): v=v.item()
   if v is None or (isinstance(v,float) and (v!=v or abs(v)==float("inf"))): v=None
   item[k]=v
  out.append(item)
 return out

product_records=clean(products.to_dict(orient="records"))
yearly=clean(con.execute("""
SELECT year(CAST(review_time_utc AS TIMESTAMP)) AS year,count(*) AS reviews,avg(rating_valid) AS avg_rating,
 count(*) FILTER(WHERE rating_valid<=2)::DOUBLE/count(*) AS negative_rate
FROM reviews WHERE rating_valid BETWEEN 1 AND 5 AND review_time_utc IS NOT NULL
GROUP BY 1 ORDER BY 1
""").fetchdf().to_dict(orient="records"))
rating_dist=clean(con.execute("""
SELECT rating_valid AS rating,count(*) AS reviews FROM reviews
WHERE rating_valid BETWEEN 1 AND 5 GROUP BY 1 ORDER BY 1 DESC
""").fetchdf().to_dict(orient="records"))
verified=clean(con.execute("""
SELECT verified_purchase_valid AS verified,count(*) AS reviews,avg(rating_valid) AS avg_rating,
 count(*) FILTER(WHERE rating_valid<=2)::DOUBLE/count(*) AS negative_rate
FROM reviews WHERE rating_valid BETWEEN 1 AND 5 AND verified_purchase_valid IS NOT NULL
GROUP BY 1 ORDER BY 1 DESC
""").fetchdf().to_dict(orient="records"))
issues=clean(con.execute("""
WITH n AS (
 SELECT LOWER(COALESCE(review_title_clean,'')||' '||COALESCE(text_clean,'')) AS t
 FROM reviews WHERE rating_valid BETWEEN 1 AND 2
), tagged AS (
 SELECT CASE
  WHEN regexp_matches(t,'smell|scent|odor|fragrance|perfume') THEN 'Scent / odor'
  WHEN regexp_matches(t,'leak|broken|packag|spill|damag|arriv') THEN 'Packaging / leak'
  WHEN regexp_matches(t,'price|expensive|worth|value|size|small|tiny') THEN 'Value / size'
  WHEN regexp_matches(t,'work|effective|result|doesn.t|ineffective') THEN 'Effectiveness'
  WHEN regexp_matches(t,'last|long.?lasting|wear|fade|durab') THEN 'Longevity / wear'
  WHEN regexp_matches(t,'itch|rash|burn|irritat|reaction|breakout') THEN 'Skin reaction'
  WHEN regexp_matches(t,'texture|feel|sticky|greasy|thick|watery') THEN 'Texture / feel'
  ELSE 'Other' END AS issue FROM n
)
SELECT issue,count(*) AS reviews FROM tagged GROUP BY issue ORDER BY reviews DESC
""").fetchdf().to_dict(orient="records"))
examples=clean(con.execute("""
SELECT product_id,product,brand,rating,review_title,review_text,helpful_votes,verified
FROM (
 SELECT COALESCE(NULLIF(parent_asin,''),NULLIF(asin,'')) AS product_id,
  COALESCE(NULLIF(product_title_clean,''),NULLIF(product_title,''),'Untitled product') AS product,
  COALESCE(NULLIF(store,''),'Unknown brand') AS brand,rating_valid AS rating,
  COALESCE(review_title_clean,review_title,'') AS review_title,
  LEFT(COALESCE(text_clean,text,''),700) AS review_text,
  COALESCE(helpful_vote_valid,0) AS helpful_votes,verified_purchase_valid AS verified,
  row_number() OVER(PARTITION BY COALESCE(NULLIF(parent_asin,''),NULLIF(asin,''))
   ORDER BY COALESCE(helpful_vote_valid,0) DESC,timestamp DESC) AS rn
 FROM reviews WHERE rating_valid<=2
) x WHERE rn<=3 ORDER BY helpful_votes DESC LIMIT 3000
""").fetchdf().to_dict(orient="records"))

payload={
 "metadata":{"source":"Amazon Reviews 2023 Beauty — cleaned Parquet",
  "total_review_rows":int(total_rows),"valid_rating_reviews":int(valid_rows),
  "product_count":len(product_records),"rating_definition":"rating_valid between 1 and 5",
  "negative_definition":"rating_valid <= 2",
  "note":"Product aggregates include all products represented in eligible reviews. Review excerpts are a capped evidence sample, not the full review corpus."},
 "products":product_records,"yearly":yearly,"rating_distribution":rating_dist,
 "verified_comparison":verified,"issues":issues,"review_examples":examples
}
path=os.path.join(OUT,"dashboard_data.json")
with open(path,"w",encoding="utf-8") as f: json.dump(payload,f,ensure_ascii=False,separators=(",",":"))
print(f"Wrote {path} ({os.path.getsize(path):,} bytes)")
print(f"Products included: {len(product_records):,}")
