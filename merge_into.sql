-- -- 1. Temporary staging table for new data
CREATE OR REPLACE TEMP VIEW src_incremental_trans AS
SELECT
  *
FROM `poc-kg-data_catalog`.bookstore.trans
WHERE updated_at >= concat(cast(date(to_timestamp(from_utc_timestamp(current_timestamp(), 'Asia/Jakarta'))- INTERVAL 2 DAYS) as string),' 00:00:00');

-- 2. Count new rows
SELECT count(1) as src_incremental_rows from src_incremental_trans;


-- 3. Merge into 
MERGE INTO trans AS t
USING src_incremental_trans AS s
ON t.trans_key = s.trans_key 
WHEN MATCHED AND s.updated_at > t.updated_at THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;
