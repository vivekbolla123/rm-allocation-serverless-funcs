-- Cleanup script for invalid audit records
-- Run this to remove malformed data before adding the unique constraint

USE RM_DW_DATA;

-- 1. Show invalid records (for review)
SELECT 'Invalid records to be deleted:' as info;
SELECT sector, flightNo, departureDate, changedAtDate, changedAtTime, colName, COUNT(*) as cnt
FROM audit_reports
WHERE sector = '' 
   OR flightNo = ''
   OR departureDate < 20000101
   OR LENGTH(departureDate) != 8
   OR changedAtDate < 20000101
   OR LENGTH(changedAtDate) != 8
   OR changedAtDate > departureDate
   OR changedAtTime < 10000
   OR changedAtTime > 19999
   OR colName = ''
GROUP BY sector, flightNo, departureDate, changedAtDate, changedAtTime, colName;

-- 2. Delete invalid records
DELETE FROM audit_reports
WHERE sector = '' 
   OR flightNo = ''
   OR departureDate < 20000101
   OR LENGTH(departureDate) != 8
   OR changedAtDate < 20000101
   OR LENGTH(changedAtDate) != 8
   OR changedAtDate > departureDate
   OR changedAtTime < 10000
   OR changedAtTime > 19999
   OR colName = '';

SELECT CONCAT('Deleted ', ROW_COUNT(), ' invalid records') as result;

-- 3. Check for exact duplicates (including time)
SELECT 'Exact duplicates (same time):' as info;
SELECT sector, flightNo, departureDate, source, changedAtDate, changedAtTime, colName, analystName, COUNT(*) as cnt
FROM audit_reports
GROUP BY sector, flightNo, departureDate, source, changedAtDate, changedAtTime, colName, analystName
HAVING cnt > 1
ORDER BY cnt DESC
LIMIT 20;

-- 4. Check for duplicates with different times
SELECT 'Duplicates with different times:' as info;
SELECT sector, flightNo, departureDate, source, changedAtDate, colName, analystName, 
       COUNT(*) as cnt, 
       GROUP_CONCAT(DISTINCT changedAtTime ORDER BY changedAtTime) as times
FROM audit_reports
GROUP BY sector, flightNo, departureDate, source, changedAtDate, colName, analystName
HAVING cnt > 1
ORDER BY cnt DESC
LIMIT 20;

-- 4a. Count total records to be deleted (not just groups)
SELECT 'Total records to be deleted (time variants):' as info;
SELECT COUNT(*) as total_to_delete
FROM audit_reports t1
INNER JOIN (
    SELECT sector, flightNo, departureDate, source, changedAtDate, colName, analystName,
           MIN(changedAtTime) as min_time
    FROM audit_reports
    GROUP BY sector, flightNo, departureDate, source, changedAtDate, colName, analystName
    HAVING COUNT(*) > 1
) t2 ON t1.sector = t2.sector
    AND t1.flightNo = t2.flightNo
    AND t1.departureDate = t2.departureDate
    AND t1.source = t2.source
    AND t1.changedAtDate = t2.changedAtDate
    AND t1.colName = t2.colName
    AND COALESCE(t1.analystName, '') = COALESCE(t2.analystName, '')
    AND t1.changedAtTime > t2.min_time;

-- 5. Remove exact duplicates (keeps lowest ID, same time)
DELETE t1 FROM audit_reports t1
INNER JOIN audit_reports t2 
WHERE t1.id > t2.id
  AND t1.sector = t2.sector
  AND t1.flightNo = t2.flightNo
  AND t1.departureDate = t2.departureDate
  AND t1.source = t2.source
  AND t1.changedAtDate = t2.changedAtDate
  AND t1.changedAtTime = t2.changedAtTime
  AND t1.colName = t2.colName
  AND COALESCE(t1.analystName, '') = COALESCE(t2.analystName, '');

SELECT CONCAT('Deleted ', ROW_COUNT(), ' exact duplicate records') as result;

-- 6. Remove duplicates with different times (keeps earliest time, lowest ID)
DELETE t1 FROM audit_reports t1
INNER JOIN (
    SELECT sector, flightNo, departureDate, source, changedAtDate, colName, analystName,
           MIN(changedAtTime) as min_time
    FROM audit_reports
    GROUP BY sector, flightNo, departureDate, source, changedAtDate, colName, analystName
    HAVING COUNT(*) > 1
) t2 ON t1.sector = t2.sector
    AND t1.flightNo = t2.flightNo
    AND t1.departureDate = t2.departureDate
    AND t1.source = t2.source
    AND t1.changedAtDate = t2.changedAtDate
    AND t1.colName = t2.colName
    AND COALESCE(t1.analystName, '') = COALESCE(t2.analystName, '')
    AND t1.changedAtTime > t2.min_time;

SELECT CONCAT('Deleted ', ROW_COUNT(), ' time-variant duplicate records') as result;

-- 5. Add unique constraint WITHOUT changedAtTime (if not exists)
-- This treats changes on same date as duplicates regardless of time
-- Note: Run this manually after verifying the cleanup
-- ALTER TABLE audit_reports
-- ADD UNIQUE KEY uk_audit_record (sector, flightNo, departureDate, source, 
--                                 changedAtDate, colName, analystName);

-- OR if you want to keep time in the constraint:
-- ALTER TABLE audit_reports
-- ADD UNIQUE KEY uk_audit_record (sector, flightNo, departureDate, source, 
--                                 changedAtDate, changedAtTime, colName, analystName);

SELECT 'Cleanup complete. Review results and manually add unique constraint if needed.' as final_message;
