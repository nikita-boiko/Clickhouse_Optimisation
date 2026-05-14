 CREATE TABLE logs_ch (
     timestamp DateTime,
     user_id UInt64,
     url String,
     response_time UInt32,
     status_code UInt16
 ) ENGINE = MergeTree()
 ORDER BY (user_id, timestamp);
 
SELECT 
	user_id, 
	count(*) 
FROM logs_ch 
WHERE timestamp > '2026-04-19'::date 
GROUP BY user_id 
 

DELETE FROM logs_ch WHERE user_id = 1;

SELECT * FROM system.mutations;

