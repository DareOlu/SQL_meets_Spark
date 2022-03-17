-- SOLUTION
WITH a AS (
   SELECT player_id, device_id,
   ROW_NUMBER() OVER(            -- Alternatively, RANK()
       PARTITION BY player_id    -- Partitions the data according to player
       ORDER BY event_date) AS rw
   From Activity
)

SELECT player_id, device_id
FROM a
WHERE rk = 1
GO