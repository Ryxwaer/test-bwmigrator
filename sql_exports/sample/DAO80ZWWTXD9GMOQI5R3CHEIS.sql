WITH routine_DAO80ZWWTXD9GMOQI5R3CHEIS_input AS (
  -- This CTE reads all data from the 2LIS_13_VDITM InfoSource, 
  -- replicating the ABAP routine's initial read of the internal table "MONITOR".
  -- No transformation or filtering happens here.
  SELECT
    t.*
  FROM `2LIS_13_VDITM` t
),
routine_DAO80ZWWTXD9GMOQI5R3CHEIS_rs AS (
  -- This CTE implements the "only credit" logic from ABAP:
  --   IF /BIC/ZC_TRTYPE = 'U3' THEN RS = /BIC/ZK_SALES ELSE 0.
  -- We replicate this with a CASE expression, setting a new column RS accordingly.
  SELECT
    t.*,
    CASE 
      WHEN t."/BIC/ZC_TRTYPE" = 'U3' THEN t."/BIC/ZK_SALES" 
      ELSE 0 
    END AS RS
  FROM routine_DAO80ZWWTXD9GMOQI5R3CHEIS_input t
),
routine_DAO80ZWWTXD9GMOQI5R3CHEIS AS (
  -- This final CTE sets RC = 0 and AB = 0 for every row, 
  -- mirroring the ABAP logic where &RC& and &AB& are both cleared (set to 0).
  SELECT
    t.*,
    0 AS RC,
    0 AS AB
  FROM routine_DAO80ZWWTXD9GMOQI5R3CHEIS_rs t
)