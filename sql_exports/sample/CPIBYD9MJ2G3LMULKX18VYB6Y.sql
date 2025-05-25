WITH routine_CPIBYD9MJ2G3LMULKX18VYB6Y_01 AS (
  ------------------------------------------------------------------------------
  -- 1) Reads the source data from 2LIS_13_VDITM (simulating the internal table).
  --    This step corresponds to the ABAP routine reading the InfoSource content.
  ------------------------------------------------------------------------------
  SELECT
    ZC_TRTYPE,
    ZK_SALESQ
    -- Include all relevant fields needed from 2LIS_13_VDITM here
  FROM 2LIS_13_VDITM
),
routine_CPIBYD9MJ2G3LMULKX18VYB6Y AS (
  --------------------------------------------------------------------------------------
  -- 2) Implements the ABAP logic:
  --    - Clears RS by default (sets it to 0)
  --    - If ZC_TRTYPE = 'U1', then RS = ZK_SALESQ, otherwise 0
  --    - Sets RC = 0 and AB = 0 (ensuring no abort and no update block)
  --------------------------------------------------------------------------------------
  SELECT
    t.*,
    CASE WHEN t.ZC_TRTYPE = 'U1' THEN t.ZK_SALESQ ELSE 0 END AS RS,
    0 AS RC,
    0 AS AB
  FROM routine_CPIBYD9MJ2G3LMULKX18VYB6Y_01 t
)