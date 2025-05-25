WITH routine_4SACWQQQAQT6OPMBIXAI5KJNA_source AS (
  -- 1) Base step: This corresponds to reading the source data table (2LIS_13_VDITM) into our transformation flow.
  --    No filtering or transformations are done here; we simply collect the raw columns.
  SELECT
    *
  FROM `2LIS_13_VDITM`
),

routine_4SACWQQQAQT6OPMBIXAI5KJNA_debit AS (
  -- 2) Only Debit logic: In ABAP, we check if /BIC/ZC_TRTYPE = 'U1'. If so, we set /BIC/ZK_SALES into RS.
  --    This is implemented with a CASE to replicate the condition and assignment. If not 'U1', RS is 0.
  SELECT
    s.*,
    CASE WHEN s.ZC_TRTYPE = 'U1' THEN s.ZK_SALES ELSE 0 END AS RS
  FROM routine_4SACWQQQAQT6OPMBIXAI5KJNA_source s
),

routine_4SACWQQQAQT6OPMBIXAI5KJNA_rc AS (
  -- 3) Return code logic: In ABAP, &RC& is set to 0 to indicate a successful processing (no error).
  --    Here, we simply add a new column RC = 0.
  SELECT
    d.*,
    0 AS RC
  FROM routine_4SACWQQQAQT6OPMBIXAI5KJNA_debit d
),

routine_4SACWQQQAQT6OPMBIXAI5KJNA AS (
  -- 4) Abort logic: In ABAP, &AB& is set to 0, meaning do not abort the loading process.
  --    We add a column AB = 0 to capture that logic.
  SELECT
    r.*,
    0 AS AB
  FROM routine_4SACWQQQAQT6OPMBIXAI5KJNA_rc r
)