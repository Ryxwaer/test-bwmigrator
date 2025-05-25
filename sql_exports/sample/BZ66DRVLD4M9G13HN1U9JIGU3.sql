WITH routine_BZ66DRVLD4M9G13HN1U9JIGU3_fill_monitor AS (
  -- This CTE corresponds to filling the internal table "MONITOR" in ABAP, 
  -- showing we are reading from the source 2LIS_13_VDITM without filtering or transformations yet.
  SELECT 
    t.*
  FROM 2LIS_13_VDITM AS t
),
routine_BZ66DRVLD4M9G13HN1U9JIGU3_debit_only AS (
  -- This CTE implements the "only Debit" ABAP logic:
  -- In ABAP, if &CS&-/BIC/ZC_TRTYPE = 'U3', then &RS& = &CS&-/BIC/ZK_SALESq; else it is cleared (0).
  SELECT
    fm.*,
    CASE 
      WHEN fm.ZC_TRTYPE = 'U3' THEN fm.ZK_SALESq 
      ELSE 0 
    END AS RS
  FROM routine_BZ66DRVLD4M9G13HN1U9JIGU3_fill_monitor AS fm
),
routine_BZ66DRVLD4M9G13HN1U9JIGU3_set_rc AS (
  -- This CTE sets the ABAP "&RC& = 0" logic, meaning no update cancellation is triggered due to return code.
  SELECT
    do.*,
    0 AS RC
  FROM routine_BZ66DRVLD4M9G13HN1U9JIGU3_debit_only AS do
),
routine_BZ66DRVLD4M9G13HN1U9JIGU3 AS (
  -- This final CTE sets "&AB& = 0" in ABAP, meaning the update process will not be aborted.
  SELECT
    sr.*,
    0 AS AB
  FROM routine_BZ66DRVLD4M9G13HN1U9JIGU3_set_rc AS sr
)