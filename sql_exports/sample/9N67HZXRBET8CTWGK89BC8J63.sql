WITH routine_9N67HZXRBET8CTWGK89BC8J63_input AS (
  -- This CTE corresponds to reading the source data (the ABAP "input table" 2LIS_13_VDITM).
  -- It simply selects all rows and columns from 2LIS_13_VDITM so they can be transformed in the next step.
  SELECT
    *
  FROM `2LIS_13_VDITM`
),

routine_9N67HZXRBET8CTWGK89BC8J63 AS (
  -- This CTE applies the ABAP routine logic:
  -- 1) "clear &RS&" and then "if &CS&-/BIC/ZC_TRTYPE = 'U3' => &RS& = &CS&-/BIC/ZK_SALESq": implemented via CASE WHEN
  -- 2) "&RC& = 0" and "&AB& = 0" are set as fixed values (the routine always updates them to zero)
  -- No filtering is done; the logic just populates RS, RC, and AB for monitoring purposes.
  SELECT
    t.*,
    CASE
      WHEN t.ZC_TRTYPE = 'U3' THEN t.ZK_SALESq
      ELSE 0
    END AS RS,
    0 AS RC,
    0 AS AB
  FROM routine_9N67HZXRBET8CTWGK89BC8J63_input t
)