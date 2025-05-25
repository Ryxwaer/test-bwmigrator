WITH routine_6GGQUWFV4ZP854KPFR52CGDQ3_input AS (
  -- This code block references the source InfoSource table "2LIS_11_VAITM".
  -- It selects all columns for subsequent transformations.
  -- No join, aggregation, or lookup is performed here.
  SELECT
    *
  FROM `project.dataset.2LIS_11_VAITM`
),

routine_6GGQUWFV4ZP854KPFR52CGDQ3_set_result AS (
  -- Implements the ABAP logic: if REASON_REJ is not initial, set RS to ZK_ORDRcq.
  -- Uses a CASE statement to handle the condition (no joins, lookups, or aggregations).
  SELECT
    t.*,
    CASE 
      WHEN t.REASON_REJ IS NOT NULL 
           AND t.REASON_REJ <> '' 
      THEN t.ZK_ORDRcq 
      ELSE NULL 
    END AS RS
  FROM routine_6GGQUWFV4ZP854KPFR52CGDQ3_input t
),

routine_6GGQUWFV4ZP854KPFR52CGDQ3 AS (
  -- Sets RC=0 and AB=0 to follow the ABAP routine logic that the process should not abort.
  -- No other transformations are done; simply adding two constant columns.
  SELECT
    sr.*,
    0 AS RC,
    0 AS AB
  FROM routine_6GGQUWFV4ZP854KPFR52CGDQ3_set_result sr
)