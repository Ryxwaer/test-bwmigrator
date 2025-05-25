WITH routine_0G8GDWH0N7V7DWZNGEPK5PY83_fill_monitor AS (
  --------------------------------------------------------------------------------
  -- This CTE corresponds to the ABAP comment "fill the internal table 'MONITOR'."
  -- It simply selects all data from the source InfoSource (2LIS_13_VDITM).
  -- No filtering, no joins, no transformations here, just reading the raw data.
  --------------------------------------------------------------------------------
  SELECT
    t.*
  FROM `project.dataset.2LIS_13_VDITM` t
),
routine_0G8GDWH0N7V7DWZNGEPK5PY83_only_credit AS (
  --------------------------------------------------------------------------------
  -- This CTE implements the "only credit" logic:
  -- 1) CLEAR &RS& => initializes RS to 0.
  -- 2) IF ZC_TRTYPE = 'U3' THEN set RS to ZK_SALES; otherwise keep RS as 0.
  -- This uses a CASE WHEN for set-based assignment, no joins or aggregations.
  --------------------------------------------------------------------------------
  SELECT
    m.*,
    CASE
      WHEN m./BIC/ZC_TRTYPE = 'U3' THEN m./BIC/ZK_SALES
      ELSE 0
    END AS rs
  FROM routine_0G8GDWH0N7V7DWZNGEPK5PY83_fill_monitor m
),
routine_0G8GDWH0N7V7DWZNGEPK5PY83_rc_ab AS (
  --------------------------------------------------------------------------------
  -- This CTE sets the ABAP variables &RC& and &AB& to 0.
  -- In ABAP: &RC& = 0 (returncode), &AB& = 0 (abort).
  -- We add two new columns rc and ab, both set to 0.
  --------------------------------------------------------------------------------
  SELECT
    c.*,
    0 AS rc,
    0 AS ab
  FROM routine_0G8GDWH0N7V7DWZNGEPK5PY83_only_credit c
),
routine_0G8GDWH0N7V7DWZNGEPK5PY83 AS (
  --------------------------------------------------------------------------------
  -- Final CTE that unifies all previous logic steps into one output set.
  -- No additional transformations are performed here; it just selects
  -- everything from the prior step.
  --------------------------------------------------------------------------------
  SELECT
    *
  FROM routine_0G8GDWH0N7V7DWZNGEPK5PY83_rc_ab
)