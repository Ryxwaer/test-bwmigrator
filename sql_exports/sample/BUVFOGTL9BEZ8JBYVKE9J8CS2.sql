WITH routine_BUVFOGTL9BEZ8JBYVKE9J8CS2_monitor_entries AS (
  ---------------------------------------------------------------------------------------------
  -- Step 1: Maps to the ABAP logic that “fills the internal table MONITOR”
  -- Here, we simply read from the source “2LIS_13_VDITM” to emulate gathering the monitor data.
  ---------------------------------------------------------------------------------------------
  SELECT
    t.*
  FROM 2LIS_13_VDITM t
),

routine_BUVFOGTL9BEZ8JBYVKE9J8CS2_debit_only AS (
  ---------------------------------------------------------------------------------------------
  -- Step 2: Maps to the ABAP “IF &CS&-/BIC/ZC_TRTYPE = 'U1'.  &RS& = &CS&-/BIC/ZK_SALES. ENDIF.”
  -- We set RS to /BIC/ZK_SALES only for entries with /BIC/ZC_TRTYPE = 'U1', otherwise 0.
  ---------------------------------------------------------------------------------------------
  SELECT
    m.*,
    CASE 
      WHEN m."/BIC/ZC_TRTYPE" = 'U1' THEN m."/BIC/ZK_SALES"
      ELSE 0
    END AS RS
  FROM routine_BUVFOGTL9BEZ8JBYVKE9J8CS2_monitor_entries m
),

routine_BUVFOGTL9BEZ8JBYVKE9J8CS2_return_code AS (
  ---------------------------------------------------------------------------------------------
  -- Step 3: Maps to the ABAP logic “&RC& = 0”
  -- We add a column RC with a fixed value 0 for all rows. 
  ---------------------------------------------------------------------------------------------
  SELECT
    d.*,
    0 AS RC
  FROM routine_BUVFOGTL9BEZ8JBYVKE9J8CS2_debit_only d
),

routine_BUVFOGTL9BEZ8JBYVKE9J8CS2_abort AS (
  ---------------------------------------------------------------------------------------------
  -- Step 4: Maps to the ABAP logic “&AB& = 0”
  -- We add a column AB with a fixed value 0 for all rows.
  ---------------------------------------------------------------------------------------------
  SELECT
    r.*,
    0 AS AB
  FROM routine_BUVFOGTL9BEZ8JBYVKE9J8CS2_return_code r
),

routine_BUVFOGTL9BEZ8JBYVKE9J8CS2 AS (
  ---------------------------------------------------------------------------------------------
  -- Final Step: Result of combining all previous steps (equivalent to the complete ABAP routine).
  -- This CTE represents the final state after all transformations (RS, RC, AB) are applied.
  ---------------------------------------------------------------------------------------------
  SELECT
    a.*
  FROM routine_BUVFOGTL9BEZ8JBYVKE9J8CS2_abort a
)