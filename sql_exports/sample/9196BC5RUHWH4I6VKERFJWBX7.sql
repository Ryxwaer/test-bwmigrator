WITH routine_9196BC5RUHWH4I6VKERFJWBX7_1 AS (
  --------------------------------------------------------------------------------
  -- ABAP Mapping:
  --   "clear &RS&" sets &RS& to 0 by default.
  --   "if &CS&-/BIC/ZC_TRTYPE = 'U1'. &RS& = &CS&-/BIC/ZK_SALESq. endif."
  --     means &RS& should only hold ZK_SALESq when ZC_TRTYPE = 'U1', otherwise 0.
  --
  -- What it does here:
  --   1) Reads from the source table 2LIS_13_VDITM.
  --   2) Creates a new column RS that is conditionally populated based on ZC_TRTYPE.
  --
  -- This is a set-based transformation using a CASE expression to replicate the IF logic.
  --------------------------------------------------------------------------------
  SELECT
    t.*,
    CASE
      WHEN t.ZC_TRTYPE = 'U1' THEN t.ZK_SALESq
      ELSE 0
    END AS RS
  FROM 2LIS_13_VDITM t
),
routine_9196BC5RUHWH4I6VKERFJWBX7 AS (
  --------------------------------------------------------------------------------
  -- ABAP Mapping:
  --   "&RC& = 0." and "&AB& = 0."
  --
  -- What it does here:
  --   1) Keeps all columns from the previous step.
  --   2) Adds the two new columns RC and AB, both set to 0.
  --
  -- This is a simple column addition with fixed constants, no joins or aggregations.
  --------------------------------------------------------------------------------
  SELECT
    s1.*,
    0 AS RC,
    0 AS AB
  FROM routine_9196BC5RUHWH4I6VKERFJWBX7_1 s1
)