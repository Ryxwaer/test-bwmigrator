WITH routine_73ONEASWI01Y0EXXPBWPZHR8H_step1 AS (
  ---------------------------------------------------------------------------
  -- This CTE corresponds to reading from the source table (2LIS_13_VDITM).
  -- In the ABAP code, this would be the starting point where &CS& structure
  -- is available for further transformations. No logic is changed here; we
  -- are simply selecting all raw data as-is for subsequent steps.
  ---------------------------------------------------------------------------
  SELECT
    t.*
  FROM `2LIS_13_VDITM` t
),

routine_73ONEASWI01Y0EXXPBWPZHR8H_step2 AS (
  ---------------------------------------------------------------------------
  -- This CTE implements the "only credit" logic from ABAP:
  --   clear &RS&);
  --   if &CS&-/BIC/ZC_TRTYPE = 'U3' then &RS& = &CS&-/BIC/ZK_SALES
  -- Translated to SQL, we set RS = ZK_SALES when ZC_TRTYPE = 'U3', else 0.
  -- This is done with a CASE expression.
  ---------------------------------------------------------------------------
  SELECT
    s1.*,
    CASE 
      WHEN s1.ZC_TRTYPE = 'U3' THEN s1.ZK_SALES
      ELSE 0
    END AS RS
  FROM routine_73ONEASWI01Y0EXXPBWPZHR8H_step1 s1
),

routine_73ONEASWI01Y0EXXPBWPZHR8H_step3 AS (
  ---------------------------------------------------------------------------
  -- This CTE implements the ABAP line:
  --   &RC& = 0.
  -- If the return code (&RC&) is not zero, the rows would be excluded from
  -- further processing in ABAP. We explicitly set RC = 0 here, mirroring
  -- the ABAP code's outcome that no rows are excluded.
  ---------------------------------------------------------------------------
  SELECT
    s2.*,
    0 AS RC
  FROM routine_73ONEASWI01Y0EXXPBWPZHR8H_step2 s2
),

routine_73ONEASWI01Y0EXXPBWPZHR8H AS (
  ---------------------------------------------------------------------------
  -- This final CTE implements the ABAP line:
  --   &AB& = 0.
  -- Setting AB = 0 means the update process is not canceled.
  -- All transformations are preserved, and this completes the routine logic.
  ---------------------------------------------------------------------------
  SELECT
    s3.*,
    0 AS AB
  FROM routine_73ONEASWI01Y0EXXPBWPZHR8H_step3 s3
)