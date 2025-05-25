WITH routine_6P2NSTVBK1LWDG2CS8MIMIUI9_step1 AS (
  --------------------------------------------------------------------------------
  -- 1) Maps to the ABAP comment: "* fill the internal table 'MONITOR', to make monitor entries"
  --    Here we simply select all data from 2LIS_13_VDITM without transformation,
  --    as the ABAP code only indicates a monitor fill with no filtering or logic.
  --------------------------------------------------------------------------------
  SELECT
    *
  FROM
    `2LIS_13_VDITM`
),
routine_6P2NSTVBK1LWDG2CS8MIMIUI9_step2 AS (
  --------------------------------------------------------------------------------
  -- 2) Maps to the ABAP logic: 
  --    "clear &RS&. if &CS&-/BIC/ZC_TRTYPE = 'U3' then &RS& = &CS&-/BIC/ZK_SALES."
  --    This logic is handled using a CASE WHEN to set RS to ZK_SALES if ZC_TRTYPE = 'U3', else 0.
  --    There is no join or aggregation, just a conditional column derivation.
  --------------------------------------------------------------------------------
  SELECT
    step1.*,
    CASE
      WHEN step1.ZC_TRTYPE = 'U3' THEN step1.ZK_SALES
      ELSE 0
    END AS RS
  FROM
    routine_6P2NSTVBK1LWDG2CS8MIMIUI9_step1 AS step1
),
routine_6P2NSTVBK1LWDG2CS8MIMIUI9 AS (
  --------------------------------------------------------------------------------
  -- 3) Maps to the ABAP logic:
  --    "* if the returncode is not equal zero, the result will not be updated -> &RC& = 0
  --     * if abort is not equal zero, the update process will be canceled -> &AB& = 0."
  --    We set RC and AB to 0 for all rows, indicating no returncode or abort.
  --------------------------------------------------------------------------------
  SELECT
    step2.*,
    0 AS RC,
    0 AS AB
  FROM
    routine_6P2NSTVBK1LWDG2CS8MIMIUI9_step2 AS step2
)