WITH routine_8JT5GBWK36B23BBEWOAPOV0BX_input AS (
  ----------------------------------------------------------------------------
  -- Reads the source table 2LIS_13_VDITM to replicate the initial ABAP read 
  -- of the communication structure (&CS&) in the routine.
  -- No filtering or transformation yet, simply reading all relevant columns.
  ----------------------------------------------------------------------------
  SELECT
    ZC_TRTYPE,
    ZK_SALESq
    -- Include other necessary columns from 2LIS_13_VDITM here if needed
  FROM `project.dataset.2LIS_13_VDITM`
),
routine_8JT5GBWK36B23BBEWOAPOV0BX_calc_rs AS (
  ----------------------------------------------------------------------------
  -- Implements:
  --   clear &RS&.
  --   IF &CS&-/BIC/ZC_TRTYPE = 'U3'. &RS& = &CS&-/BIC/ZK_SALESq. ENDIF.
  -- Uses CASE WHEN to derive RS from ZC_TRTYPE and ZK_SALESq.
  ----------------------------------------------------------------------------
  SELECT
    t.*,
    CASE 
      WHEN t.ZC_TRTYPE = 'U3' THEN t.ZK_SALESq 
      ELSE 0 
    END AS RS
  FROM routine_8JT5GBWK36B23BBEWOAPOV0BX_input t
),
routine_8JT5GBWK36B23BBEWOAPOV0BX AS (
  ----------------------------------------------------------------------------
  -- Replicates:
  --   &RC& = 0.
  --   &AB& = 0.
  -- Sets RC and AB to zero for each row, indicating no errors or abort.
  ----------------------------------------------------------------------------
  SELECT
    t.*,
    0 AS RC,
    0 AS AB
  FROM routine_8JT5GBWK36B23BBEWOAPOV0BX_calc_rs t
)