WITH routine_00O2TKW6BKZKAVEJ4NRHAXDJU_call_function_zvbw_sd_get_cuttyp AS (
  ------------------------------------------------------------------
  -- This CTE replicates the CALL FUNCTION 'ZVBW_SD_GET_CUTTYP' step.
  -- 1) It takes MATNR (material) from 2LIS_13_VDITM (the routine input).
  -- 2) In ABAP, the function would return CUTTYP (e_cuttyp), RETURN CODE, ABORT info, etc.
  -- Here, we simply carry forward the input columns for further joins/lookups.
  -- No filtering or special transformation is done yet.
  ------------------------------------------------------------------
  SELECT
    i.*
    -- Optionally add placeholders for function output (e.g. e_returncode) if needed:
    --, NULL AS e_returncode
    --, NULL AS e_abort
  FROM `2LIS_13_VDITM` AS i
),

routine_00O2TKW6BKZKAVEJ4NRHAXDJU_read_table_it_matnr AS (
  ------------------------------------------------------------------
  -- This CTE replicates the READ TABLE it_matnr logic:
  -- 1) The ABAP code "READ TABLE it_matnr ... WITH TABLE KEY /bic/zc_matnr = &TS&-MATNR" 
  --    indicates a lookup by material.
  -- 2) If a matching row is found, it_matnr-/bic/zc_cuttyp is read.
  -- Here, we perform that lookup via LEFT JOIN on MATNR = /bic/zc_matnr,
  -- retrieving any matching /bic/zc_cuttyp from it_matnr.
  ------------------------------------------------------------------
  SELECT
    cf.*,
    m.`/bic/zc_cuttyp` AS matched_cuttyp
  FROM routine_00O2TKW6BKZKAVEJ4NRHAXDJU_call_function_zvbw_sd_get_cuttyp AS cf
  LEFT JOIN `it_matnr` AS m
    ON cf.matnr = m.`/bic/zc_matnr`
),

routine_00O2TKW6BKZKAVEJ4NRHAXDJU AS (
  ------------------------------------------------------------------
  -- This final CTE applies the ABAP "IF sy-subrc = 0 THEN &RS& = <fs_matnr>-/bic/zc_cuttyp":
  -- 1) In ABAP, sy-subrc = 0 means a successful READ TABLE match.
  -- 2) Here, we use CASE WHEN matched_cuttyp IS NOT NULL to emulate that condition.
  -- 3) &RS& is effectively the output field holding the /bic/zc_cuttyp value if found.
  ------------------------------------------------------------------
  SELECT
    rt.*,
    CASE WHEN rt.matched_cuttyp IS NOT NULL
         THEN rt.matched_cuttyp
         ELSE NULL
    END AS routine_result
  FROM routine_00O2TKW6BKZKAVEJ4NRHAXDJU_read_table_it_matnr AS rt
)