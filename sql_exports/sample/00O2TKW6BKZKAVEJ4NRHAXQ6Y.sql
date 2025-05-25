WITH routine_00O2TKW6BKZKAVEJ4NRHAXQ6Y_source AS (
  --------------------------------------------------------------------------------
  -- This CTE represents the routine input table (2LIS_13_VDITM).
  -- It corresponds to ABAP’s reading of &TS&-MATNR from the Data Source.
  -- No filtering is done here; it just reads the source data so subsequent steps
  -- can join or enrich it.
  --------------------------------------------------------------------------------
  SELECT
    *
  FROM `2LIS_13_VDITM`
),
routine_00O2TKW6BKZKAVEJ4NRHAXQ6Y_it_matnr AS (
  --------------------------------------------------------------------------------
  -- This CTE represents the internal table it_matnr, holding the fields
  -- /bic/zc_matnr and /bic/zc_mstaso.
  -- In ABAP, “READ TABLE it_matnr WITH TABLE KEY /bic/zc_matnr = &TS&-MATNR”
  -- is a lookup logic; here we simply expose those fields so we can join on them.
  --------------------------------------------------------------------------------
  SELECT
    /bic/zc_matnr,
    /bic/zc_mstaso
  FROM `it_matnr`
),
routine_00O2TKW6BKZKAVEJ4NRHAXQ6Y AS (
  --------------------------------------------------------------------------------
  -- This CTE implements the ABAP logic:
  --   IF sy-subrc = 0 THEN &RS& = <fs_matnr>-/bic/zc_mstaso ELSE skip.
  -- We use an INNER JOIN to skip rows with no matching entry (sy-subrc ≠ 0 in ABAP).
  -- The joined field /bic/zc_mstaso becomes RS, matching the routine’s assignment.
  --------------------------------------------------------------------------------
  SELECT
    src.*,
    it./bic/zc_mstaso AS RS
  FROM routine_00O2TKW6BKZKAVEJ4NRHAXQ6Y_source AS src
  JOIN routine_00O2TKW6BKZKAVEJ4NRHAXQ6Y_it_matnr AS it
    ON src.MATNR = it./bic/zc_matnr
)