WITH
  -- [routine_00O2TQKP3JEC5ZU5VZMMFYO4G_input]
  -- This CTE maps directly to reading all columns from the DataSource table ZSA_P_V2LIS_13_VDITM_V2 in ABAP.
  -- No transformations or filters are applied here; this is simply the input reference for subsequent steps.
  routine_00O2TQKP3JEC5ZU5VZMMFYO4G_input AS (
    SELECT
      *
    FROM
      `ZSA_P_V2LIS_13_VDITM_V2`
  ),

  -- [routine_00O2TQKP3JEC5ZU5VZMMFYO4G]
  -- This CTE implements the ABAP routine logic:
  -- 1) Extract the year from billing date (equivalent to &TS&-fkdat(4)).
  -- 2) &RE& = 0 → return code is 0, meaning do not skip this record.
  -- 3) &AB& = 0 → abort code is 0, meaning no abort of the entire data package.
  -- No joins, lookups, or aggregations are necessary; just a straightforward substring operation and fixed numeric assignments.
  routine_00O2TQKP3JEC5ZU5VZMMFYO4G AS (
    SELECT
      t.*,
      SUBSTR(t.FKDAT, 1, 4) AS YEAR_FROM_BILLING_DATE,
      0 AS RETURN_CODE,
      0 AS ABORT_CODE
    FROM
      routine_00O2TQKP3JEC5ZU5VZMMFYO4G_input t
  )