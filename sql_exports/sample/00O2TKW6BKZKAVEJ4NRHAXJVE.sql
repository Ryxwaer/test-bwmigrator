WITH routine_00O2TKW6BKZKAVEJ4NRHAXJVE_step1 AS (
  -- This code block corresponds to reading from the SAP BW DataSource "2LIS_13_VDITM" 
  -- and transforming the field VGBEL to uppercase, mirroring the ABAP "TRANSLATE &TS&-VGBEL TO UPPER CASE".
  -- No joins, aggregations, or lookups are performed here; it simply selects all columns
  -- and adds an uppercase version of VGBEL.
  SELECT
    t.*,
    UPPER(t.VGBEL) AS VGBEL_UPPER
  FROM 2LIS_13_VDITM AS t
),

routine_00O2TKW6BKZKAVEJ4NRHAXJVE AS (
  -- This code block maps to setting &RS& = &TS&-VGBEL (which is now uppercase),
  -- and setting &RE& = 0, and &AB& = 0. Per the ABAP routine, returncode (RE) = 0 
  -- indicates we keep the record, and abort (AB) = 0 indicates not skipping the entire package.
  -- No additional join, aggregation, or filtering occurs.
  SELECT
    s.* EXCEPT(VGBEL_UPPER),
    s.VGBEL_UPPER AS RS,
    0 AS RE,
    0 AS AB
  FROM routine_00O2TKW6BKZKAVEJ4NRHAXJVE_step1 AS s
)