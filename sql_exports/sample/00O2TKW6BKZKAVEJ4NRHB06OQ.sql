WITH routine_00O2TKW6BKZKAVEJ4NRHB06OQ_input AS (
  --------------------------------------------------------------------------------
  -- Maps to: Reading the input table 2LIS_13_VDITM (the “&TS&” structure in ABAP)
  -- What it does: Retrieves all columns from 2LIS_13_VDITM and generates a row
  -- number (RN) to replicate the &RN& usage in ABAP for error logging context.
  -- No joins or aggregations here—just direct data read.
  --------------------------------------------------------------------------------
  SELECT
    ROW_NUMBER() OVER() AS RN,
    t.*
  FROM 2LIS_13_VDITM t
),

routine_00O2TKW6BKZKAVEJ4NRHB06OQ_join AS (
  --------------------------------------------------------------------------------
  -- Maps to: "READ TABLE it_billtype INTO wa_billtype WITH KEY bill_type = &TS&-fkart"
  -- What it does: Performs a LEFT JOIN to replicate the lookup of bill type 
  -- from the it_billtype table. If no record is found, columns from billtype are null.
  -- This is effectively the ABAP sy-subrc=0 vs. sy-subrc<>0 check via found/not found joined rows.
  -- No aggregation, just a lookup join.
  --------------------------------------------------------------------------------
  SELECT
    i.RN,
    i.FKART,
    i.SHKZG,
    i.*,
    b./BIC/ZC_TRTYPE  AS zc_trtype,
    b./BIC/ZC_EXCLD   AS zc_excld
  FROM routine_00O2TKW6BKZKAVEJ4NRHB06OQ_input i
  LEFT JOIN it_billtype b
    ON i.FKART = b.bill_type
),

routine_00O2TKW6BKZKAVEJ4NRHB06OQ AS (
  --------------------------------------------------------------------------------
  -- Maps to: The core IF/ELSE logic setting &RS& (RS), &RE& (RE), &AB& (AB).
  --
  -- 1) If a matching billtype row is found (zc_trtype not empty),
  --    then RS is set to zc_trtype unless (FKART in ZBON, ZUMS, ZPRL
  --    and SHKZG in X,B) => RS = 'U3'. RE=0 (no error).
  --
  -- 2) Otherwise RS is cleared ('') and if zc_excld is empty, then RE=4
  --    (record-level skip/error) else RE=0. AB=0 is always set (no package abort).
  --
  -- Uses CASE WHEN for conditions. No further joins or aggregations here.
  --------------------------------------------------------------------------------
  SELECT
    j.*,
    CASE
      WHEN j.zc_trtype IS NOT NULL AND j.zc_trtype <> ''
      THEN CASE
             WHEN j.FKART IN ('ZBON', 'ZUMS', 'ZPRL')
                  AND j.SHKZG IN ('X', 'B')
             THEN 'U3'
             ELSE j.zc_trtype
           END
      ELSE ''
    END AS RS,
    CASE
      WHEN j.zc_trtype IS NOT NULL AND j.zc_trtype <> ''
      THEN 0
      ELSE CASE
             WHEN j.zc_excld IS NULL OR j.zc_excld = ''
             THEN 4
             ELSE 0
           END
    END AS RE,
    0 AS AB
  FROM routine_00O2TKW6BKZKAVEJ4NRHB06OQ_join j
)