WITH routine_2MN1WZTKV14ELK1AYC34WA7V7_pre AS (
  -- Reads all data from 2LIS_13_VDITM (Info Source).
  -- Mirrors the ABAP routine's initial data access step.
  SELECT
    *
  FROM `myproject.mydataset.2LIS_13_VDITM`
),
routine_2MN1WZTKV14ELK1AYC34WA7V7_debit_only AS (
  -- Implements "only Debit" logic from ABAP:
  --   CLEAR &RS& => RS initialized to 0
  --   IF ZC_TRTYPE = 'U1' => RS = ZK_SALES
  SELECT
    pre.*,
    CASE WHEN pre.ZC_TRTYPE = 'U1' THEN pre.ZK_SALES ELSE 0 END AS RS
  FROM routine_2MN1WZTKV14ELK1AYC34WA7V7_pre pre
),
routine_2MN1WZTKV14ELK1AYC34WA7V7 AS (
  -- Mirrors ABAP final steps:
  --   &RC& = 0 => no error return code
  --   &AB& = 0 => no abort
  SELECT
    debit_only.*,
    0 AS RC,
    0 AS AB
  FROM routine_2MN1WZTKV14ELK1AYC34WA7V7_debit_only debit_only
)