WITH routine_00O2TQKP3JEC5ZU5UED31U5R4_step1 AS (
  -- This CTE retrieves all records from the source DS (ZSA_P_V2LIS_13_VDITM_V2).
  -- It also simulates capturing a record index (&RN&) needed in later error handling
  -- (equivalent to filling l_s_errorlog-RECORD in ABAP) by using ROW_NUMBER.
  SELECT
    t.*,
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RN
  FROM ZSA_P_V2LIS_13_VDITM_V2 AS t
),
routine_00O2TQKP3JEC5ZU5UED31U5R4_step2 AS (
  -- This CTE simulates the ABAP logic:
  -- 1) Check if a given SalesOrg (VKORG) has a cached entry (it_salesorg) - 
  --    in ABAP, if sy-subrc is not initial, then 2) fallback to SELECT SINGLE from /BI0/PSALESORG.
  -- Here we condense it into a single left join to /BI0/PSALESORG for the lookup,
  -- replicating the final outcome of possibly setting &RS& if the record is found.
  SELECT
    s1.*,
    p."/BIC/ZC_ICGRP" AS RS
  FROM routine_00O2TQKP3JEC5ZU5UED31U5R4_step1 AS s1
  LEFT JOIN /BI0/PSALESORG AS p
    ON s1.VKORG = p.SALESORG
    AND p.OBJVERS = 'A'
),
routine_00O2TQKP3JEC5ZU5UED31U5R4 AS (
  -- This final CTE completes the ABAP routine behavior:
  -- 1) If RS (/BIC/ZC_ICGRP) is blank (NULL), set &RE&=4 (skip record) and prepare error logging fields.
  -- 2) Otherwise set &RE&=0 and &AB&=0, meaning the record is valid and does not abort the data package.
  SELECT
    s2.*,
    CASE 
      WHEN s2.RS IS NULL THEN 4    -- &RE&=4 if not found
      ELSE 0
    END AS RE,
    CASE
      WHEN s2.RS IS NULL THEN 0    -- In this ABAP code, &AB&=0 always (even if error), so we replicate that
      ELSE 0
    END AS AB,
    CASE
      WHEN s2.RS IS NULL THEN s2.RN
      ELSE NULL
    END AS ERROR_RECORD,  -- l_s_errorlog-RECORD
    CASE
      WHEN s2.RS IS NULL THEN 'E'
      ELSE NULL
    END AS ERROR_MSGTY,   -- l_s_errorlog-MSGTY
    CASE
      WHEN s2.RS IS NULL THEN 'ZBW'
      ELSE NULL
    END AS ERROR_MSGID,   -- l_s_errorlog-MSGID
    CASE
      WHEN s2.RS IS NULL THEN '040'
      ELSE NULL
    END AS ERROR_MSGNO,   -- l_s_errorlog-MSGNO
    CASE
      WHEN s2.RS IS NULL THEN s2.VKORG
      ELSE NULL
    END AS ERROR_MSGV1    -- l_s_errorlog-MSGV1 (holds &TS&-vkorg)
  FROM routine_00O2TQKP3JEC5ZU5UED31U5R4_step2 AS s2
);