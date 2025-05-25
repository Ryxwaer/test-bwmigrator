WITH routine_2IZGF5UVPZ0ZW95H7KKZVMQW7_input AS (
  ----------------------------------------------------------------------------------------------------
  -- Maps to: "DATA: ...  read table it_billtype ...  &TS&-fkart."
  -- This block mimics reading from the SAP BW datasource ZSA_P_V2LIS_13_VDITM.
  -- We also create a row number (RN) to replicate the &RN& record counter for error logging.
  ----------------------------------------------------------------------------------------------------
  SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RN,
    t.*
  FROM `ZSA_P_V2LIS_13_VDITM` t
),

routine_2IZGF5UVPZ0ZW95H7KKZVMQW7_billtype AS (
  ----------------------------------------------------------------------------------------------------
  -- Maps to: "read table it_billtype ... into wa_billtype with key bill_type = ..."
  -- This block represents the it_billtype look-up table holding bill_type, /BIC/ZC_TRTYPE, /BIC/ZC_EXCLD.
  -- Used for the later JOIN to find ZC_TRTYPE and ZC_EXCLD based on fkart.
  ----------------------------------------------------------------------------------------------------
  SELECT
    bill_type,
    ZC_TRTYPE, 
    ZC_EXCLD
  FROM `it_billtype`
),

routine_2IZGF5UVPZ0ZW95H7KKZVMQW7_main AS (
  ----------------------------------------------------------------------------------------------------
  -- Maps to the main ABAP logic:
  --  IF a matching billtype record is found AND ZC_TRTYPE is not initial:
  --    &RS& = ZC_TRTYPE
  --    &RE& = 0
  --  ELSE
  --    &RS& = NULL
  --    IF ZC_EXCLD is initial => &RE& = 4 (error), otherwise &RE& = 0
  --  &AB& = 0 always
  -- Performed via a LEFT JOIN and CASE statements.
  ----------------------------------------------------------------------------------------------------
  SELECT
    i.*,
    CASE 
      WHEN b.ZC_TRTYPE IS NOT NULL AND b.ZC_TRTYPE <> '' THEN b.ZC_TRTYPE
      ELSE NULL
    END AS RS,
    CASE
      WHEN b.ZC_TRTYPE IS NOT NULL AND b.ZC_TRTYPE <> '' THEN 0
      WHEN b.ZC_EXCLD IS NULL OR b.ZC_EXCLD = '' THEN 4
      ELSE 0
    END AS RE,
    0 AS AB
  FROM routine_2IZGF5UVPZ0ZW95H7KKZVMQW7_input i
  LEFT JOIN routine_2IZGF5UVPZ0ZW95H7KKZVMQW7_billtype b
    ON i.fkart = b.bill_type
),

routine_2IZGF5UVPZ0ZW95H7KKZVMQW7_errorlog AS (
  ----------------------------------------------------------------------------------------------------
  -- Maps to: "append l_s_errorlog to G_T_ERRORLOG" if &RE&=4.
  -- This block gathers error log rows, containing record number (RN), error message info, and fkart.
  ----------------------------------------------------------------------------------------------------
  SELECT
    m.RN AS RECORD,
    'E' AS MSGTY,
    'ZBW' AS MSGID,
    '007' AS MSGNO,
    m.fkart AS MSGV1
  FROM routine_2IZGF5UVPZ0ZW95H7KKZVMQW7_main m
  WHERE m.RE = 4
),

routine_2IZGF5UVPZ0ZW95H7KKZVMQW7 AS (
  ----------------------------------------------------------------------------------------------------
  -- Final CTE: Represents the end result of the routine after all steps and lookups.
  -- Contains the main transformed data with &RS&, &RE&, &AB& set per the ABAP logic.
  ----------------------------------------------------------------------------------------------------
  SELECT
    *
  FROM routine_2IZGF5UVPZ0ZW95H7KKZVMQW7_main
)