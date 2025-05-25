WITH -- Read from the source table ZSA_P_V2LIS_13_VDITM and map columns to the function input parameters
routine_B9RIM6FYQE3MJYB03UJ23XLE7_source AS (
  --------------------------------------------------------------------------------
  -- This CTE corresponds to reading the source data (ZSA_P_V2LIS_13_VDITM).
  -- It selects the fields needed to call ZBW_CUSTSL:
  --   pkunwe  -> i_ship_to
  --   KUNAG   -> i_sold_to
  --   KUNRG   -> i_payer
  --   h_bus   -> i_bus
  --   vkorg   -> i_vkorg
  -- The optional i_date and i_busunit are not provided in the ABAP caller,
  -- so they are set to NULL here, keeping the logic intact.
  -- No filtering/aggregation is done here; it just prepares inputs.
  --------------------------------------------------------------------------------
  SELECT
    pkunwe  AS i_ship_to,
    KUNAG   AS i_sold_to,
    KUNRG   AS i_payer,
    h_bus   AS i_bus,
    vkorg   AS i_vkorg,
    NULL    AS i_date,     -- ABAP optional parameter not passed
    NULL    AS i_busunit   -- ABAP optional parameter not passed
  FROM ZSA_P_V2LIS_13_VDITM
),

-- Look up /bic/zc_cln05 (lv_busunit) from /bi0/pcustomer using i_ship_to
routine_B9RIM6FYQE3MJYB03UJ23XLE7_pcustomer AS (
  --------------------------------------------------------------------------------
  -- This CTE simulates the "SELECT SINGLE ... FROM /bi0/pcustomer"
  -- by joining on (i_ship_to = customer) and objvers = 'A'.
  -- In ABAP, sy-subrc IS INITIAL means a row was found; here that equates
  -- to a non-null match via LEFT JOIN.
  -- No aggregation is performed; it's a simple lookup.
  --------------------------------------------------------------------------------
  SELECT
    src.*,
    pcust."/bic/zc_cln05" AS pcust_zc_cln05
  FROM routine_B9RIM6FYQE3MJYB03UJ23XLE7_source src
  LEFT JOIN (
    SELECT
      customer,
      objvers,
      "/bic/zc_cln05"
    FROM `/bi0/pcustomer`
    WHERE objvers = 'A'
  ) pcust
  ON src.i_ship_to = pcust.customer
),

-- Apply the ZBW_CUSTSL logic step-by-step to compute e_result, e_returncode, and e_abort
routine_B9RIM6FYQE3MJYB03UJ23XLE7 AS (
  --------------------------------------------------------------------------------
  -- This CTE replicates function ZBW_CUSTSL:
  -- 1) Checks i_date >= '20220101'.
  --    a) If i_vkorg in {1640,1642,1010,0800,0530}, then e_result = i_sold_to.
  --    b) Else if i_busunit IS NULL, then we examine pcust_zc_cln05:
  --       - If found (sy-subrc = 0) and pcust_zc_cln05 = 'B2B',
  --         e_result = i_sold_to
  --       - Else e_result = i_ship_to
  --       - If row not found, e_result remains NULL (ABAP does nothing).
  --      If i_busunit is not NULL, e_result remains not assigned here (ABAP logic).
  -- 2) If i_date < '20220101':
  --    a) If i_vkorg in {1640,1642,1010,0800,0530}, then e_result = i_sold_to.
  --    b) Else:
  --       - If i_bus='01', e_result = i_sold_to
  --       - ElseIf i_bus='02', e_result = i_ship_to
  --       - Else e_result = i_sold_to
  -- 3) Sets e_returncode=0 and e_abort=0.
  -- No aggregation or ranking, just conditional data transformation.
  --------------------------------------------------------------------------------
  SELECT
    pc.*,
    CASE
      WHEN pc.i_date >= '20220101'
      THEN CASE
             WHEN pc.i_vkorg IN ('1640','1642','1010','0800','0530') THEN pc.i_sold_to
             ELSE CASE
                    WHEN pc.i_busunit IS NULL
                    THEN CASE
                           WHEN pc.pcust_zc_cln05 IS NOT NULL
                           THEN CASE
                                  WHEN pc.pcust_zc_cln05 = 'B2B' THEN pc.i_sold_to
                                  ELSE pc.i_ship_to
                                END
                           ELSE NULL
                         END
                    ELSE NULL
                  END
           END
      ELSE CASE
             WHEN pc.i_vkorg IN ('1640','1642','1010','0800','0530') THEN pc.i_sold_to
             ELSE CASE
                    WHEN pc.i_bus = '01' THEN pc.i_sold_to
                    WHEN pc.i_bus = '02' THEN pc.i_ship_to
                    ELSE pc.i_sold_to
                  END
           END
    END AS e_result,
    0 AS e_returncode,
    0 AS e_abort
  FROM routine_B9RIM6FYQE3MJYB03UJ23XLE7_pcustomer pc
)