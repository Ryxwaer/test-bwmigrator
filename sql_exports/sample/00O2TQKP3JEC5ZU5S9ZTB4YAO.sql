WITH routine_00O2TQKP3JEC5ZU5S9ZTB4YAO_source AS (
  ------------------------------------------------------------------
  -- This block corresponds to reading the source table ZSA_P_V2LIS_13_VDITM_V2.
  -- It provides the fields needed to call ZBW_CUSTSL:
  --   i_ship_to  <- pkunwe
  --   i_sold_to  <- kunag
  --   i_payer    <- kunrg
  --   i_vkorg    <- vkorg
  --   i_bus      <- h_bus
  --   i_date and i_busunit are assumed optional as per ABAP code,
  --   set to NULL here if not available in the source data.
  ------------------------------------------------------------------
  SELECT
    pkunwe AS i_ship_to,
    KUNAG  AS i_sold_to,
    kunrg  AS i_payer,
    vkorg  AS i_vkorg,
    h_bus  AS i_bus,
    CAST(NULL AS STRING) AS i_date,
    CAST(NULL AS STRING) AS i_busunit
  FROM ZSA_P_V2LIS_13_VDITM_V2
),

routine_00O2TQKP3JEC5ZU5S9ZTB4YAO_pcustomer AS (
  ------------------------------------------------------------------
  -- This block simulates the SELECT SINGLE from /BI0/PCUSTOMER,
  -- used in the ABAP code when i_busunit is initial.
  -- We do a LEFT JOIN so that rows with no matching customer
  -- still pass through (SY-SUBRC not initial logic -> no busunit found).
  -- /bic/zc_cln05 is brought in for potentially determining B2B vs B2C, etc.
  ------------------------------------------------------------------
  SELECT
    s.*,
    c."/bic/zc_cln05" AS busunit_lookup
  FROM routine_00O2TQKP3JEC5ZU5S9ZTB4YAO_source s
  LEFT JOIN `bi0_pcustomer` c
    ON s.i_ship_to = c.customer
   AND c.objvers = 'A'
),

routine_00O2TQKP3JEC5ZU5S9ZTB4YAO AS (
  ------------------------------------------------------------------
  -- This block corresponds to the main ZBW_CUSTSL function logic.
  --  1) Checks i_date >= '20220101' to choose between two code paths.
  --  2) For recent dates, if VKORG in ('1640','1642','1010','0800','0530'),
  --     then e_result = i_sold_to;
  --     else it may look up busunit (if i_busunit is initial).
  --  3) For older dates, again checks VKORG and then uses i_bus
  --     to decide if e_result = i_sold_to or i_ship_to.
  --  4) If the lookup (join) has no match, e_result remains NULL
  --     when i_busunit is null and SY-SUBRC is not 0.
  --  5) e_returncode = 0, e_abort = 0 for all rows (no skip logic triggered).
  ------------------------------------------------------------------
  SELECT
    p.*,
    CASE
      WHEN p.i_date >= '20220101'
      THEN CASE
             WHEN p.i_vkorg IN ('1640','1642','1010','0800','0530')
               THEN p.i_sold_to
             ELSE CASE
                    WHEN p.i_busunit IS NULL
                         AND p.busunit_lookup IS NOT NULL
                         AND p.busunit_lookup = 'B2B'
                      THEN p.i_sold_to
                    WHEN p.i_busunit IS NULL
                         AND p.busunit_lookup IS NOT NULL
                         AND p.busunit_lookup != 'B2B'
                      THEN p.i_ship_to
                    ELSE NULL
                  END
           END
      ELSE CASE
             WHEN p.i_vkorg IN ('1640','1642','1010','0800','0530')
               THEN p.i_sold_to
             ELSE CASE
                    WHEN p.i_bus = '01' THEN p.i_sold_to
                    WHEN p.i_bus = '02' THEN p.i_ship_to
                    ELSE p.i_sold_to
                  END
           END
    END AS e_result,
    0 AS e_returncode,
    0 AS e_abort
  FROM routine_00O2TQKP3JEC5ZU5S9ZTB4YAO_pcustomer p
)