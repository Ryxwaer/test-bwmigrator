WITH 
--------------------------------------------------------------------------------
-- [CTE 1] Reads all source records from 2LIS_13_VDITM (the routine input).
-- Mirrors the initial step of the ABAP code where &TS& structure fields 
-- (prodh, vkorg, vtweg, spart, matnr) are read as the transformation input.
--------------------------------------------------------------------------------
routine_00O2TKW6BKZKAVEJ4NRHB0JBU_input AS (
  SELECT
    -- Assuming these columns match &TS&-prodh, &TS&-vkorg, etc.
    PRODH        AS i_prodh,
    VKORG        AS i_salesorg,
    VTWEG        AS i_distr_chan,
    SPART        AS i_division,
    MATNR        AS i_matnr
  FROM
    2LIS_13_VDITM
),

--------------------------------------------------------------------------------
-- [CTE 2] Derives l_sd_flag based on sales org and distribution channel. 
-- In ABAP, l_sd_flag is cleared if i_salesorg AND i_distr_chan are both non-empty.
-- Otherwise, it remains its default (which can be considered 'X' or blank as needed).
--------------------------------------------------------------------------------
routine_00O2TKW6BKZKAVEJ4NRHB0JBU_step2_determine_sd_flag AS (
  SELECT
    t1.*,
    CASE
      WHEN i_salesorg IS NOT NULL 
           AND i_salesorg <> ''
           AND i_distr_chan IS NOT NULL 
           AND i_distr_chan <> ''
      THEN ''  -- mirrors ABAP: l_sd_flag = '' 
      ELSE 'X' -- no explicit "ELSE" in ABAP, but we assume 'X' if not set
    END AS l_sd_flag
  FROM routine_00O2TKW6BKZKAVEJ4NRHB0JBU_input t1
),

--------------------------------------------------------------------------------
-- [CTE 3] Builds the preliminary product hierarchy field (l_prodh). 
-- 1) If i_prodh is non-empty => l_prodh = i_prodh concatenated with '000000000000000000'.
-- 2) Else, if i_matnr is non-empty => depending on l_sd_flag we try to look up 
--    the product hierarchy from either /bic/azo_mmd0100 (when l_sd_flag <> 'X' 
--    per the ABAP code snippet) or /bic/azo_mmd0500 (when l_sd_flag = '').
--    In both cases, if we find a matching row, we again append '000000000000000000'.
--------------------------------------------------------------------------------
routine_00O2TKW6BKZKAVEJ4NRHB0JBU_step3_determine_l_prodh AS (
  SELECT
    s2.*,
    CASE
      WHEN s2.i_prodh IS NOT NULL AND s2.i_prodh <> ''
        THEN CONCAT(s2.i_prodh, '000000000000000000')

      WHEN s2.i_matnr IS NOT NULL AND s2.i_matnr <> ''
        THEN CASE 
               WHEN s2.l_sd_flag <> 'X'
                 -- ABAP: select single from /bic/azo_mmd0100 where /bic/zc_matnr = i_matnr
                 -- If found, attach '000000000000000000'
                 THEN COALESCE(
                      CONCAT(mmd01.prod_hier, '000000000000000000'),
                      '' -- if no match, blank
                    )
               WHEN s2.l_sd_flag = ''
                 -- ABAP: select single from /bic/azo_mmd0500 
                 -- where mat_sales = i_matnr and salesorg = i_salesorg and distr_chan = i_distr_chan
                 THEN COALESCE(
                      CONCAT(mmd05.prod_hier, '000000000000000000'),
                      '' 
                    )
               ELSE '' -- fallback
             END
      ELSE '' 
    END AS l_prodh
  FROM routine_00O2TKW6BKZKAVEJ4NRHB0JBU_step2_determine_sd_flag s2
  LEFT JOIN (
      -- /bic/azo_mmd0100 lookup
      SELECT 
        /bic/zc_matnr      AS az_matnr,
        prod_hier
      FROM /bic/azo_mmd0100
  ) mmd01
    ON s2.i_matnr = mmd01.az_matnr
  LEFT JOIN (
      -- /bic/azo_mmd0500 lookup
      SELECT
        mat_sales  AS mat_sales,
        salesorg   AS salesorg,
        distr_chan AS distr_chan,
        prod_hier
      FROM /bic/azo_mmd0500
  ) mmd05
    ON s2.i_matnr    = mmd05.mat_sales
   AND s2.i_salesorg = mmd05.salesorg
   AND s2.i_distr_chan = mmd05.distr_chan
),

--------------------------------------------------------------------------------
-- [CTE 4] Loads zbw_prodhier_new which is the custom product hierarchy table. 
-- The table has salesorg, distr_chan, division, prodhfrom, prodhto, plus 
-- business, company, line, subline, itemgrseg. The ABAP code cleans wildcards
-- and then sorts by salesorg/distr_chan/division/prodhto/prodhfrom. 
-- The ABAP "delete" of empty entries is also handled by filtering out those rows.
--------------------------------------------------------------------------------
routine_00O2TKW6BKZKAVEJ4NRHB0JBU_step4_prodhier_new AS (
  SELECT
    salesorg,
    distr_chan,
    division,
    REPLACE(prodhfrom, '*', '') || '000000000000000000' AS prodhfrom_low,
    REPLACE(prodhto,   '*', '') || '999999999999999999' AS prodhfrom_high,
    business,
    company,
    line,
    subline,
    itemgrseg
  FROM zbw_prodhier_new
  WHERE NOT (
    salesorg = '' 
    AND distr_chan = '' 
    AND division = '' 
    AND prodhfrom = '' 
    AND prodhto = ''
  )
),

--------------------------------------------------------------------------------
-- [CTE 5] Applies the "access sequence" logic to find the matching row in 
-- zbw_prodhier_new. The ABAP routine tries multiple "access types" in order:
--   1) VKORG+VTWEG+SPART+prodh range
--   2) VKORG+VTWEG+''+prodh range
--   3) VKORG+''+''+prodh range
--   4) ''+VTWEG+''+prodh range
--   5) ''+''+''+prodh range
-- If i_salesorg/i_distr_chan/i_division are all empty => jump directly to #5.
-- We union them with ascending priority, then pick the first match (lowest priority).
--------------------------------------------------------------------------------
routine_00O2TKW6BKZKAVEJ4NRHB0JBU_step5_access_sequence AS (
  SELECT *
  FROM (
    SELECT 
      s3.*,
      ph.business,
      ph.company,
      ph.line,
      ph.subline,
      ph.itemgrseg,
      1 AS access_priority
    FROM routine_00O2TKW6BKZKAVEJ4NRHB0JBU_step3_determine_l_prodh s3
    JOIN routine_00O2TKW6BKZKAVEJ4NRHB0JBU_step4_prodhier_new ph
      ON ph.salesorg     = s3.i_salesorg
     AND ph.distr_chan   = s3.i_distr_chan
     AND ph.division     = s3.i_division
     AND s3.l_prodh     BETWEEN ph.prodhfrom_low AND ph.prodhfrom_high
    WHERE s3.i_salesorg <> '' AND s3.i_distr_chan <> '' AND s3.i_division <> ''

    UNION ALL

    SELECT 
      s3.*,
      ph.business,
      ph.company,
      ph.line,
      ph.subline,
      ph.itemgrseg,
      2 AS access_priority
    FROM routine_00O2TKW6BKZKAVEJ4NRHB0JBU_step3_determine_l_prodh s3
    JOIN routine_00O2TKW6BKZKAVEJ4NRHB0JBU_step4_prodhier_new ph
      ON ph.salesorg     = s3.i_salesorg
     AND ph.distr_chan   = s3.i_distr_chan
     AND ph.division     = ''
     AND s3.l_prodh     BETWEEN ph.prodhfrom_low AND ph.prodhfrom_high
    WHERE s3.i_salesorg <> '' AND s3.i_distr_chan <> '' AND s3.i_division <> 'X' -- keep same filter logic

    UNION ALL

    SELECT 
      s3.*,
      ph.business,
      ph.company,
      ph.line,
      ph.subline,
      ph.itemgrseg,
      3 AS access_priority
    FROM routine_00O2TKW6BKZKAVEJ4NRHB0JBU_step3_determine_l_prodh s3
    JOIN routine_00O2TKW6BKZKAVEJ4NRHB0JBU_step4_prodhier_new ph
      ON ph.salesorg   = s3.i_salesorg
     AND ph.distr_chan = ''
     AND ph.division   = ''
     AND s3.l_prodh   BETWEEN ph.prodhfrom_low AND ph.prodhfrom_high
    WHERE s3.i_salesorg <> ''

    UNION ALL

    SELECT 
      s3.*,
      ph.business,
      ph.company,
      ph.line,
      ph.subline,
      ph.itemgrseg,
      4 AS access_priority
    FROM routine_00O2TKW6BKZKAVEJ4NRHB0JBU_step3_determine_l_prodh s3
    JOIN routine_00O2TKW6BKZKAVEJ4NRHB0JBU_step4_prodhier_new ph
      ON ph.salesorg   = ''
     AND ph.distr_chan = s3.i_distr_chan
     AND ph.division   = ''
     AND s3.l_prodh   BETWEEN ph.prodhfrom_low AND ph.prodhfrom_high
    WHERE s3.i_distr_chan <> ''

    UNION ALL

    SELECT 
      s3.*,
      ph.business,
      ph.company,
      ph.line,
      ph.subline,
      ph.itemgrseg,
      5 AS access_priority
    FROM routine_00O2TKW6BKZKAVEJ4NRHB0JBU_step3_determine_l_prodh s3
    JOIN routine_00O2TKW6BKZKAVEJ4NRHB0JBU_step4_prodhier_new ph
      ON ph.salesorg   = ''
     AND ph.distr_chan = ''
     AND ph.division   = ''
     AND s3.l_prodh   BETWEEN ph.prodhfrom_low AND ph.prodhfrom_high
  ) unioned
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY 
      unioned.i_prodh,
      unioned.i_salesorg,
      unioned.i_distr_chan,
      unioned.i_division,
      unioned.i_matnr
    ORDER BY access_priority
  ) = 1
),

--------------------------------------------------------------------------------
-- [CTE 6] Emulates calls to go_prodh->get_cs, get_ps, get_rd. 
-- Since code for these methods is not shown, assume they fill e_bu_cs, e_prodseg, 
-- e_rd with values derived from the final l_prodh. 
-- Also return e_bus, e_comp, e_line, e_subli, e_itemgrseg from the matched row. 
-- This block concludes the function logic that picks and returns all relevant fields.
--------------------------------------------------------------------------------
routine_00O2TKW6BKZKAVEJ4NRHB0JBU_step6_derive_output AS (
  SELECT 
    step5.*,
    -- e_bus from matched business
    step5.business AS e_bus,
    -- e_comp from matched company
    step5.company  AS e_comp,
    -- e_line from matched line
    step5.line     AS e_line,
    -- e_subli from matched subline
    step5.subline  AS e_subli,
    -- e_itemgrseg from matched itemgrseg
    step5.itemgrseg AS e_itemgrseg,
    -- placeholders for get_cs, get_ps, get_rd calls
    -- in ABAP these are derived from go_prodh->get_cs(), get_ps(), get_rd()
    -- for demonstration, simply place a CASE or placeholder logic:
    CASE WHEN step5.l_prodh IS NOT NULL AND step5.l_prodh <> '' 
         THEN CONCAT('CS_for_', step5.l_prodh)
         ELSE ''
    END AS e_bu_cs,
    CASE WHEN step5.l_prodh IS NOT NULL AND step5.l_prodh <> '' 
         THEN CONCAT('PS_for_', step5.l_prodh)
         ELSE ''
    END AS e_prodseg,
    CASE WHEN step5.l_prodh IS NOT NULL AND step5.l_prodh <> '' 
         THEN CONCAT('RD_for_', step5.l_prodh)
         ELSE ''
    END AS e_rd
  FROM routine_00O2TKW6BKZKAVEJ4NRHB0JBU_step5_access_sequence step5
),

--------------------------------------------------------------------------------
-- [CTE 7 - FINAL CTE] Implements the ABAP logic for error handling: 
-- If e_bus (the final output) is empty => we set an error condition (re=4). 
-- Otherwise re=0. Also set &AB&=0 (no package abort). 
-- This concludes the routine, returning all original fields plus the derived ones.
--------------------------------------------------------------------------------
routine_00O2TKW6BKZKAVEJ4NRHB0JBU AS (
  SELECT
    s6.i_prodh,
    s6.i_salesorg,
    s6.i_distr_chan,
    s6.i_division,
    s6.i_matnr,
    s6.l_sd_flag,
    s6.l_prodh,
    s6.business AS matched_business,  -- debug only
    s6.company  AS matched_company,   -- debug only
    s6.line     AS matched_line,      -- debug only
    s6.subline  AS matched_subline,   -- debug only
    s6.itemgrseg AS matched_itemgrseg,-- debug only

    -- The final ABAP outputs:
    s6.e_bus,
    s6.e_bu_cs,
    s6.e_comp,
    s6.e_line,
    s6.e_subli,
    s6.e_prodseg,
    s6.e_itemgrseg,
    s6.e_rd,

    -- Return code (RE) and abort code (AB), per ABAP:
    CASE 
      WHEN s6.e_bus IS NULL OR s6.e_bus = '' THEN 4
      ELSE 0
    END AS RE,
    0 AS AB
  FROM routine_00O2TKW6BKZKAVEJ4NRHB0JBU_step6_derive_output s6
)
