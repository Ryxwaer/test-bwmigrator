WITH ------------------------------------------------------------------------------
-- [1] Initial input data
-- Reads from the routine input table ZSA_P_V2LIS_13_VDITM_V2
-- and prepares the basic fields needed for subsequent logic.
-- No joins yet, just exposing input columns.
------------------------------------------------------------------------------
routine_00O2TQKP3JEC5ZU5JMM15K334_input AS (
  SELECT
    /* Original &TS&-prodh, &TS&-vkorg, &TS&-vtweg, &TS&-spart, &TS&-matnr */
    prodh       AS i_prodh,
    vkorg       AS i_salesorg,
    vtweg       AS i_distr_chan,
    spart       AS i_division,
    matnr       AS i_matnr
  FROM ZSA_P_V2LIS_13_VDITM_V2
),

------------------------------------------------------------------------------
-- [2] Derive SD flag and initial l_prodh
-- This replicates the ABAP logic:
--   IF i_salesorg <> '' AND i_distr_chan <> '' THEN l_sd_flag=''
--   If i_prodh <> '' => l_prodh = i_prodh || '000000000000000000'
--   Else we will handle i_matnr logic in later steps (placeholder here).
-- No join here, just CASE WHEN transformations.
------------------------------------------------------------------------------
routine_00O2TQKP3JEC5ZU5JMM15K334_derive_prodh AS (
  SELECT
    i_prodh,
    i_salesorg,
    i_distr_chan,
    i_division,
    i_matnr,
    CASE 
      WHEN i_salesorg <> '' AND i_distr_chan <> '' THEN ''
      ELSE NULL
    END AS l_sd_flag,
    CASE
      WHEN i_prodh <> '' THEN CONCAT(i_prodh, '000000000000000000')
      ELSE '' 
    END AS l_prodh
  FROM routine_00O2TQKP3JEC5ZU5JMM15K334_input
),

------------------------------------------------------------------------------
-- [3] Lookup product hierarchy if original i_prodh was empty.
-- This step replicates the ABAP logic:
--   If i_prodh is still blank and i_matnr is not blank,
--   then depending on l_sd_flag we do a single SELECT from /bic/azo_mmd0100 or /bic/azo_mmd0500.
--   For demonstration, using LEFT JOIN with each table, pick logic with CASE WHEN.
-- No aggregation, just set-based lookups.
------------------------------------------------------------------------------
routine_00O2TQKP3JEC5ZU5JMM15K334_lprodh_enriched AS (
  SELECT
    d.i_prodh,
    d.i_salesorg,
    d.i_distr_chan,
    d.i_division,
    d.i_matnr,
    d.l_sd_flag,
    CASE 
      WHEN d.l_prodh <> '' THEN d.l_prodh
      WHEN d.i_matnr = '' THEN '' 
      WHEN d.l_sd_flag = 'X'
        -- Per ABAP, the code in WHEN 'X' is effectively blocked by "AND l_sd_flag <> 'X'",
        -- but we replicate it as a no-op. Use the joined /bic/azo_mmd0100 for demonstration if needed. 
        THEN COALESCE(CONCAT(am0.prod_hier, '000000000000000000'), '')
      ELSE
        /* WHEN l_sd_flag = '' => from /bic/azo_mmd0500 matching mat_sales, salesorg, distr_chan */
        COALESCE(CONCAT(am5.prod_hier, '000000000000000000'), '')
    END AS l_prodh_final
  FROM routine_00O2TQKP3JEC5ZU5JMM15K334_derive_prodh d
  LEFT JOIN /* /bic/azo_mmd0100 placeholder */ (
    SELECT
      /bic/zc_matnr   AS matnr_key,
      prod_hier
    FROM `bic_azo_mmd0100`
  ) am0
    ON d.i_matnr = am0.matnr_key
  LEFT JOIN /* /bic/azo_mmd0500 placeholder */ (
    SELECT
      mat_sales   AS matnr_key,
      salesorg    AS salesorg_key,
      distr_chan  AS vtweg_key,
      prod_hier
    FROM `bic_azo_mmd0500`
  ) am5
    ON d.i_matnr    = am5.matnr_key
   AND d.i_salesorg = am5.salesorg_key
   AND d.i_distr_chan = am5.vtweg_key
),

------------------------------------------------------------------------------
-- [4] Simulate reading buffer table gt_bus_buf first.
-- If match is found by keys (salesorg,distr_chan,division,prodh),
-- we take the buffer fields. Otherwise remain null for now.
-- This matches the ABAP "READ TABLE gt_bus_buf... IF sy-subrc=0 THEN fill e_bus/etc and EXIT."
-- No final exit in SQL, but we show all columns. No aggregation, just a left join.
------------------------------------------------------------------------------
routine_00O2TQKP3JEC5ZU5JMM15K334_buffer_lookup AS (
  SELECT
    l.i_prodh,
    l.i_salesorg,
    l.i_distr_chan,
    l.i_division,
    l.i_matnr,
    l.l_sd_flag,
    l.l_prodh_final,
    b.zc_bus    AS buf_bus,
    b.zc_bu_cs  AS buf_bu_cs,
    b.zc_comp   AS buf_comp,
    b.zc_line   AS buf_line,
    b.zc_subli  AS buf_subli,
    b.zc_ps     AS buf_ps,
    b.zc_ags    AS buf_ags,
    b.zc_respd  AS buf_respd
  FROM routine_00O2TQKP3JEC5ZU5JMM15K334_lprodh_enriched l
  LEFT JOIN (
    -- Simulating gt_bus_buf as some permanent or interim store
    SELECT
      salesorg,
      distr_chan,
      division,
      prodh,
      zc_bus,
      zc_bu_cs,
      zc_comp,
      zc_line,
      zc_subli,
      zc_ps,
      zc_ags,
      zc_respd
    FROM `gt_bus_buf`
  ) b
    ON l.i_salesorg    = b.salesorg
   AND l.i_distr_chan  = b.distr_chan
   AND l.i_division    = b.division
   AND l.l_prodh_final = b.prodh
),

------------------------------------------------------------------------------
-- [5] Load and prepare ZBW_PRODHier_New (gt_prodhier_new),
-- mimicking the ABAP: "SELECT * FROM zbw_prodhier_new"
-- plus cleaning up '*' in from/to, then sorting. For simplicity,
-- we just load it, and remove fully empty entries. No grouping, no windows.
------------------------------------------------------------------------------
routine_00O2TQKP3JEC5ZU5JMM15K334_hier_tab AS (
  SELECT
    salesorg,
    distr_chan,
    division,
    REGEXP_REPLACE(prodhfrom, r'\*', '') || '000000000000000000' AS prodhfrom_mod,
    REGEXP_REPLACE(prodhto,   r'\*', '') || '999999999999999999' AS prodhto_mod,
    business,
    company,
    line,
    subline,
    itemgrseg
  FROM `zbw_prodhier_new`
  WHERE NOT(
       salesorg = ''
   AND  distr_chan = ''
   AND  division = ''
   AND  prodhfrom = ''
   AND  prodhto   = ''
  )
),

------------------------------------------------------------------------------
-- [6] Access sequence to pick correct hierarchy row based on i_salesorg/i_distr_chan/i_division
-- and prodh range. This block emulates all 5 checks:
-- (1) By VKORG/VTWEG/SPART/PRODH
-- (2) By VKORG/VTWEG/PRODH
-- (3) By VKORG/PRODH
-- (4) By VTWEG/PRODH
-- (5) By just PRODH
-- If any match is found, we pick that row (highest priority first).
-- We mimic the ABAP "LOOP AT ... EXIT" by using CASE/COALESCE priority logic.
-- No aggregates, just left join expansions with conditions. 
------------------------------------------------------------------------------
routine_00O2TQKP3JEC5ZU5JMM15K334_access_seq AS (
  SELECT
    bl.*,

    /* No SD org context => match row with all blank and range match */
    t0.business AS t0_business,
    t0.company  AS t0_company,
    t0.line     AS t0_line,
    t0.subline  AS t0_subline,
    t0.itemgrseg AS t0_ags,

    /* Access type 1 => all fields match */
    t1.business AS t1_business,
    t1.company  AS t1_company,
    t1.line     AS t1_line,
    t1.subline  AS t1_subline,
    t1.itemgrseg AS t1_ags,

    /* Access type 2 => salesorg+distr_chan, division blank */
    t2.business AS t2_business,
    t2.company  AS t2_company,
    t2.line     AS t2_line,
    t2.subline  AS t2_subline,
    t2.itemgrseg AS t2_ags,

    /* Access type 3 => salesorg only, rest blank */
    t3.business AS t3_business,
    t3.company  AS t3_company,
    t3.line     AS t3_line,
    t3.subline  AS t3_subline,
    t3.itemgrseg AS t3_ags,

    /* Access type 4 => distr_chan only, rest blank */
    t4.business AS t4_business,
    t4.company  AS t4_company,
    t4.line     AS t4_line,
    t4.subline  AS t4_subline,
    t4.itemgrseg AS t4_ags,

    /* Access type 5 => all blank, just range */
    t5.business AS t5_business,
    t5.company  AS t5_company,
    t5.line     AS t5_line,
    t5.subline  AS t5_subline,
    t5.itemgrseg AS t5_ags
  FROM routine_00O2TQKP3JEC5ZU5JMM15K334_buffer_lookup bl

  /* For the "no SD org context" scenario */
  LEFT JOIN routine_00O2TQKP3JEC5ZU5JMM15K334_hier_tab t0
    ON bl.i_salesorg = ''
   AND bl.i_distr_chan = ''
   AND bl.i_division = ''
   AND bl.l_prodh_final BETWEEN t0.prodhfrom_mod AND t0.prodhto_mod
   AND t0.salesorg = ''
   AND t0.distr_chan = ''
   AND t0.division = ''

  /* Access type 1 => i_salesorg/i_distr_chan/i_division not blank */
  LEFT JOIN routine_00O2TQKP3JEC5ZU5JMM15K334_hier_tab t1
    ON bl.i_salesorg <> ''
   AND bl.i_distr_chan <> ''
   AND bl.i_division  <> ''
   AND bl.i_salesorg = t1.salesorg
   AND bl.i_distr_chan = t1.distr_chan
   AND bl.i_division = t1.division
   AND bl.l_prodh_final BETWEEN t1.prodhfrom_mod AND t1.prodhto_mod

  /* Access type 2 => i_salesorg/i_distr_chan, division = '' */
  LEFT JOIN routine_00O2TQKP3JEC5ZU5JMM15K334_hier_tab t2
    ON bl.i_salesorg <> ''
   AND bl.i_distr_chan <> ''
   AND bl.i_division = ''
   AND bl.i_salesorg = t2.salesorg
   AND bl.i_distr_chan = t2.distr_chan
   AND t2.division = ''
   AND bl.l_prodh_final BETWEEN t2.prodhfrom_mod AND t2.prodhto_mod

  /* Access type 3 => i_salesorg, rest blank */
  LEFT JOIN routine_00O2TQKP3JEC5ZU5JMM15K334_hier_tab t3
    ON bl.i_salesorg <> ''
   AND bl.i_distr_chan = ''
   AND bl.i_division = ''
   AND bl.i_salesorg = t3.salesorg
   AND t3.distr_chan = ''
   AND t3.division = ''
   AND bl.l_prodh_final BETWEEN t3.prodhfrom_mod AND t3.prodhto_mod

  /* Access type 4 => i_distr_chan only */
  LEFT JOIN routine_00O2TQKP3JEC5ZU5JMM15K334_hier_tab t4
    ON bl.i_salesorg = ''
   AND bl.i_distr_chan <> ''
   AND bl.i_division = ''
   AND bl.i_distr_chan = t4.distr_chan
   AND t4.salesorg = ''
   AND t4.division = ''
   AND bl.l_prodh_final BETWEEN t4.prodhfrom_mod AND t4.prodhto_mod

  /* Access type 5 => all blank, just range */
  LEFT JOIN routine_00O2TQKP3JEC5ZU5JMM15K334_hier_tab t5
    ON (bl.i_salesorg <> '' OR bl.i_distr_chan <> '' OR bl.i_division <> '')
   AND t5.salesorg = ''
   AND t5.distr_chan = ''
   AND t5.division = ''
   AND bl.l_prodh_final BETWEEN t5.prodhfrom_mod AND t5.prodhto_mod
),

------------------------------------------------------------------------------
-- [7] Decide which matched row to use. Imitates "LOOP ... EXIT" priority:
--  If the buffer was found (buf_bus not null), use that. Else from step 1..5 in order.
--  We produce final "found" business, company, line, subline, itemgrseg.
--  Also replicate "get_cs", "get_ps", "get_rd" calls as if they are lookups. 
--  Replicate e_bus/e_bu_cs/e_comp/e_line/e_subli/e_prodseg/e_itemgrseg/e_rd logic.
--  If final e_bus is empty => &RE&=4 (skip), else &RE&=0.
------------------------------------------------------------------------------
routine_00O2TQKP3JEC5ZU5JMM15K334 AS (
  SELECT
    a.*,

    -- If buffer record found, we use that first. Otherwise we pick from access type in order.
    CASE
      WHEN a.buf_bus IS NOT NULL THEN a.buf_bus
      WHEN a.t1_business IS NOT NULL THEN a.t1_business
      WHEN a.t2_business IS NOT NULL THEN a.t2_business
      WHEN a.t3_business IS NOT NULL THEN a.t3_business
      WHEN a.t4_business IS NOT NULL THEN a.t4_business
      WHEN a.t5_business IS NOT NULL THEN a.t5_business
      WHEN a.t0_business IS NOT NULL THEN a.t0_business
      ELSE ''
    END AS e_bus,

    CASE
      WHEN a.buf_comp IS NOT NULL THEN a.buf_comp
      WHEN a.t1_company IS NOT NULL THEN a.t1_company
      WHEN a.t2_company IS NOT NULL THEN a.t2_company
      WHEN a.t3_company IS NOT NULL THEN a.t3_company
      WHEN a.t4_company IS NOT NULL THEN a.t4_company
      WHEN a.t5_company IS NOT NULL THEN a.t5_company
      WHEN a.t0_company IS NOT NULL THEN a.t0_company
      ELSE ''
    END AS e_comp,

    CASE
      WHEN a.buf_line IS NOT NULL THEN a.buf_line
      WHEN a.t1_line IS NOT NULL THEN a.t1_line
      WHEN a.t2_line IS NOT NULL THEN a.t2_line
      WHEN a.t3_line IS NOT NULL THEN a.t3_line
      WHEN a.t4_line IS NOT NULL THEN a.t4_line
      WHEN a.t5_line IS NOT NULL THEN a.t5_line
      WHEN a.t0_line IS NOT NULL THEN a.t0_line
      ELSE ''
    END AS e_line,

    CASE
      WHEN a.buf_subli IS NOT NULL THEN a.buf_subli
      WHEN a.t1_subline IS NOT NULL THEN a.t1_subline
      WHEN a.t2_subline IS NOT NULL THEN a.t2_subline
      WHEN a.t3_subline IS NOT NULL THEN a.t3_subline
      WHEN a.t4_subline IS NOT NULL THEN a.t4_subline
      WHEN a.t5_subline IS NOT NULL THEN a.t5_subline
      WHEN a.t0_subline IS NOT NULL THEN a.t0_subline
      ELSE ''
    END AS e_subli,

    CASE
      WHEN a.buf_ags IS NOT NULL THEN a.buf_ags
      WHEN a.t1_ags IS NOT NULL THEN a.t1_ags
      WHEN a.t2_ags IS NOT NULL THEN a.t2_ags
      WHEN a.t3_ags IS NOT NULL THEN a.t3_ags
      WHEN a.t4_ags IS NOT NULL THEN a.t4_ags
      WHEN a.t5_ags IS NOT NULL THEN a.t5_ags
      WHEN a.t0_ags IS NOT NULL THEN a.t0_ags
      ELSE ''
    END AS e_itemgrseg,

    -- Emulate go_prodh->get_cs(l_prodh_final) => e_bu_cs
    -- For demonstration, do a placeholder join/lookup for get_cs. 
    -- For details, replicate as needed. 
    COALESCE(a.buf_bu_cs,
      (SELECT ANY_VALUE(x.cs_code)
       FROM `some_cs_lookup` x
       WHERE x.prodhier_key = a.l_prodh_final),
      ''
    ) AS e_bu_cs,

    -- Emulate go_prodh->get_ps(l_prodh_final) => e_prodseg
    COALESCE(a.buf_ps,
      (SELECT ANY_VALUE(x.ps_code)
       FROM `some_ps_lookup` x
       WHERE x.prodhier_key = a.l_prodh_final),
      ''
    ) AS e_prodseg,

    -- Emulate go_prodh->get_rd(l_prodh_final) => e_rd
    COALESCE(a.buf_respd,
      (SELECT ANY_VALUE(x.rd_code)
       FROM `some_rd_lookup` x
       WHERE x.prodhier_key = a.l_prodh_final),
      ''
    ) AS e_rd,

    /* Simulate &RE&=4 if e_bus is blank, else 0. Also &AB&=0 always in ABAP. */
    CASE 
      WHEN (
        CASE
          WHEN a.buf_bus IS NOT NULL THEN a.buf_bus
          WHEN a.t1_business IS NOT NULL THEN a.t1_business
          WHEN a.t2_business IS NOT NULL THEN a.t2_business
          WHEN a.t3_business IS NOT NULL THEN a.t3_business
          WHEN a.t4_business IS NOT NULL THEN a.t4_business
          WHEN a.t5_business IS NOT NULL THEN a.t5_business
          WHEN a.t0_business IS NOT NULL THEN a.t0_business
          ELSE ''
        END
      ) = '' THEN 4
      ELSE 0
    END AS re_value,

    0 AS ab_value
  FROM routine_00O2TQKP3JEC5ZU5JMM15K334_access_seq a
)