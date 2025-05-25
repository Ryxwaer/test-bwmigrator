WITH routine_00O2TKW6BKZKAVEJ4NRHAYFH6_input AS (
  ----------------------------------------------------------------------------
  -- This CTE corresponds to reading the ABAP input structure (&TS&) 
  -- from the source DS 2LIS_13_VDITM. 
  -- It simply selects all rows/columns, mirroring the input data the routine processes.
  ----------------------------------------------------------------------------
  SELECT
    -- Include all relevant columns from 2LIS_13_VDITM here:
    vkorg_auft,
    spara,
    vtweg,
    kunag,
    pkunwe,
    matnr,
    fkdat,
    prodh,
    zzpdflag
    -- plus any other columns needed
  FROM your_project.your_dataset.2LIS_13_VDITM
),
routine_00O2TKW6BKZKAVEJ4NRHAYFH6_pddet AS (
  ----------------------------------------------------------------------------
  -- This CTE corresponds to the lt_pddet internal table in ABAP. 
  -- It provides the lookup data: salesorg, division, distr_chan, sold_to, etc.
  -- ABAP "LOOP AT lt_pddet" is replaced by a full set scan here.
  ----------------------------------------------------------------------------
  SELECT
    salesorg,
    division,
    distr_chan,
    sold_to,
    ship_to,
    /bic/zc_matnr AS zc_matnr,
    datefrom,
    dateto,
    prodh,
    /bic/zc_pdcatv AS zc_pdcatv
    -- plus any other columns needed
  FROM your_project.your_dataset.lt_pddet
),
routine_00O2TKW6BKZKAVEJ4NRHAYFH6_joined AS (
  ----------------------------------------------------------------------------
  -- This CTE applies the ABAP "LOOP AT lt_pddet ... WHERE ..." logic in a set-based way:
  --
  -- 1) It only attempts a match if zzpdflag is not initial (zzpdflag <> '').
  -- 2) Joins on matching salesorg / division / distr_chan.
  -- 3) Matches sold_to/ship_to/matnr if they are either identical or blank in lt_pddet.
  -- 4) Checks that the requested date (fkdat) is between datefrom and dateto.
  -- 5) Recreates the ABAP PRODHS check ("IF prodh IS NOT INITIAL THEN ... CHECK ...").
  --    - If pddet.prodh is blank, no check required (it automatically matches).
  --    - Else if pddet.prodh contains '+' or '*', do a pattern-like check against the input's prodh.
  --    - Else do an exact match against the input's prodh.
  -- 6) Uses ROW_NUMBER to emulate "LOOP ... EXIT" in ABAP (taking the first matching row).
  ----------------------------------------------------------------------------
  SELECT
    i.*,
    p.zc_pdcatv AS matched_zc_pdcatv,
    ROW_NUMBER() OVER (
      PARTITION BY i.vkorg_auft,
                   i.spara,
                   i.vtweg,
                   i.kunag,
                   i.pkunwe,
                   i.matnr,
                   i.fkdat,
                   i.prodh,
                   i.zzpdflag
      ORDER BY p.prodh -- arbitrary tie-breaker for "EXIT" on first encountered match
    ) AS rn
  FROM routine_00O2TKW6BKZKAVEJ4NRHAYFH6_input i
  LEFT JOIN routine_00O2TKW6BKZKAVEJ4NRHAYFH6_pddet p
    ON i.zzpdflag <> ''  -- only attempt a lookup when zzpdflag is not initial
    AND p.salesorg        = i.vkorg_auft
    AND p.division        = i.spara
    AND p.distr_chan      = i.vtweg
    AND (p.sold_to        = i.kunag  OR p.sold_to  = '')
    AND (p.ship_to        = i.pkunwe OR p.ship_to  = '')
    AND (p.zc_matnr       = i.matnr  OR p.zc_matnr = '')
    AND p.datefrom       <= i.fkdat
    AND p.dateto         >= i.fkdat
    AND (
         p.prodh = '' 
         OR (
             -- If lt_pddet.prodh has '+' or '*', do a pattern-like check (ABAP CA/CP).
             (INSTR(p.prodh, '+') > 0 OR INSTR(p.prodh, '*') > 0)
             AND p.prodh LIKE REPLACE(REPLACE(i.prodh, '*', '%'), '+', '_')
            )
         OR (
             -- Otherwise, do an exact match.
             (INSTR(p.prodh, '+') = 0 AND INSTR(p.prodh, '*') = 0)
             AND p.prodh = i.prodh
            )
        )
),
routine_00O2TKW6BKZKAVEJ4NRHAYFH6 AS (
  ----------------------------------------------------------------------------
  -- Final CTE applying the ABAP routine's output fields:
  --
  -- 1) &RS& is set to the first matching pddet.zc_pdcatv (if any).
  --    Emulated by picking the row where rn=1. Otherwise &RS& stays NULL.
  -- 2) &RE& is forced to 0 (do not skip this record).
  -- 3) &AB& is forced to 0 (do not abort the data package).
  ----------------------------------------------------------------------------
  SELECT
    j.*,
    CASE 
      WHEN j.rn = 1 THEN j.matched_zc_pdcatv 
      ELSE NULL 
    END AS rs,
    0 AS re,
    0 AS ab
  FROM routine_00O2TKW6BKZKAVEJ4NRHAYFH6_joined j
)