WITH routine_00O2TKW6BKZKAVEJ4NRHAYYFU_lt_pddet AS (
  ------------------------------------------------------------------------------
  -- This CTE represents the lookup table lt_pddet from the ABAP code.
  -- In ABAP, we LOOP AT lt_pddet with various WHERE conditions. Here,
  -- we simply expose all columns assumed to exist in lt_pddet so we can
  -- later JOIN to replicate the “loop” lookup logic set-based.
  -- (salesorg, division, distr_chan, sold_to, ship_to, /bic/zc_matnr,
  --  datefrom, dateto, prodh, /bic/zc_pdcatu)
  ------------------------------------------------------------------------------
  SELECT
    salesorg,
    division,
    distr_chan,
    sold_to,
    ship_to,
    `/bic/zc_matnr` AS zc_matnr,
    datefrom,
    dateto,
    prodh,
    `/bic/zc_pdcatu` AS zc_pdcatu
  FROM `your_dataset.lt_pddet`  -- Replace with the actual table for lt_pddet
),

routine_00O2TKW6BKZKAVEJ4NRHAYYFU_filter_input AS (
  ------------------------------------------------------------------------------
  -- This CTE loads data from the routine input table 2LIS_13_VDITM.
  -- In ABAP, &TS& references come from the input record (2LIS_13_VDITM).
  -- Here we keep all columns and add a helper flag (zzpdflag_not_empty)
  -- to indicate whether &TS&-zzpdflag is initial or not, mirroring the
  -- IF &TS&-zzpdflag IS NOT INITIAL condition.
  ------------------------------------------------------------------------------
  SELECT
    t.*,
    CASE 
      WHEN COALESCE(TRIM(t.zzpdflag), '') <> '' THEN 1
      ELSE 0
    END AS zzpdflag_not_empty
  FROM `your_dataset.2LIS_13_VDITM` t
),

routine_00O2TKW6BKZKAVEJ4NRHAYYFU_joined AS (
  ------------------------------------------------------------------------------
  -- This CTE replicates the LOOP AT lt_pddet with WHERE conditions:
  --  - salesorg = vkorg_auft
  --  - division = spara
  --  - distr_chan = vtweg
  --  - (sold_to = kunag OR sold_to = '')
  --  - (ship_to = pkunwe OR ship_to = '')
  --  - (/bic/zc_matnr = matnr OR /bic/zc_matnr = '')
  --  - datefrom <= fkdat <= dateto
  --
  -- Additionally, it handles the further check on prodh:
  --  IF ls_pddet-prodh IS NOT INITIAL.
  --    IF ls_pddet-prodh CA '+*' => pattern match (CP).
  --    ELSE => exact match.
  --
  -- We use a LEFT JOIN so that rows in the input can still appear even
  -- if no match is found in lt_pddet. Then we apply a ROW_NUMBER window
  -- so we can pick the first matching row, mirroring the “EXIT” in ABAP.
  ------------------------------------------------------------------------------
  SELECT
    f.*,
    p.zc_pdcatu,
    ROW_NUMBER() OVER (
       PARTITION BY
         f.zzpdflag_not_empty,
         f.vkorg_auft,
         f.spara,
         f.vtweg,
         f.kunag,
         f.pkunwe,
         f.matnr,
         f.fkdat,
         f.prodh
       ORDER BY
         p.salesorg,
         p.division,
         p.distr_chan,
         p.sold_to,
         p.ship_to,
         p.zc_matnr,
         p.datefrom,
         p.dateto,
         p.prodh
    ) AS rn
  FROM routine_00O2TKW6BKZKAVEJ4NRHAYYFU_filter_input f
  LEFT JOIN routine_00O2TKW6BKZKAVEJ4NRHAYYFU_lt_pddet p
    ON f.zzpdflag_not_empty = 1
       AND p.salesorg = f.vkorg_auft
       AND p.division = f.spara
       AND p.distr_chan = f.vtweg
       AND (p.sold_to = f.kunag OR p.sold_to = '')
       AND (p.ship_to = f.pkunwe OR p.ship_to = '')
       AND (p.zc_matnr = f.matnr OR p.zc_matnr = '')
       AND p.datefrom <= f.fkdat
       AND p.dateto   >= f.fkdat
       AND (
           -- Replicating: IF ls_pddet-prodh IS NOT INITIAL ...
           p.prodh IS NULL
           OR p.prodh = ''
           OR (
               CASE
                 -- IF ls_pddet-prodh CA '+*' => do pattern match
                 WHEN REGEXP_CONTAINS(p.prodh, r'(\+|\*)') THEN
                   CASE
                     -- Replicate "CHECK ls_pddet-prodh CP &TS&-prodh" by
                     -- converting p.prodh into a SQL LIKE pattern, and verifying
                     -- f.prodh matches that pattern
                     WHEN f.prodh LIKE REGEXP_REPLACE(
                                        REGEXP_REPLACE(p.prodh, r'\*', '%'),
                                        r'\+', '_'
                                      )
                     THEN 1
                     ELSE 0
                   END
                 -- ELSE => check equality
                 ELSE CASE WHEN p.prodh = f.prodh THEN 1 ELSE 0 END
               END
             ) = 1
         )
),

routine_00O2TKW6BKZKAVEJ4NRHAYYFU AS (
  ------------------------------------------------------------------------------
  -- This final CTE picks only the first valid “lt_pddet” match per input row
  -- by keeping rows with rn=1 or no match (rn IS NULL). That replicates the
  -- ABAP LOOP/EXIT logic for the first found entry. Then it assigns:
  --   &RS& = ls_pddet-/bic/zc_pdcatu (zc_pdcatu here) if found
  --   &RE& = 0
  --   &AB& = 0
  ------------------------------------------------------------------------------
  SELECT
    j.*,
    CASE WHEN j.rn = 1 THEN j.zc_pdcatu ELSE NULL END AS result_zc_pdcatu,
    0 AS returncode,   -- &RE& = 0
    0 AS abort         -- &AB& = 0
  FROM routine_00O2TKW6BKZKAVEJ4NRHAYYFU_joined j
  WHERE j.rn = 1
     OR j.rn IS NULL
);