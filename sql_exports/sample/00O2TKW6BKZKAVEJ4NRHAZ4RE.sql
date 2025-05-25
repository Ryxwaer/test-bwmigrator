WITH routine_00O2TKW6BKZKAVEJ4NRHAZ4RE_input AS (
    --------------------------------------------------------------------------------
    -- This block gathers all relevant fields from 2LIS_13_VDITM (the source table).
    -- In ABAP, this corresponds to reading &TS& structure from the DataSource.
    -- No filtering here other than selecting the columns needed for subsequent steps.
    -- No join or aggregation is done at this point.
    --------------------------------------------------------------------------------
    SELECT
        vkorg_auft,        -- &TS&-vkorg_auft
        spara,             -- &TS&-spara
        vtweg,             -- &TS&-vtweg
        kunag,             -- &TS&-kunag
        pkunwe,            -- &TS&-pkunwe
        matnr,             -- &TS&-matnr
        fkdat,             -- &TS&-fkdat
        prodh,             -- &TS&-prodh
        zzpdflag           -- &TS&-zzpdflag
    FROM 2LIS_13_VDITM
),
routine_00O2TKW6BKZKAVEJ4NRHAZ4RE_pddet AS (
    --------------------------------------------------------------------------------
    -- This block represents the lt_pddet in ABAP.
    -- In the original ABAP routine, we loop over lt_pddet to find matching records.
    -- Here, we simply select all columns needed for the subsequent join/lookups.
    -- No join or filtering is done here yet, just a direct read of lt_pddet.
    --------------------------------------------------------------------------------
    SELECT
        salesorg,
        division,
        distr_chan,
        sold_to,
        ship_to,
        /bic/zc_matnr       AS zc_matnr,
        datefrom,
        dateto,
        prodh,
        /bic/zc_pdcata      AS zc_pdcata
    FROM lt_pddet
),
routine_00O2TKW6BKZKAVEJ4NRHAZ4RE_joined AS (
    --------------------------------------------------------------------------------
    -- This block performs the equivalent of the ABAP LOOP with WHERE conditions.
    --  1) Only rows where input.zzpdflag is NOT INITIAL (non-empty) will match lt_pddet.
    --  2) The join conditions replicate all WHERE criteria used in the ABAP:
    --     - Matching salesorg, division, distr_chan
    --     - Matching sold_to and ship_to to either the actual or ''
    --     - Matching zc_matnr to either matnr or ''
    --     - Matching date ranges (datefrom <= fkdat <= dateto)
    --  3) For prodh checks:
    --     - If pddet.prodh is '', no check is required (as in ABAP IF prodh IS INITIAL).
    --     - Else if pddet.prodh contains '+' or '*', use LIKE (CP in ABAP)
    --     - Else use an equality check (= in ABAP).
    -- This step uses a LEFT JOIN to preserve all input rows and to allow picking the
    -- first match in subsequent logic. The ROW_NUMBER function is used to emulate 
    -- the ABAP "LOOP ... EXIT" after the first qualifying record.
    --------------------------------------------------------------------------------
    SELECT
        i.vkorg_auft,
        i.spara,
        i.vtweg,
        i.kunag,
        i.pkunwe,
        i.matnr,
        i.fkdat,
        i.prodh,
        i.zzpdflag,
        p.zc_pdcata,
        ROW_NUMBER() OVER (
            PARTITION BY 
               i.vkorg_auft, i.spara, i.vtweg, 
               i.kunag, i.pkunwe, i.matnr, i.fkdat, i.prodh, i.zzpdflag
            ORDER BY 
               p.salesorg, p.division, p.distr_chan, p.prodh
        ) AS rn
    FROM routine_00O2TKW6BKZKAVEJ4NRHAZ4RE_input i
    LEFT JOIN routine_00O2TKW6BKZKAVEJ4NRHAZ4RE_pddet p
      ON i.zzpdflag <> ''                                -- IF &TS&-zzpdflag IS NOT INITIAL
     AND p.salesorg      = i.vkorg_auft
     AND p.division      = i.spara
     AND p.distr_chan    = i.vtweg
     AND (p.sold_to      = i.kunag  OR p.sold_to      = '')
     AND (p.ship_to      = i.pkunwe OR p.ship_to      = '')
     AND (p.zc_matnr     = i.matnr  OR p.zc_matnr     = '')
     AND p.datefrom     <= i.fkdat
     AND p.dateto       >= i.fkdat
     AND (
          p.prodh = ''
          OR (
             (INSTR(p.prodh, '+') > 0 OR INSTR(p.prodh, '*') > 0)
             AND p.prodh LIKE i.prodh
          )
          OR (
             (INSTR(p.prodh, '+') = 0 AND INSTR(p.prodh, '*') = 0)
             AND p.prodh = i.prodh
          )
     )
),
routine_00O2TKW6BKZKAVEJ4NRHAZ4RE AS (
    --------------------------------------------------------------------------------
    -- This block finalizes the logic by applying the "EXIT after first match" concept:
    --   - We filter down to rn=1, meaning we pick the first matching lt_pddet record.
    --   - If no match exists, all p.* columns remain NULL, effectively leaving RS empty.
    --   - We set RE=0 and AB=0 for all rows, matching the ABAP code:
    --        &RE& = 0 (do not skip record)
    --        &AB& = 0 (do not abort the package)
    -- The field RS corresponds to &RS& = ls_pddet-/bic/zc_pdcata if a match was found.
    --------------------------------------------------------------------------------
    SELECT
        j.vkorg_auft,
        j.spara,
        j.vtweg,
        j.kunag,
        j.pkunwe,
        j.matnr,
        j.fkdat,
        j.prodh,
        j.zzpdflag,
        CASE 
            WHEN j.zzpdflag <> '' THEN j.zc_pdcata
            ELSE NULL
        END AS RS,
        0 AS RE,
        0 AS AB
    FROM routine_00O2TKW6BKZKAVEJ4NRHAZ4RE_joined j
    WHERE j.rn = 1 
       OR j.rn IS NULL  -- keeps rows with no matching lt_pddet so we don't lose them
)