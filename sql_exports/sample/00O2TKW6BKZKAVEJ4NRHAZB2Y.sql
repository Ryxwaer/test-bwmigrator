WITH routine_00O2TKW6BKZKAVEJ4NRHAZB2Y_input AS (
  /* ---------------------------------------------------------------------------
     Maps to the initial data read from 2LIS_13_VDITM in the ABAP routine.
     Here we simply select all rows from the source and add a surrogate key
     (row_id) to emulate per-record processing later, similar to the ABAP loop.
     No filtering or further transformation is performed yet.
  --------------------------------------------------------------------------- */
  SELECT
    t.*,
    GENERATE_UUID() AS row_id
  FROM 2LIS_13_VDITM t
),

routine_00O2TKW6BKZKAVEJ4NRHAZB2Y_joined AS (
  /* ---------------------------------------------------------------------------
     Corresponds to the ABAP LOOP over lt_pddet with multiple WHERE conditions.
     We do a LEFT JOIN to "pddet" (the equivalent of lt_pddet) so that every
     record from 2LIS_13_VDITM is retained. The join conditions replicate:
       - IF &TS&-zzpdflag IS NOT INITIAL     =>  (i.zzpdflag <> '')
       - salesorg   = vkorg_auft
       - division   = spara
       - distr_chan = vtweg
       - sold_to    = kunag OR ''
       - ship_to    = pkunwe OR ''
       - /bic/zc_matnr = matnr OR ''
       - datefrom  <= fkdat
       - dateto    >= fkdat
     If zzpdflag is blank, the join will yield NULL pddet columns (like no match),
     matching the ABAP logic that the PD detail lookup is skipped.
  --------------------------------------------------------------------------- */
  SELECT
    i.row_id,
    i.zzpdflag,
    i.vkorg_auft,
    i.spara,
    i.vtweg,
    i.kunag,
    i.pkunwe,
    i.matnr,
    i.fkdat,
    i.prodh,
    -- All other columns from 2LIS_13_VDITM as needed...
    p.salesorg,
    p.division,
    p.distr_chan,
    p.sold_to,
    p.ship_to,
    p.datefrom,
    p.dateto,
    p.prodh        AS p_prodh,
    p./bic/zc_matnr AS p_zc_matnr,
    p./bic/zc_pdcati AS p_zc_pdcati
  FROM routine_00O2TKW6BKZKAVEJ4NRHAZB2Y_input i
  LEFT JOIN pddet p
    ON i.zzpdflag <> ''
   AND p.salesorg       = i.vkorg_auft
   AND p.division       = i.spara
   AND p.distr_chan     = i.vtweg
   AND (p.sold_to       = i.kunag   OR p.sold_to       = '')
   AND (p.ship_to       = i.pkunwe  OR p.ship_to       = '')
   AND (p_zc_matnr      = i.matnr   OR p_zc_matnr      = '')
   AND p.datefrom      <= i.fkdat
   AND p.dateto        >= i.fkdat
),

routine_00O2TKW6BKZKAVEJ4NRHAZB2Y_filtered AS (
  /* ---------------------------------------------------------------------------
     Implements the ABAP CHECK statements for pddet.prodh:
        1) If p_prodh is empty, do not filter it out (accept immediately).
        2) If p_prodh contains '+' or '*', then do an ABAP 'CP' check, here
           approximated with a regex transformation:
             - '+' translates to a single-character wildcard.
             - '*' translates to a multi-character wildcard.
        3) Otherwise, compare equality p_prodh = i.prodh.
     Any row failing these conditions is filtered out, mirroring ABAP's
     CHECK ... statement skipping the loop iteration.
  --------------------------------------------------------------------------- */
  SELECT
    j.*,
    CASE
      WHEN p_prodh = '' THEN 1
      WHEN REGEXP_CONTAINS(p_prodh, r'[\+\*]') THEN (
        CASE
          /* Convert the ABAP pattern in i.prodh by replacing
             '+' with '.' and '*' with '.*', then apply as a regex. */
          WHEN REGEXP_CONTAINS(
                 p_prodh,
                 CONCAT(
                   '^',
                   REGEXP_REPLACE(
                     REGEXP_REPLACE(j.prodh, r'\+', '.'),
                     r'\*',
                     '.*'
                   ),
                   '$'
                 )
               )
          THEN 1
          ELSE 0
        END
      )
      ELSE
        CASE
          WHEN p_prodh = j.prodh THEN 1
          ELSE 0
        END
    END AS prodh_pass
  FROM routine_00O2TKW6BKZKAVEJ4NRHAZB2Y_joined j
),

routine_00O2TKW6BKZKAVEJ4NRHAZB2Y_ranked AS (
  /* ---------------------------------------------------------------------------
     Here we keep only rows that pass the ABAP CHECK. Then we use ROW_NUMBER()
     to imitate the "EXIT AFTER FIRST MATCH" behavior of the ABAP LOOP.
     The code picks the first matching row from pddet for each input row_id.
  --------------------------------------------------------------------------- */
  SELECT
    f.*,
    ROW_NUMBER() OVER (PARTITION BY f.row_id ORDER BY f.salesorg, f.division, f.distr_chan) AS match_rank
  FROM routine_00O2TKW6BKZKAVEJ4NRHAZB2Y_filtered f
  WHERE f.prodh_pass = 1
),

routine_00O2TKW6BKZKAVEJ4NRHAZB2Y AS (
  /* ---------------------------------------------------------------------------
     Final step replicating:
       - IF no match found, &RS& remains unchanged (NULL here if no prior value).
       - IF match_rank = 1, that is the ABAP "EXIT" condition (first match).
       - &RE& = 0 => do not skip this record.
       - &AB& = 0 => do not abort the data load.
     This CTE would typically be joined back (LEFT JOIN) with the full input
     to ensure every row is retained. We do not produce the final SELECT here
     by requirement, only the CTE. The calling query would select from this CTE
     (or a subsequent join) to obtain the final result (including &RS&, &RE&, &AB&).
  --------------------------------------------------------------------------- */
  SELECT
    i.row_id,
    i.zzpdflag,
    i.vkorg_auft,
    i.spara,
    i.vtweg,
    i.kunag,
    i.pkunwe,
    i.matnr,
    i.fkdat,
    i.prodh,
    -- Other fields from input as needed...
    CASE 
      WHEN r.match_rank = 1 THEN r.p_zc_pdcati
      ELSE NULL
    END AS rs_value,       -- &RS& equivalent
    0 AS re_value,         -- &RE& = 0
    0 AS ab_value          -- &AB& = 0
  FROM routine_00O2TKW6BKZKAVEJ4NRHAZB2Y_input i
  LEFT JOIN routine_00O2TKW6BKZKAVEJ4NRHAZB2Y_ranked r
         ON i.row_id = r.row_id
        AND r.match_rank = 1
);