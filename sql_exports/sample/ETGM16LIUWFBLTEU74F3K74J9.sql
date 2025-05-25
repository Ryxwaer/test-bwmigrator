WITH routine_ETGM16LIUWFBLTEU74F3K74J9_input AS (
  ------------------------------------------------------------------------------
  -- 1) Read all rows from the BW source table (ZSA_P_V2LIS_13_VDITM), 
  --    simulating the internal table of the ABAP routine. 
  --    We add a row_id to help partition and replicate "LOOP AT" behavior later.
  ------------------------------------------------------------------------------
  SELECT
    ROW_NUMBER() OVER() AS row_id,
    T.*
  FROM `ZSA_P_V2LIS_13_VDITM` T
),
routine_ETGM16LIUWFBLTEU74F3K74J9_join_loop AS (
  ------------------------------------------------------------------------------
  -- 2) Replicate the ABAP "LOOP AT it_promot WHERE ..." logic via a self-join.
  --    For each "input" row (i), find matching "promot" row (p) where:
  --      p.salesorg = i.vkorg
  --      p.datefrom <= i.fkdat
  --      p.dateto   >= i.fkdat
  --    Use ROW_NUMBER to simulate that multiple matches could occur, 
  --    and we will pick the "last" one in the next step (mirroring final overwrite in ABAP).
  ------------------------------------------------------------------------------
  SELECT
    i.row_id,
    i.salesorg AS i_salesorg,
    i.datefrom AS i_datefrom,
    i.dateto   AS i_dateto,
    i.vkorg,
    i.fkdat,
    p."/BIC/ZC_PROMOT" AS matched_promot,
    ROW_NUMBER() OVER(
      PARTITION BY i.row_id
      ORDER BY p.datefrom
    ) AS joined_rn
  FROM routine_ETGM16LIUWFBLTEU74F3K74J9_input i
  JOIN routine_ETGM16LIUWFBLTEU74F3K74J9_input p
    ON p.salesorg = i.vkorg
    AND p.datefrom <= i.fkdat
    AND p.dateto   >= i.fkdat
),
routine_ETGM16LIUWFBLTEU74F3K74J9 AS (
  ------------------------------------------------------------------------------
  -- 3) For each input row, retrieve the "last" matched_promot based on the highest 
  --    joined_rn (simulating the final value of &RS& in ABAP after the loop).
  --    Also set &RE& = 0 and &AB& = 0 to signal no record/package skipping.
  ------------------------------------------------------------------------------
  SELECT
    jl.row_id,
    jl.i_salesorg,
    jl.i_datefrom,
    jl.i_dateto,
    jl.vkorg,
    jl.fkdat,
    CASE 
      WHEN jl.joined_rn = mx.max_joined_rn THEN jl.matched_promot 
    END AS RS,
    0 AS RE,
    0 AS AB
  FROM routine_ETGM16LIUWFBLTEU74F3K74J9_join_loop jl
  JOIN (
    SELECT
      row_id,
      MAX(joined_rn) AS max_joined_rn
    FROM routine_ETGM16LIUWFBLTEU74F3K74J9_join_loop
    GROUP BY row_id
  ) mx
  ON jl.row_id = mx.row_id
)