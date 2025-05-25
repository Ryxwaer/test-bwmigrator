WITH routine_00O2TKW6BKZKAVEJ4NRHB0D0A_input AS (
  -------------------------------------------------------------------------------------------------------------------
  -- 1) This CTE represents the ABAP “routine input table” (2LIS_13_VDITM).
  --    In ABAP, “&TS&-vkorg” and “&TS&-fkdat” come from this input structure.
  --    We simply select the relevant columns (VKORG, FKDAT, etc.) as the source data.
  -------------------------------------------------------------------------------------------------------------------
  SELECT
    VKORG,
    FKDAT
    -- include any other needed columns from 2LIS_13_VDITM
  FROM 2LIS_13_VDITM
),
routine_00O2TKW6BKZKAVEJ4NRHB0D0A_promot AS (
  -------------------------------------------------------------------------------------------------------------------
  -- 2) This CTE corresponds to “it_promot” in ABAP. 
  --    The ABAP code loops over this table using “WHERE salesorg = &TS&-vkorg 
  --    AND datefrom <= &TS&-fkdat AND dateto >= &TS&-fkdat”, then sets &RS& = /BIC/ZC_PROMOT.
  --    Here we expose those columns for the join in the next step.
  -------------------------------------------------------------------------------------------------------------------
  SELECT
    salesorg,
    datefrom,
    dateto,
    "/BIC/ZC_PROMOT" AS zc_promot
  FROM it_promot  -- assume the actual promotions table is named "it_promot"
),
routine_00O2TKW6BKZKAVEJ4NRHB0D0A_joined AS (
  -------------------------------------------------------------------------------------------------------------------
  -- 3) This CTE replicates the ABAP loop condition by performing the join:
  --    it_promot.salesorg = &TS&-vkorg
  --    it_promot.datefrom <= &TS&-fkdat
  --    it_promot.dateto   >= &TS&-fkdat
  --    All matching rows are returned, similar to how the ABAP loop iterates over every match.
  -------------------------------------------------------------------------------------------------------------------
  SELECT
    i.VKORG,
    i.FKDAT,
    p.zc_promot,
    p.datefrom,
    p.dateto
  FROM routine_00O2TKW6BKZKAVEJ4NRHB0D0A_input i
  LEFT JOIN routine_00O2TKW6BKZKAVEJ4NRHB0D0A_promot p
    ON i.VKORG = p.salesorg
   AND i.FKDAT >= p.datefrom
   AND i.FKDAT <= p.dateto
),
routine_00O2TKW6BKZKAVEJ4NRHB0D0A_ranked AS (
  -------------------------------------------------------------------------------------------------------------------
  -- 4) In ABAP, the loop overwrites &RS& with the last matching record. 
  --    We replicate this with a window function:
  --    - Partition by the input row (VKORG, FKDAT).
  --    - Order by datefrom/ dateto descending so that the final row in ABAP is the row_number = 1 here.
  -------------------------------------------------------------------------------------------------------------------
  SELECT
    VKORG,
    FKDAT,
    zc_promot,
    ROW_NUMBER() OVER (
      PARTITION BY VKORG, FKDAT
      ORDER BY datefrom DESC, dateto DESC
    ) AS rn
  FROM routine_00O2TKW6BKZKAVEJ4NRHB0D0A_joined
),
routine_00O2TKW6BKZKAVEJ4NRHB0D0A AS (
  -------------------------------------------------------------------------------------------------------------------
  -- 5) This final CTE sets:
  --    - &RS& to the promotion of the “last” matching row (where rn=1).
  --    - &RE& = 0 and &AB& = 0 (ABAP signals to not skip or abort).
  --    This completes the ABAP routine logic in a set-based manner.
  -------------------------------------------------------------------------------------------------------------------
  SELECT
    VKORG,
    FKDAT,
    CASE WHEN rn = 1 THEN zc_promot END AS RS,
    0 AS RE,
    0 AS AB
  FROM routine_00O2TKW6BKZKAVEJ4NRHB0D0A_ranked
)