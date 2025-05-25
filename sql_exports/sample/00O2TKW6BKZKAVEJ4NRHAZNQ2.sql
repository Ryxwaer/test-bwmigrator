WITH routine_00O2TKW6BKZKAVEJ4NRHAZNQ2_base AS (
    --------------------------------------------------------------------------------
    -- This CTE corresponds to the initial data read from the source table 2LIS_13_VDITM.
    -- It prepares the fields that are passed to the ZBW_CUSTSL function (i_sold_to, i_ship_to, i_payer, i_bus, i_vkorg, i_date)
    -- and includes the additional field zzkunnr_yo for the final override check.
    -- No joins or aggregations are performed here; it's a simple selection of the relevant columns.
    --------------------------------------------------------------------------------
    SELECT
      pkunwe AS i_ship_to,
      kunag  AS i_sold_to,
      kunrg  AS i_payer,
      h_bus  AS i_bus,
      vkorg  AS i_vkorg,
      fkdat  AS i_date,
      zzkunnr_yo
    FROM 2LIS_13_VDITM
),
routine_00O2TKW6BKZKAVEJ4NRHAZNQ2_pcust_join AS (
    --------------------------------------------------------------------------------
    -- This CTE replicates the "SELECT SINGLE /bic/zc_cln05 FROM /bi0/pcustomer" lookup
    -- for each row in the base data. We do a LEFT JOIN on /bi0/pcustomer matching customer
    -- to i_ship_to and objvers = 'A'. The retrieved field /bic/zc_cln05 is stored as lv_busunit,
    -- and sy_subrc is simulated by checking if a matching row was found.
    -- No aggregations are done; it's purely a lookup join.
    --------------------------------------------------------------------------------
    SELECT
      b.*,
      p./bic/zc_cln05 AS lv_busunit,
      CASE WHEN p.customer IS NOT NULL THEN 0 ELSE 4 END AS sy_subrc
    FROM routine_00O2TKW6BKZKAVEJ4NRHAZNQ2_base b
    LEFT JOIN /bi0/pcustomer p
      ON p.customer = b.i_ship_to
     AND p.objvers = 'A'
),
routine_00O2TKW6BKZKAVEJ4NRHAZNQ2_fn_zbw_custsl AS (
    --------------------------------------------------------------------------------
    -- This CTE implements the logic of FUNCTION ZBW_CUSTSL exactly:
    -- 1) If i_date >= '20220101':
    --      a) If i_vkorg in {1640,1642,1010,0800,0530}, e_result = i_sold_to.
    --      b) Otherwise, if the joined record (sy_subrc=0) was found:
    --         - If lv_busunit = 'B2B', e_result = i_sold_to,
    --           else e_result = i_ship_to.
    --         If no record was found, e_result remains NULL by default
    --         (the original ABAP code does not specify a fallback).
    -- 2) Else (i_date < '20220101'):
    --      a) If i_vkorg in {1640,1642,1010,0800,0530}, e_result = i_sold_to.
    --      b) Otherwise:
    --         - If i_bus='01', e_result = i_sold_to
    --         - If i_bus='02', e_result = i_ship_to
    --         - Else e_result = i_sold_to.
    -- 3) e_returncode=0, e_abort=0 are assigned at the end.
    -- No aggregations; all logic is handled with CASE expressions.
    --------------------------------------------------------------------------------
    SELECT
      *,
      CASE 
        WHEN i_date >= '20220101' THEN 
          CASE 
            WHEN i_vkorg IN ('1640','1642','1010','0800','0530') THEN i_sold_to
            ELSE
              CASE
                WHEN sy_subrc = 0 AND lv_busunit = 'B2B' THEN i_sold_to
                WHEN sy_subrc = 0 AND lv_busunit IS NOT NULL AND lv_busunit <> 'B2B' THEN i_ship_to
                ELSE NULL 
              END
          END
        ELSE
          CASE 
            WHEN i_vkorg IN ('1640','1642','1010','0800','0530') THEN i_sold_to
            ELSE
              CASE
                WHEN i_bus = '01' THEN i_sold_to
                WHEN i_bus = '02' THEN i_ship_to
                ELSE i_sold_to
              END
          END
      END AS tmp_result,
      0 AS e_returncode,
      0 AS e_abort
    FROM routine_00O2TKW6BKZKAVEJ4NRHAZNQ2_pcust_join
),
routine_00O2TKW6BKZKAVEJ4NRHAZNQ2 AS (
    --------------------------------------------------------------------------------
    -- This final CTE applies the additional override from the ABAP routine:
    --  If zzkunnr_yo <> '' AND i_vkorg = '1157', then e_result should be zzkunnr_yo
    --  (overriding the e_result from the function ZBW_CUSTSL).
    --  Otherwise, e_result remains the outcome of tmp_result.
    -- No join or aggregation takes place here.
    --------------------------------------------------------------------------------
    SELECT
      *,
      CASE
        WHEN zzkunnr_yo IS NOT NULL AND zzkunnr_yo <> '' AND i_vkorg = '1157'
          THEN zzkunnr_yo
        ELSE tmp_result
      END AS e_result
    FROM routine_00O2TKW6BKZKAVEJ4NRHAZNQ2_fn_zbw_custsl
)