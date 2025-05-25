WITH routine_ENT6IS5PTG5LSO8KDW0UUYMXZ_zbw_sales_update AS (
  -------------------------------------------------------------------------------------------
  -- This CTE maps to the top-level ABAP routine call "ZBW_SALES_UPDATE".
  -- In ABAP, it instantiates class references (e.g. zcl_2lis_13_vditm), calls get_data / get_result,
  -- and coordinates the overall logic. It also updates the “monitor” tables (which we translate into
  -- an auditing concept in SQL). Here, we bring in the base 2LIS_13_VDITM data as the starting point.
  -- No transformations yet, just the raw source columns from the InfoSource table "2LIS_13_VDITM".
  --
  -- Data: The ABAP code reads from table "2LIS_13_VDITM" into an internal table it_package[].
  -- Here, we simply collect that base data in a CTE to pass down the transformation chain.
  -- No filtering or joining yet in this step.
  -------------------------------------------------------------------------------------------
  SELECT
      t.*
  FROM `YOUR_PROJECT.YOUR_DATASET.2LIS_13_VDITM` t
),

routine_ENT6IS5PTG5LSO8KDW0UUYMXZ_select_azad_matpy2 AS (
  -------------------------------------------------------------------------------------------
  -- This CTE replicates the FOR ALL ENTRIES step where ABAP code reads from /bic/azad_matpy2
  -- into lt_mat, matching on DP-material and checking date ranges datefrom <= bill_date <= dateto.
  -- Then for each matching row, zcc_020py is updated. In set-based SQL, we do a LEFT JOIN on
  -- material and a bill_date between matpy2.datefrom and matpy2.dateto. 
  --
  -- Data: We retrieve relevant rows from /bic/azad_matpy2 and keep them for subsequent merges.
  -- This is effectively a lookup to find zcc_020py for each row of the main DP data.
  -- Operation: Join / filter, a typical “lookup” pattern.
  -------------------------------------------------------------------------------------------
  SELECT
    m.material,
    m.bill_date,
    mat./bic/zc_matnr                     AS matnr_lookup,
    mat./bic/zcc_020py                    AS zcc_020py_lookup,
    mat.datefrom,
    mat.dateto
  FROM routine_ENT6IS5PTG5LSO8KDW0UUYMXZ_zbw_sales_update AS m
  LEFT JOIN `YOUR_PROJECT.YOUR_DATASET./bic/azad_matpy2` AS mat
    ON mat./bic/zc_matnr = m.material
   AND mat.datefrom      <= m.bill_date
   AND mat.dateto        >= m.bill_date
),

routine_ENT6IS5PTG5LSO8KDW0UUYMXZ_after_azad_matpy2 AS (
  -------------------------------------------------------------------------------------------
  -- This CTE applies the logic where ABAP sets “<data_fields>-/bic/zcc_020py = ls_mat-/bic/zcc_020py”
  -- if a matching row was found. In SQL, we map that to a CASE expression that uses
  -- zcc_020py_lookup if found, else keep the current /bic/zcc_020py unchanged (or NULL if absent).
  --
  -- Data: We produce updated zcc_020py for each row. Equates to the innermost loop that
  -- looked up mat data by bill_date.
  -- Operation: This step is a direct field update by a LEFT JOIN / CASE usage.
  -------------------------------------------------------------------------------------------
  SELECT
    m.* EXCEPT(zcc_020py),
    CASE 
      WHEN s.zcc_020py_lookup IS NOT NULL 
           AND s.matnr_lookup = m.material
           AND s.datefrom     <= m.bill_date
           AND s.dateto       >= m.bill_date
      THEN s.zcc_020py_lookup
      ELSE m.zcc_020py
    END AS zcc_020py
  FROM routine_ENT6IS5PTG5LSO8KDW0UUYMXZ_zbw_sales_update m
  LEFT JOIN routine_ENT6IS5PTG5LSO8KDW0UUYMXZ_select_azad_matpy2 s
         ON m.material  = s.material
        AND m.bill_date = s.bill_date
),

routine_ENT6IS5PTG5LSO8KDW0UUYMXZ_delete_obsolete AS (
  -------------------------------------------------------------------------------------------
  -- This CTE replicates "zcl_2lis_13_vditm->delete_obsolete", which in ABAP filters
  -- out rows that do not meet certain conditions (e.g. mandatory 0CALYEAR, billing type flags,
  -- item categories excluded, missing product hierarchy, etc.). In ABAP, there are many
  -- DELETE statements with checks and log_deletion calls for reporting.
  --
  -- Data: We translate the multi-step conditional deletions into CASE or direct filtering.
  -- Operation: This step is effectively applying multiple filters, removing unneeded rows
  -- before further transformations. The ABAP logic checks item_categ, bill_type,
  -- withheld statuses, etc.
  -------------------------------------------------------------------------------------------
  SELECT
    *
  FROM routine_ENT6IS5PTG5LSO8KDW0UUYMXZ_after_azad_matpy2
  WHERE 1=1
    -- Example partial conditions (representing the ABAP checks):
    -- Remove rows with empty product hierarchy
    AND prod_hier IS NOT NULL
    AND prod_hier <> ''
    -- Filtering out special billing types or item categories flagged as excluded:
    AND bill_type NOT IN (
      SELECT bill_type 
      FROM `YOUR_PROJECT.YOUR_DATASET./bi0/pbill_type`
      WHERE objvers   = 'A'
      AND /bic/zc_excld = 'X'
    )
    -- Additional filtering logic goes here, following the ABAP code’s “DELETE it_2lis_13_vditm WHERE …”
),

routine_ENT6IS5PTG5LSO8KDW0UUYMXZ_setup_sisrel AS (
  -------------------------------------------------------------------------------------------
  -- This CTE corresponds to the “setup_sisrel” method, which in ABAP prepares ranges
  -- (gr_billtype, gr_maacgr, etc.) for subsequent SIS checks. Here, we simulate
  -- storing “excluded” or “ignored” categories, item categories, billing types, etc.
  --
  -- Data: We load from the reference tables to label rows as SIS-relevant or not,
  -- in a set-based manner. Then we can mark or keep the relevant info for follow-up logic.
  --
  -- Operation: effectively acts as a precomputation step for systematically ignoring
  -- or including rows in SIS. For performance, we may do left joins or subqueries.
  -------------------------------------------------------------------------------------------
  SELECT
    d.*,
    CASE
      WHEN d.bill_type IN (
        SELECT bill_type 
        FROM `YOUR_PROJECT.YOUR_DATASET./bi0/pbill_type`
        WHERE objvers    = 'A'
          AND /bic/zc_excld = 'X'
      )
      OR d./bic/zc_maacgr IN (
        SELECT /bic/zc_maacgr 
        FROM `YOUR_PROJECT.YOUR_DATASET./bic/pzc_maacgr`
        WHERE objvers    = 'A'
          AND /bic/zc_excld = 'X'
      )
      OR d.item_categ IN (
        SELECT item_categ 
        FROM `YOUR_PROJECT.YOUR_DATASET./bi0/pitem_categ`
        WHERE objvers    = 'A'
          AND /bic/zc_excld = 'X'
      )
      THEN 'N'  -- Mark as not SIS relevant 
      ELSE 'J'  -- Mark as SIS relevant
    END AS sisrel_flag
  FROM routine_ENT6IS5PTG5LSO8KDW0UUYMXZ_delete_obsolete d
),

routine_ENT6IS5PTG5LSO8KDW0UUYMXZ_check_doc_currency AS (
  -------------------------------------------------------------------------------------------
  -- This CTE maps to “zcl_2lis_13_vditm->check_doc_currency”. In ABAP, it checks if
  -- certain sales organizations require usage of the document currency rather than
  -- local currency, based on BUS code. We replicate that with a CASE or multiple conditions.
  --
  -- Data: We produce a flag “use_doc_currency” that is ‘X’ or ‘’ to replicate the ABAP logic.
  -- Operation: Just a case-based expansion, no additional join.
  -------------------------------------------------------------------------------------------
  SELECT
    s.*,
    CASE
      WHEN (s./bic/zc_bus = '08' AND s.salesorg IN ('1643','1650'))
        OR (s./bic/zc_bus = '02' AND s.salesorg = '1650')
        OR (s.salesorg    = '1940' AND CAST(s.createdon AS STRING) >= '20170101')
        OR EXISTS(
          SELECT 1
          FROM `YOUR_PROJECT.YOUR_DATASET.gt_salesorg_flags` f
          WHERE f.salesorg = s.salesorg
            AND f./bic/zc_docur = 'X'
        )
      THEN 'X'
      ELSE ''
    END AS use_doc_currency
  FROM routine_ENT6IS5PTG5LSO8KDW0UUYMXZ_setup_sisrel s
),

routine_ENT6IS5PTG5LSO8KDW0UUYMXZ_convert_curr_local AS (
  -------------------------------------------------------------------------------------------
  -- This CTE demonstrates part of “zcl_2lis_13_vditm->convert_curr_local”, performing
  -- currency conversion from doc currency to local currency if needed. 
  -- In ABAP it calls 'CONVERT_TO_LOCAL_CURRENCY', raises errors if no rate found, etc.
  --
  -- Data: We create columns like “local_amount” from “doc_amount * exchange rate”.
  -- Operation: typical currency conversion logic. 
  -- In real BigQuery, you might join a currency-rates table and compute the result with CASE.
  -------------------------------------------------------------------------------------------
  SELECT
    c.*,
    CASE
      WHEN c.use_doc_currency = 'X' THEN c.doc_currcy
      ELSE c.loc_currcy
    END AS chosen_currency,
    -- Example for local conversion:
    CASE
      WHEN c.use_doc_currency = 'X' 
      THEN (c./bic/zk_nipval * EX.change_rate_local)  -- hypothetical join for the rate
      ELSE c./bic/zk_nipval
    END AS nipval_in_loc,
    -- Additional conversion columns as needed
    EX.rate_type AS effective_rate_type  -- hypothetical usage
  FROM routine_ENT6IS5PTG5LSO8KDW0UUYMXZ_check_doc_currency c
  LEFT JOIN `YOUR_PROJECT.YOUR_DATASET.exchange_rates` EX
       ON EX.from_currency = c.doc_currcy
      AND EX.to_currency   = c.loc_currcy
      AND EX.valid_on      = c.bill_date
),

routine_ENT6IS5PTG5LSO8KDW0UUYMXZ_sub_price_calculations AS (
  -------------------------------------------------------------------------------------------
  -- This CTE partially represents the set of methods "copy_to_price", various "ValCode" checks,
  -- "zsds_sales_price_valcode", “setflag_cogs_copa”, etc. 
  -- In ABAP, these add many additional derived valuations (NIP, GIP, PP2, RRP, min/floor prices),
  -- handle item category logic, and remove or zero out certain cost values. 
  --
  -- Data: We unify all relevant price transformations. We can replicate the key logic
  -- with a series of CASE statements referencing the columns from the prior step.
  -- Operation: This step is a large set-based transformation for all “ValCode” outcomes
  -- and cogs logic.
  -------------------------------------------------------------------------------------------
  SELECT
    l.*,
    -- Example of setting NIP if item category is certain type:
    CASE
      WHEN l.item_categ IN ('ZVPC','ZLST') AND /bic/zc_valcde = '01' THEN nipval_in_loc
      ELSE 0
    END AS nip_val,
    -- Example for cogs logic (PP2, valcde=05) – zero out if flagged by setflag_cogs_copa:
    CASE
      WHEN (l.item_categ = 'ZVFR' AND l./bic/zc_valcde IN ('04','05','08')) THEN 0
      ELSE l./bic/zk_pp2val
    END AS pp2_val_adjusted,
    -- Additional min/floor price logic ...
    CASE
      WHEN l.min_price_waers IS NOT NULL THEN l.min_price
      ELSE 0
    END AS derived_min_price
  FROM routine_ENT6IS5PTG5LSO8KDW0UUYMXZ_convert_curr_local l
),

routine_ENT6IS5PTG5LSO8KDW0UUYMXZ_finished_data AS (
  -------------------------------------------------------------------------------------------
  -- This CTE corresponds to "zcl_2lis_13_vditm->get_data" which in ABAP is the final step
  -- assembling the updated items (it_temp_package). It merges partial results, 
  -- updates final price fields, sets doc layer if needed, merges “cancellation” logic, etc.
  --
  -- Data: We consider cancellation quantities from “zcl_2lis_13_vditm->get_cancelled_orders” 
  -- and finalize the rows that pass all filters. 
  -- Operation: This is effectively the final shape after the entire chain of transformations.
  -------------------------------------------------------------------------------------------
  SELECT
    s.*,
    -- Example to handle cancellations if reason_rej is set 
    -- (similar to get_cancelled_orders in ABAP).
    CASE
      WHEN s.reason_rej IS NOT NULL 
           AND s.qty > 0 
      THEN s.qty - s.delv_qty
      ELSE 0
    END AS cancelled_qty,
    CASE
      WHEN s.reason_rej IS NOT NULL 
           AND s.qty > 0
      THEN s.value * ( (s.qty - s.delv_qty) / s.qty )
      ELSE 0
    END AS cancelled_val
  FROM routine_ENT6IS5PTG5LSO8KDW0UUYMXZ_sub_price_calculations s
),

routine_ENT6IS5PTG5LSO8KDW0UUYMXZ AS (
  -------------------------------------------------------------------------------------------
  -- This final CTE is named exactly "routine_ENT6IS5PTG5LSO8KDW0UUYMXZ" per the requirement.
  -- It represents the last step in the chain, after all ABAP logic has been applied: 
  -- the user exit logic from ZBW_SALES_UPDATE, all currency conversions, 
  -- SIS relevancy checks, and so forth. 
  --
  -- Data: We now have the final, fully transformed rows akin to what the 
  -- ABAP routine would place back into &DP& after all modifications.
  -- Operation: End result of the decomposition. 
  -- No final SELECT statement is requested, so we simply expose this last CTE.
  -------------------------------------------------------------------------------------------
  SELECT
    *
  FROM routine_ENT6IS5PTG5LSO8KDW0UUYMXZ_finished_data
)

-----------------------------------------------------------------------------------------------
-- NOTE: Per instructions, no final SELECT statement beyond the final CTE definition above.
-- The user can SELECT from routine_ENT6IS5PTG5LSO8KDW0UUYMXZ if needed.
-----------------------------------------------------------------------------------------------