WITH
-- [ Step 1: Input Rows ]
-- This CTE corresponds to reading from the 2LIS_13_VDITM (Data Source) table in ABAP. 
-- We select all fields and additionally assign a row number (RN) to simulate &RN& for logging purposes.
routine_00O2TKW6BKZKAVEJ4NRHB0VYY_Step1 AS (
    SELECT
        t.*,
        ROW_NUMBER() OVER (ORDER BY t.VKORG) AS RN
    FROM
        2LIS_13_VDITM t
),

-- [ Step 2: Retrieve ZC_ICGRP ]
-- This CTE mirrors the ABAP logic of first checking an internal cache “it_salesorg” and, if not found, 
-- selecting from /BI0/PSALESORG with OBJVERS = 'A'. In a set-based SQL approach, we simply LEFT JOIN 
-- to /BI0/PSALESORG to obtain /BIC/ZC_ICGRP (mapped to &RS& in ABAP).
routine_00O2TKW6BKZKAVEJ4NRHB0VYY_Step2 AS (
    SELECT
        s1.*,
        ps./BIC/ZC_ICGRP AS RS
    FROM
        routine_00O2TKW6BKZKAVEJ4NRHB0VYY_Step1 s1
    LEFT JOIN
        /BI0/PSALESORG ps
    ON
        s1.VKORG = ps.SALESORG
        AND ps.OBJVERS = 'A'
),

-- [ Final Step: Error Handling and Return Codes ]
-- This CTE implements the ABAP conditions:
-- - If RS is initial (NULL), set &RE&=4, fill error log fields (MSG*), and skip the record.
-- - Otherwise set &RE&=0, &AB&=0 (proceed).
-- This completes the logic of the routine.
routine_00O2TKW6BKZKAVEJ4NRHB0VYY AS (
    SELECT
        s2.*,
        CASE WHEN s2.RS IS NULL THEN 4 ELSE 0 END AS RE,
        CASE WHEN s2.RS IS NULL THEN 0 ELSE 0 END AS AB,
        CASE WHEN s2.RS IS NULL THEN 'E' END AS MSGTY,
        CASE WHEN s2.RS IS NULL THEN 'ZBW' END AS MSGID,
        CASE WHEN s2.RS IS NULL THEN '040' END AS MSGNO,
        CASE WHEN s2.RS IS NULL THEN s2.VKORG END AS MSGV1
    FROM
        routine_00O2TKW6BKZKAVEJ4NRHB0VYY_Step2 s2
)