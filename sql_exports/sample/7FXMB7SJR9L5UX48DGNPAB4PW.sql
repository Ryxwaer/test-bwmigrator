WITH 
--------------------------------------------------------------------------------
-- [routine_7FXMB7SJR9L5UX48DGNPAB4PW_step_1]:
-- 1) Maps to "READ TABLE it_soursystem ... WITH KEY rnr = &TS&-REQUEST_."
-- 2) For each row in ZSA_P_V2LIS_13_VDITM, attempt to find a matching row in IT_SOURSYSTEM by RNR (REQUEST_).
-- 3) Uses a LEFT JOIN for a lookup; no aggregation or ranking.
--------------------------------------------------------------------------------
routine_7FXMB7SJR9L5UX48DGNPAB4PW_step_1 AS (
  SELECT 
    t.*,
    s.rnr      AS s_rnr,
    s.logsys   AS s_logsys,
    s.soursysid AS s_soursysid
  FROM ZSA_P_V2LIS_13_VDITM AS t
  LEFT JOIN it_soursystem AS s
    ON s.rnr = t.REQUEST_
),

--------------------------------------------------------------------------------
-- [routine_7FXMB7SJR9L5UX48DGNPAB4PW_step_2]:
-- 1) Maps to "SELECT SINGLE ... FROM rssoursystem AS a INNER JOIN rsseldone AS b ON a~logsys = b~logsys WHERE b~rnr = &TS&-REQUEST_" when the record was not found in IT_SOURSYSTEM.
-- 2) If s_rnr is NULL, we retrieve matching data from rssoursystem + rsseldone. Otherwise, keep existing it_soursystem data.
-- 3) Uses a JOIN for this lookup; no aggregation or ranking.
--------------------------------------------------------------------------------
routine_7FXMB7SJR9L5UX48DGNPAB4PW_step_2 AS (
  SELECT
    st1.*,
    sub.found_rnr,
    sub.found_logsys,
    sub.found_soursysid
  FROM routine_7FXMB7SJR9L5UX48DGNPAB4PW_step_1 st1
  LEFT JOIN (
    SELECT 
      b.rnr            AS found_rnr,
      a.logsys         AS found_logsys,
      a.soursysid      AS found_soursysid
    FROM rssoursystem AS a
    JOIN rsseldone    AS b
      ON a.logsys = b.logsys
  ) sub
    ON sub.found_rnr = st1.REQUEST_
),

--------------------------------------------------------------------------------
-- [routine_7FXMB7SJR9L5UX48DGNPAB4PW_step_3]:
-- 1) Maps to "if not sy-subrc is initial ... insert wa_soursystem" and then checks "if not wa_soursystem-soursysid is initial then &RS& = soursysid."
-- 2) Determines the final RNR, LOGSYS, and SOURSYSID by coalescing IT_SOURSYSTEM data or newly found data; prepares an &RS& value.
-- 3) No direct join here, just CASE/COALESCE for transformations.
--------------------------------------------------------------------------------
routine_7FXMB7SJR9L5UX48DGNPAB4PW_step_3 AS (
  SELECT
    st2.*,
    CASE 
      WHEN st2.s_rnr IS NOT NULL 
      THEN st2.s_rnr 
      ELSE st2.found_rnr 
    END AS rnr_final,
    CASE 
      WHEN st2.s_logsys IS NOT NULL 
      THEN st2.s_logsys 
      ELSE st2.found_logsys 
    END AS logsys_final,
    CASE 
      WHEN st2.s_soursysid IS NOT NULL 
      THEN st2.s_soursysid 
      ELSE st2.found_soursysid 
    END AS soursysid_final
  FROM routine_7FXMB7SJR9L5UX48DGNPAB4PW_step_2 st2
),

--------------------------------------------------------------------------------
-- [routine_7FXMB7SJR9L5UX48DGNPAB4PW]:
-- 1) Maps to setting "&RS& = wa_soursystem-soursysid," "&RE& = 0," and "&AB& = 0."
-- 2) Final assignment of RS, RE, and AB output fields (no filtering or join).
-- 3) Straightforward column additions.
--------------------------------------------------------------------------------
routine_7FXMB7SJR9L5UX48DGNPAB4PW AS (
  SELECT
    st3.*,
    CASE 
      WHEN st3.soursysid_final IS NOT NULL 
      THEN st3.soursysid_final 
      ELSE NULL 
    END AS rs_field,
    0 AS re_field,
    0 AS ab_field
  FROM routine_7FXMB7SJR9L5UX48DGNPAB4PW_step_3 st3
);