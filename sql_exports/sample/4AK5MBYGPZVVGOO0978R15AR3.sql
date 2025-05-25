WITH routine_4AK5MBYGPZVVGOO0978R15AR3_input AS (
  -- This block reads from the DataSource 2LIS_13_VDITM and initializes RS.
  -- It implements ABAP logic: "clear &RS&" and "if &CS&-/BIC/ZC_TRTYPE = 'U1' then &RS& = &CS&-/BIC/ZK_SALESq else 0."
  SELECT
    t.*,
    CASE 
      WHEN t.ZC_TRTYPE = 'U1' THEN t.ZK_SALESq
      ELSE 0 
    END AS RS
  FROM 2LIS_13_VDITM t
),
routine_4AK5MBYGPZVVGOO0978R15AR3_rc AS (
  -- This block sets RC = 0 for every row, complying with the ABAP logic:
  -- "&RC& = 0" (if return code is not zero, result won't be updated).
  SELECT
    t.*,
    0 AS RC
  FROM routine_4AK5MBYGPZVVGOO0978R15AR3_input t
),
routine_4AK5MBYGPZVVGOO0978R15AR3 AS (
  -- This block sets AB = 0 for every row, following the ABAP logic:
  -- "&AB& = 0" (if AB is not zero, update process is canceled).
  SELECT
    t.*,
    0 AS AB
  FROM routine_4AK5MBYGPZVVGOO0978R15AR3_rc t
)