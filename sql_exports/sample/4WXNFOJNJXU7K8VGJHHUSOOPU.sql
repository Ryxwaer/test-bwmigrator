WITH routine_4WXNFOJNJXU7K8VGJHHUSOOPU_step1 AS (
  -- 1) Reads all data from the source table 2LIS_13_VDITM.
  -- This corresponds to preparing the input for the routine.
  -- No join, aggregation, or filter here, just a simple load of source columns.
  SELECT
    *
  FROM
    2LIS_13_VDITM
),

routine_4WXNFOJNJXU7K8VGJHHUSOOPU_step2 AS (
  -- 2) Implements the "only Debit" check (ABAP IF statement).
  --    IF /BIC/ZC_TRTYPE = 'U1', THEN RS = /BIC/ZK_SALESq ELSE 0.
  -- This translates to a CASE expression:
  SELECT
    t.*,
    CASE
      WHEN t."/BIC/ZC_TRTYPE" = 'U1' THEN t."/BIC/ZK_SALESq"
      ELSE 0
    END AS RS
  FROM
    routine_4WXNFOJNJXU7K8VGJHHUSOOPU_step1 t
),

routine_4WXNFOJNJXU7K8VGJHHUSOOPU AS (
  -- 3) Sets RC = 0 and AB = 0 (ABAP &RC& = 0 and &AB& = 0).
  --    This means no error and no abort condition.
  -- No join or aggregation, just adding constant fields.
  SELECT
    t.*,
    0 AS RC,
    0 AS AB
  FROM
    routine_4WXNFOJNJXU7K8VGJHHUSOOPU_step2 t
)