TRUNCATE table gold_bec_dwh.FACT_WO_VALUE_VARIANCE;
INSERT INTO gold_bec_dwh.FACT_WO_VALUE_VARIANCE
(
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY dw_load_id) AS Rownumber
  FROM gold_bec_dwh.FACT_WO_VALUE_VARIANCE_UNION1_STG
  UNION ALL
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY dw_load_id) AS Rownumber
  FROM gold_bec_dwh.FACT_WO_VALUE_VARIANCE_UNION2_STG
  UNION ALL
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY dw_load_id) AS Rownumber
  FROM gold_bec_dwh.FACT_WO_VALUE_VARIANCE_UNION3_STG
  UNION ALL
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY dw_load_id) AS Rownumber
  FROM gold_bec_dwh.FACT_WO_VALUE_VARIANCE_UNION4_STG
  UNION ALL
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY dw_load_id) AS Rownumber
  FROM gold_bec_dwh.FACT_WO_VALUE_VARIANCE_UNION5_STG
  UNION ALL
  SELECT
    *
  FROM gold_bec_dwh.FACT_WO_VALUE_VARIANCE_UNION6_STG
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_wo_value_variance' AND batch_name = 'wip';