DROP table IF EXISTS gold_bec_dwh_rpt.FACT_INVENTORY_VALUE_RT;
CREATE TABLE gold_bec_dwh_rpt.FACT_INVENTORY_VALUE_RT /* 3806330 */ AS
(
  SELECT DISTINCT
    xx.*
  FROM BEC_ODS.XXBEC_INVENTORY_VALUE_RPT AS xx
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_inventory_value_rt' AND batch_name = 'inv';