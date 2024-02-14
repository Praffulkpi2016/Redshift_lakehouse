DROP table IF EXISTS gold_bec_dwh.DIM_AP_INVOICE_POS;
CREATE TABLE gold_bec_dwh.dim_ap_invoice_pos AS
SELECT
  invoice_id,
  MAX(POSTED_FLAG) AS POSTED_FLAG
FROM silver_bec_ods.ap_invoice_distributions_all
WHERE
  is_deleted_flg <> 'Y'
GROUP BY
  invoice_id;
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_invoice_pos' AND batch_name = 'ap';