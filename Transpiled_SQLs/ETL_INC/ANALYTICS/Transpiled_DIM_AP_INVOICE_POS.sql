/* delete */
DELETE FROM gold_bec_dwh.dim_ap_invoice_pos
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.ap_invoice_distributions_all AS aid
    WHERE
      aid.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ap_invoice_pos' AND batch_name = 'ap'
      )
      AND dim_ap_invoice_pos.invoice_id = aid.invoice_id
  );
/* INSERT */
INSERT INTO gold_bec_dwh.dim_ap_invoice_pos
SELECT
  invoice_id,
  MAX(POSTED_FLAG) AS POSTED_FLAG
FROM silver_bec_ods.ap_invoice_distributions_all
WHERE
  kca_seq_date > (
    SELECT
      (
        executebegints - prune_days
      )
    FROM bec_etl_ctrl.batch_dw_info
    WHERE
      dw_table_name = 'dim_ap_invoice_pos' AND batch_name = 'ap'
  )
GROUP BY
  invoice_id;
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_invoice_pos' AND batch_name = 'ap';