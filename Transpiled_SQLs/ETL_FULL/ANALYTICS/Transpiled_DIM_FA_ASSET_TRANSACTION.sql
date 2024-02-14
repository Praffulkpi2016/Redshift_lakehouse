DROP table IF EXISTS gold_bec_dwh.dim_fa_asset_transaction;
CREATE TABLE gold_bec_dwh.DIM_FA_ASSET_TRANSACTION AS
(
  SELECT
    fai.asset_id,
    fai.po_number,
    fai.invoice_number,
    fai.payables_batch_name,
    fai.invoice_date,
    fai.invoice_id,
    fai.ap_distribution_line_number,
    fai.asset_invoice_id,
    fai.invoice_transaction_id_in,
    fai.description,
    fai.last_update_date,
    fai.last_updated_by,
    fai.created_by,
    fai.creation_date,
    fai.project_asset_line_id,
    '0' AS x_custom,
    fai.payables_cost,
    fai.ATTRIBUTE3,
    fai.ATTRIBUTE4,
    fai.ATTRIBUTE6,
    fai.ATTRIBUTE7,
    'N' AS is_deleted_flg, /* audit columns */
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) AS source_app_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(fai.asset_id, 0) || '-' || COALESCE(fai.asset_invoice_id, 0) || '-' || COALESCE(fai.invoice_transaction_id_in, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.fa_asset_invoices AS fai
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_fa_asset_transaction' AND batch_name = 'fa';