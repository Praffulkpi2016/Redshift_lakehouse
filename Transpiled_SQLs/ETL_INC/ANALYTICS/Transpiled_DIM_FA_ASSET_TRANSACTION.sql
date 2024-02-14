/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_FA_ASSET_TRANSACTION
WHERE
  (COALESCE(asset_id, 0), COALESCE(asset_invoice_id, 0), COALESCE(invoice_transaction_id_in, 0)) IN (
    SELECT
      COALESCE(ods.asset_id, 0) AS asset_id,
      COALESCE(ods.asset_invoice_id, 0) AS asset_invoice_id,
      COALESCE(ods.invoice_transaction_id_in, 0) AS invoice_transaction_id_in
    FROM gold_bec_dwh.dim_fa_asset_transaction AS dw, (
      SELECT
        fai.asset_id,
        fai.asset_invoice_id,
        fai.invoice_transaction_id_in
      FROM silver_bec_ods.fa_asset_invoices AS fai
      WHERE
        (
          fai.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_fa_asset_transaction' AND batch_name = 'fa'
          )
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.asset_id, 0) || '-' || COALESCE(ods.asset_invoice_id, 0) || '-' || COALESCE(ods.invoice_transaction_id_in, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.dim_fa_asset_transaction (
  asset_id,
  po_number,
  invoice_number,
  payables_batch_name,
  invoice_date,
  invoice_id,
  ap_distribution_line_number,
  asset_invoice_id,
  invoice_transaction_id_in,
  description,
  last_update_date,
  last_updated_by,
  created_by,
  creation_date,
  project_asset_line_id,
  x_custom,
  payables_cost,
  attribute3,
  attribute4,
  attribute6,
  attribute7,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
    fai.attribute3,
    fai.attribute4,
    fai.attribute6,
    fai.attribute7,
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
  WHERE
    (
      fai.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_fa_asset_transaction' AND batch_name = 'fa'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_fa_asset_transaction SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(asset_id, 0), COALESCE(asset_invoice_id, 0), COALESCE(invoice_transaction_id_in, 0)) IN (
    SELECT
      COALESCE(ods.asset_id, 0) AS asset_id,
      COALESCE(ods.asset_invoice_id, 0) AS asset_invoice_id,
      COALESCE(ods.invoice_transaction_id_in, 0) AS invoice_transaction_id_in
    FROM gold_bec_dwh.dim_fa_asset_transaction AS dw, (
      SELECT
        fai.asset_id,
        fai.asset_invoice_id,
        fai.invoice_transaction_id_in
      FROM silver_bec_ods.fa_asset_invoices AS fai
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.asset_id, 0) || '-' || COALESCE(ods.asset_invoice_id, 0) || '-' || COALESCE(ods.invoice_transaction_id_in, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_fa_asset_transaction' AND batch_name = 'fa';