/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_EXPENDITURE_ITEMS
WHERE
  (
    COALESCE(EXPENDITURE_ITEM_ID, 0)
  ) IN (
    SELECT
      COALESCE(ods.EXPENDITURE_ITEM_ID, 0)
    FROM gold_bec_dwh.DIM_EXPENDITURE_ITEMS AS dw, silver_bec_ods.pa_expenditure_items_all AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.EXPENDITURE_ITEM_ID, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_expenditure_items' AND batch_name = 'gl'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_EXPENDITURE_ITEMS (
  expenditure_item_id,
  expenditure_id,
  expenditure_type,
  orig_transaction_reference,
  transaction_source,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    expenditure_item_id,
    expenditure_id,
    expenditure_type,
    orig_transaction_reference,
    transaction_source,
    'N' AS is_deleted_flg,
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
    ) || '-' || COALESCE(EXPENDITURE_ITEM_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.pa_expenditure_items_all
  WHERE
    1 = 1
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_expenditure_items' AND batch_name = 'gl'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_EXPENDITURE_ITEMS SET is_deleted_flg = 'Y'
WHERE
  NOT (
    COALESCE(EXPENDITURE_ITEM_ID, 0)
  ) IN (
    SELECT
      COALESCE(ods.EXPENDITURE_ITEM_ID, 0) AS EXPENDITURE_ITEM_ID
    FROM gold_bec_dwh.DIM_EXPENDITURE_ITEMS AS dw, silver_bec_ods.pa_expenditure_items_all AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.EXPENDITURE_ITEM_ID, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_expenditure_items' AND batch_name = 'gl';