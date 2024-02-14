/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_PO_LINE_TYPE
WHERE
  (
    COALESCE(line_type_id, 0)
  ) IN (
    SELECT
      COALESCE(ods.line_type_id, 0) AS LINE_TYPE_ID
    FROM gold_bec_dwh.DIM_PO_LINE_TYPE AS dw, silver_bec_ods.PO_LINE_TYPES_TL AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.line_type_id, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_po_line_type' AND batch_name = 'po'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_PO_LINE_TYPE (
  line_type_id,
  line_type,
  line_type_desc,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    line_type_id,
    line_type,
    DESCRIPTION AS line_type_desc,
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
    ) || '-' || COALESCE(LINE_TYPE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.PO_LINE_TYPES_TL
  WHERE
    COALESCE(Language, 'US') = 'US'
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_po_line_type' AND batch_name = 'po'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_PO_LINE_TYPE SET is_deleted_flg = 'Y'
WHERE
  NOT (
    COALESCE(line_type_id, 0)
  ) IN (
    SELECT
      COALESCE(ods.line_type_id, 0) AS LINE_TYPE_ID
    FROM gold_bec_dwh.DIM_PO_LINE_TYPE AS dw, silver_bec_ods.PO_LINE_TYPES_TL AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.line_type_id, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_po_line_type' AND batch_name = 'po';