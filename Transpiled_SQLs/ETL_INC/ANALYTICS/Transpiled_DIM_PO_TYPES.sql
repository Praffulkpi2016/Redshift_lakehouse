/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_PO_TYPES
WHERE
  (COALESCE(PO_TYPE, '0'), COALESCE(LOOKUP_TYPE, '0')) IN (
    SELECT
      COALESCE(ods.LOOKUP_CODE, '0'),
      COALESCE(ods.LOOKUP_TYPE, '0')
    FROM gold_bec_dwh.DIM_PO_TYPES AS dw, (
      SELECT
        LOOKUP_CODE,
        LOOKUP_TYPE
      FROM silver_bec_ods.PO_LOOKUP_CODES
      WHERE
        1 = 1
        AND kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_po_types' AND batch_name = 'po'
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.LOOKUP_CODE, '0') || '-' || COALESCE(ods.LOOKUP_TYPE, '0')
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_PO_TYPES (
  PO_TYPE,
  LOOKUP_TYPE,
  PO_TYPE_DISPLAY,
  PO_TYPE_DESC,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    LOOKUP_CODE AS PO_TYPE,
    LOOKUP_TYPE,
    DISPLAYED_FIELD AS PO_TYPE_DISPLAY,
    DESCRIPTION AS PO_TYPE_DESC,
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
    ) || '-' || COALESCE(LOOKUP_CODE, '0') || '-' || COALESCE(LOOKUP_TYPE, '0') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.PO_LOOKUP_CODES
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
          dw_table_name = 'dim_po_types' AND batch_name = 'po'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_PO_TYPES SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(PO_TYPE, '0'), COALESCE(LOOKUP_TYPE, '0')) IN (
    SELECT
      COALESCE(ods.LOOKUP_CODE, '0'),
      COALESCE(ods.LOOKUP_TYPE, '0')
    FROM gold_bec_dwh.DIM_PO_TYPES AS dw, (
      SELECT
        LOOKUP_CODE,
        LOOKUP_TYPE
      FROM silver_bec_ods.PO_LOOKUP_CODES
      WHERE
        1 = 1
        AND is_deleted_flg <> 'Y'
        AND kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_po_types' AND batch_name = 'po'
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.LOOKUP_CODE, '0') || '-' || COALESCE(ods.LOOKUP_TYPE, '0')
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_po_types' AND batch_name = 'po';