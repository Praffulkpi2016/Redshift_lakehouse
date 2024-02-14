/* Delete Records */
DELETE FROM gold_bec_dwh.dim_po_need_by
WHERE
  (COALESCE(po_header_id, 0), COALESCE(po_line_id, 0)) IN (
    SELECT
      COALESCE(ods.po_header_id) AS po_header_id,
      COALESCE(ods.po_line_id, 0) AS po_line_id
    FROM gold_bec_dwh.dim_po_need_by AS dw, (
      SELECT
        po_header_id,
        po_line_id
      FROM silver_bec_ods.po_line_locations_all
      WHERE
        (
          kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_po_need_by' AND batch_name = 'po'
          )
          OR is_deleted_flg = 'Y'
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.po_header_id, 0) || '-' || COALESCE(ods.po_line_id, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_PO_NEED_BY (
  po_header_id,
  po_line_id,
  max_need_by,
  min_need_by,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    po_header_id,
    po_line_id,
    MAX(need_by_date) AS max_need_by,
    MIN(need_by_date) AS min_need_by,
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
    ) || '-' || COALESCE(po_header_id, 0) || '-' || COALESCE(po_line_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.po_line_locations_all
  WHERE
    1 = 1
    AND is_deleted_flg <> 'Y'
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_po_need_by' AND batch_name = 'po'
      )
    )
  GROUP BY
    po_header_id,
    po_line_id
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_po_need_by' AND batch_name = 'po';