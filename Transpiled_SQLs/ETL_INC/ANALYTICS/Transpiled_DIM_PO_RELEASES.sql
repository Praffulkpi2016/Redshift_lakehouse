/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_PO_RELEASES
WHERE
  (
    COALESCE(PO_RELEASE_ID, 0)
  ) IN (
    SELECT
      COALESCE(ods.PO_RELEASE_ID, 0) AS PO_RELEASE_ID
    FROM gold_bec_dwh.DIM_PO_RELEASES AS dw, silver_bec_ods.PO_RELEASES_ALL AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.PO_RELEASE_ID, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_po_releases' AND batch_name = 'ap'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_PO_RELEASES (
  PO_RELEASE_ID,
  PO_HEADER_ID,
  RELEASE_NUM,
  AGENT_ID,
  RELEASE_DATE,
  REVISION_NUM,
  REVISED_DATE,
  APPROVED_FLAG,
  APPROVED_DATE,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    PO_RELEASE_ID,
    PO_HEADER_ID,
    RELEASE_NUM,
    AGENT_ID,
    RELEASE_DATE,
    REVISION_NUM,
    REVISED_DATE,
    APPROVED_FLAG,
    APPROVED_DATE,
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
    ) || '-' || COALESCE(PO_RELEASE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.PO_RELEASES_ALL
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
          dw_table_name = 'dim_po_releases' AND batch_name = 'ap'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_PO_RELEASES SET is_deleted_flg = 'Y'
WHERE
  NOT (
    COALESCE(PO_RELEASE_ID, 0)
  ) IN (
    SELECT
      COALESCE(ods.PO_RELEASE_ID, 0) AS PO_RELEASE_ID
    FROM gold_bec_dwh.DIM_PO_RELEASES AS dw, silver_bec_ods.PO_RELEASES_ALL AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.PO_RELEASE_ID, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_po_releases' AND batch_name = 'ap';