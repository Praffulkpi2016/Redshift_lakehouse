/* Delete Records */
DELETE FROM silver_bec_ods.OE_DROP_SHIP_SOURCES
WHERE
  (HEADER_ID, LINE_ID) IN (
    SELECT
      stg.HEADER_ID,
      stg.LINE_ID
    FROM silver_bec_ods.OE_DROP_SHIP_SOURCES AS ods, bronze_bec_ods_stg.OE_DROP_SHIP_SOURCES AS stg
    WHERE
      ods.HEADER_ID = stg.HEADER_ID
      AND ods.LINE_ID = stg.LINE_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.OE_DROP_SHIP_SOURCES (
  drop_ship_source_id,
  header_id,
  line_id,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  last_update_login,
  org_id,
  destination_organization_id,
  requisition_header_id,
  requisition_line_id,
  po_header_id,
  po_line_id,
  line_location_id,
  po_release_id,
  inst_id,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    drop_ship_source_id,
    header_id,
    line_id,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    org_id,
    destination_organization_id,
    requisition_header_id,
    requisition_line_id,
    po_header_id,
    po_line_id,
    line_location_id,
    po_release_id,
    inst_id,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.OE_DROP_SHIP_SOURCES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (HEADER_ID, LINE_ID, kca_seq_id) IN (
      SELECT
        HEADER_ID,
        LINE_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.OE_DROP_SHIP_SOURCES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        HEADER_ID,
        LINE_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.OE_DROP_SHIP_SOURCES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.OE_DROP_SHIP_SOURCES SET IS_DELETED_FLG = 'Y'
WHERE
  (HEADER_ID, LINE_ID) IN (
    SELECT
      HEADER_ID,
      LINE_ID
    FROM bec_raw_dl_ext.OE_DROP_SHIP_SOURCES
    WHERE
      (HEADER_ID, LINE_ID, KCA_SEQ_ID) IN (
        SELECT
          HEADER_ID,
          LINE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.OE_DROP_SHIP_SOURCES
        GROUP BY
          HEADER_ID,
          LINE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'oe_drop_ship_sources';