TRUNCATE table bronze_bec_ods_stg.OE_DROP_SHIP_SOURCES;
INSERT INTO bronze_bec_ods_stg.OE_DROP_SHIP_SOURCES (
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
    kca_operation,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.OE_DROP_SHIP_SOURCES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (HEADER_ID, LINE_ID, kca_seq_id) IN (
      SELECT
        HEADER_ID,
        LINE_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.OE_DROP_SHIP_SOURCES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        HEADER_ID,
        LINE_ID
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'oe_drop_ship_sources'
      )
    )
);