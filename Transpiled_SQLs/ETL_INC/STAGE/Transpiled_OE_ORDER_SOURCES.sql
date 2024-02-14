TRUNCATE table
	table bronze_bec_ods_stg.OE_ORDER_SOURCES;
INSERT INTO bronze_bec_ods_stg.OE_ORDER_SOURCES (
  order_source_id,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  last_update_login,
  `name`,
  description,
  enabled_flag,
  create_customers_flag,
  use_ids_flag,
  aia_enabled_flag,
  zd_edition_name,
  KCA_OPERATION,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    order_source_id,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    `name`,
    description,
    enabled_flag,
    create_customers_flag,
    use_ids_flag,
    aia_enabled_flag,
    zd_edition_name,
    KCA_OPERATION,
    KCA_SEQ_ID,
    kca_seq_date
  FROM bec_raw_dl_ext.OE_ORDER_SOURCES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(ORDER_SOURCE_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(ORDER_SOURCE_ID, 0) AS ORDER_SOURCE_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.OE_ORDER_SOURCES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        ORDER_SOURCE_ID
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'oe_order_sources'
      )
    )
);