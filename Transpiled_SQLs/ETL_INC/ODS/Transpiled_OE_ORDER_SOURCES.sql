/* Delete Records */
DELETE FROM silver_bec_ods.OE_ORDER_SOURCES
WHERE
  (
    COALESCE(ORDER_SOURCE_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.ORDER_SOURCE_ID, 0) AS ORDER_SOURCE_ID
    FROM silver_bec_ods.OE_ORDER_SOURCES AS ods, bronze_bec_ods_stg.OE_ORDER_SOURCES AS stg
    WHERE
      COALESCE(ods.ORDER_SOURCE_ID, 0) = COALESCE(stg.ORDER_SOURCE_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.OE_ORDER_SOURCES (
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
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
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
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.OE_ORDER_SOURCES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(ORDER_SOURCE_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(ORDER_SOURCE_ID, 0) AS ORDER_SOURCE_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.OE_ORDER_SOURCES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        ORDER_SOURCE_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.OE_ORDER_SOURCES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.OE_ORDER_SOURCES SET IS_DELETED_FLG = 'Y'
WHERE
  (
    ORDER_SOURCE_ID
  ) IN (
    SELECT
      ORDER_SOURCE_ID
    FROM bec_raw_dl_ext.OE_ORDER_SOURCES
    WHERE
      (ORDER_SOURCE_ID, KCA_SEQ_ID) IN (
        SELECT
          ORDER_SOURCE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.OE_ORDER_SOURCES
        GROUP BY
          ORDER_SOURCE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'oe_order_sources';