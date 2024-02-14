/* Delete Records */
DELETE FROM silver_bec_ods.MSC_TRADING_PARTNER_MAPS
WHERE
  (
    COALESCE(MAP_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.MAP_ID, 0) AS MAP_ID
    FROM silver_bec_ods.MSC_TRADING_PARTNER_MAPS AS ods, bronze_bec_ods_stg.MSC_TRADING_PARTNER_MAPS AS stg
    WHERE
      COALESCE(ods.MAP_ID, 0) = COALESCE(stg.MAP_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MSC_TRADING_PARTNER_MAPS (
  map_id,
  map_type,
  tp_key,
  company_key,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  last_update_login,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    map_id,
    map_type,
    tp_key,
    company_key,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MSC_TRADING_PARTNER_MAPS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(MAP_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(MAP_ID, 0) AS MAP_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.MSC_TRADING_PARTNER_MAPS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(MAP_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MSC_TRADING_PARTNER_MAPS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MSC_TRADING_PARTNER_MAPS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    MAP_ID
  ) IN (
    SELECT
      MAP_ID
    FROM bec_raw_dl_ext.MSC_TRADING_PARTNER_MAPS
    WHERE
      (MAP_ID, KCA_SEQ_ID) IN (
        SELECT
          MAP_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MSC_TRADING_PARTNER_MAPS
        GROUP BY
          MAP_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'msc_trading_partner_maps';