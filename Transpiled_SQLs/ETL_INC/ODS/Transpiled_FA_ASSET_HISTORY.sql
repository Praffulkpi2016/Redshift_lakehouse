/* Delete Records */
DELETE FROM silver_bec_ods.FA_ASSET_HISTORY
WHERE
  (COALESCE(ASSET_ID, 0), COALESCE(DATE_EFFECTIVE, '1900-01-01 00:00:00')) IN (
    SELECT
      COALESCE(stg.ASSET_ID, 0) AS ASSET_ID,
      COALESCE(stg.DATE_EFFECTIVE, '1900-01-01 00:00:00') AS DATE_EFFECTIVE
    FROM silver_bec_ods.FA_ASSET_HISTORY AS ods, bronze_bec_ods_stg.FA_ASSET_HISTORY AS stg
    WHERE
      COALESCE(ods.ASSET_ID, 0) = COALESCE(stg.ASSET_ID, 0)
      AND COALESCE(ods.DATE_EFFECTIVE, '1900-01-01 00:00:00') = COALESCE(stg.DATE_EFFECTIVE, '1900-01-01 00:00:00')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.FA_ASSET_HISTORY (
  asset_id,
  category_id,
  asset_type,
  units,
  date_effective,
  date_ineffective,
  transaction_header_id_in,
  transaction_header_id_out,
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
    asset_id,
    category_id,
    asset_type,
    units,
    date_effective,
    date_ineffective,
    transaction_header_id_in,
    transaction_header_id_out,
    last_update_date,
    last_updated_by,
    last_update_login,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.FA_ASSET_HISTORY
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(ASSET_ID, 0), COALESCE(DATE_EFFECTIVE, '1900-01-01 00:00:00'), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(ASSET_ID, 0) AS ASSET_ID,
        COALESCE(DATE_EFFECTIVE, '1900-01-01 00:00:00') AS DATE_EFFECTIVE,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.FA_ASSET_HISTORY
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(ASSET_ID, 0),
        COALESCE(DATE_EFFECTIVE, '1900-01-01 00:00:00')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.FA_ASSET_HISTORY SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.FA_ASSET_HISTORY SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(ASSET_ID, 0), COALESCE(DATE_EFFECTIVE, '1900-01-01 00:00:00')) IN (
    SELECT
      COALESCE(ASSET_ID, 0),
      COALESCE(DATE_EFFECTIVE, '1900-01-01 00:00:00')
    FROM bec_raw_dl_ext.FA_ASSET_HISTORY
    WHERE
      (COALESCE(ASSET_ID, 0), COALESCE(DATE_EFFECTIVE, '1900-01-01 00:00:00'), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(ASSET_ID, 0),
          COALESCE(DATE_EFFECTIVE, '1900-01-01 00:00:00'),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.FA_ASSET_HISTORY
        GROUP BY
          COALESCE(ASSET_ID, 0),
          COALESCE(DATE_EFFECTIVE, '1900-01-01 00:00:00')
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fa_asset_history';