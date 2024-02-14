TRUNCATE table
	table bronze_bec_ods_stg.FA_ASSET_HISTORY;
INSERT INTO bronze_bec_ods_stg.FA_ASSET_HISTORY (
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
  KCA_OPERATION,
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
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.FA_ASSET_HISTORY
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(ASSET_ID, 0), COALESCE(DATE_EFFECTIVE, '1900-01-01 00:00:00'), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(ASSET_ID, 0) AS ASSET_ID,
        COALESCE(DATE_EFFECTIVE, '1900-01-01 00:00:00') AS DATE_EFFECTIVE,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.FA_ASSET_HISTORY
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(ASSET_ID, 0),
        COALESCE(DATE_EFFECTIVE, '1900-01-01 00:00:00')
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'fa_asset_history'
    )
);