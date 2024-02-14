TRUNCATE table bronze_bec_ods_stg.MSC_TRADING_PARTNER_MAPS;
INSERT INTO bronze_bec_ods_stg.MSC_TRADING_PARTNER_MAPS (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MSC_TRADING_PARTNER_MAPS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (MAP_ID, kca_seq_id) IN (
      SELECT
        MAP_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.MSC_TRADING_PARTNER_MAPS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        MAP_ID
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'msc_trading_partner_maps'
      )
    )
);