DROP table IF EXISTS silver_bec_ods.QP_PRICELISTS_LOV_V;
CREATE TABLE IF NOT EXISTS silver_bec_ods.QP_PRICELISTS_LOV_V (
  price_list_id DECIMAL(15, 0),
  name STRING,
  description STRING,
  start_date_active TIMESTAMP,
  end_date_active TIMESTAMP,
  qdt_start_date_active TIMESTAMP,
  qdt_end_date_active TIMESTAMP,
  currency_code STRING,
  agreement_id DECIMAL(15, 0),
  sold_to_org_id DECIMAL(15, 0),
  source_system_code STRING,
  shareable_flag STRING,
  orig_system_header_ref STRING,
  list_source_code STRING,
  qry_type DECIMAL(15, 0),
  global_flag STRING,
  orig_org_id DECIMAL(15, 0),
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.QP_PRICELISTS_LOV_V (
  price_list_id,
  `name`,
  description,
  start_date_active,
  end_date_active,
  qdt_start_date_active,
  qdt_end_date_active,
  currency_code,
  agreement_id,
  sold_to_org_id,
  source_system_code,
  shareable_flag,
  orig_system_header_ref,
  list_source_code,
  qry_type,
  global_flag,
  orig_org_id,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    price_list_id,
    `name`,
    description,
    start_date_active,
    end_date_active,
    qdt_start_date_active,
    qdt_end_date_active,
    currency_code,
    agreement_id,
    sold_to_org_id,
    source_system_code,
    shareable_flag,
    orig_system_header_ref,
    list_source_code,
    qry_type,
    global_flag,
    orig_org_id,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.QP_PRICELISTS_LOV_V
);
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'qp_pricelists_lov_v';