DROP table IF EXISTS silver_bec_ods.OKS_LINE_DETAILS_V;
CREATE TABLE IF NOT EXISTS silver_bec_ods.OKS_LINE_DETAILS_V (
  contract_id DECIMAL(15, 0),
  line_id STRING,
  line_reference STRING,
  service_line_number STRING,
  service_name STRING,
  object1_id1 STRING,
  object1_id2 STRING,
  service_start_date TIMESTAMP,
  service_end_date TIMESTAMP,
  customer_account_name STRING,
  line_bto_address STRING,
  line_sto_address STRING,
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.OKS_LINE_DETAILS_V (
  contract_id,
  line_id,
  line_reference,
  service_line_number,
  service_name,
  object1_id1,
  object1_id2,
  service_start_date,
  service_end_date,
  customer_account_name,
  line_bto_address,
  line_sto_address,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    contract_id,
    line_id,
    line_reference,
    service_line_number,
    service_name,
    object1_id1,
    object1_id2,
    service_start_date,
    service_end_date,
    customer_account_name,
    line_bto_address,
    line_sto_address,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.OKS_LINE_DETAILS_V
);
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'oks_line_details_v';