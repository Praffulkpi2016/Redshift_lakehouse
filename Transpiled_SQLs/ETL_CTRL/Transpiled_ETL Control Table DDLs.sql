/* DROP TABLE bec_etl_ctrl.batch_dw_info; */
CREATE TABLE IF NOT EXISTS bec_etl_ctrl.batch_dw_info (
  dw_table_name STRING,
  batch_priority INT,
  last_refresh_date TIMESTAMP,
  refresh_frequency STRING,
  disable_flag BOOLEAN,
  load_type STRING,
  parallel_on BOOLEAN,
  executebegints TIMESTAMP,
  batch_name STRING,
  unload_flag BOOLEAN,
  prune_days INT,
  object_level STRING,
  last_refresh_date_pst TIMESTAMP
);
/* DROP TABLE bec_etl_ctrl.batch_ods_info; */
CREATE TABLE IF NOT EXISTS bec_etl_ctrl.batch_ods_info (
  ods_table_name STRING,
  last_refresh_date TIMESTAMP,
  refresh_frequency STRING,
  disable_flag BOOLEAN,
  load_type STRING,
  batch_name STRING,
  executebegints TIMESTAMP,
  prune_days INT,
  object_priority STRING
);
/* DROP TABLE bec_etl_ctrl.batch_variable_info; */
CREATE TABLE IF NOT EXISTS bec_etl_ctrl.batch_variable_info (
  seq_num INT,
  variable_key STRING,
  variable_value STRING
);
/* DROP TABLE bec_etl_ctrl.etlsourceappid; */
CREATE TABLE IF NOT EXISTS bec_etl_ctrl.etlsourceappid (
  source_system STRING,
  system_id INT
);