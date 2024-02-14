DROP TABLE IF EXISTS silver_bec_ods.CST_COST_ELEMENTS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.CST_COST_ELEMENTS (
  COST_ELEMENT_ID DECIMAL(15, 0),
  LAST_UPDATE_DATE TIMESTAMP NOT NULL,
  LAST_UPDATED_BY DECIMAL(15, 0),
  CREATION_DATE TIMESTAMP NOT NULL,
  CREATED_BY DECIMAL(15, 0),
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  COST_ELEMENT STRING,
  DESCRIPTION STRING,
  REQUEST_ID DECIMAL(15, 0),
  PROGRAM_APPLICATION_ID DECIMAL(15, 0),
  PROGRAM_ID DECIMAL(15, 0),
  PROGRAM_UPDATE_DATE DATE,
  ZD_EDITION_NAME STRING,
  ZD_SYNC STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.CST_COST_ELEMENTS (
  COST_ELEMENT_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  COST_ELEMENT,
  DESCRIPTION,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  ZD_EDITION_NAME,
  ZD_SYNC,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  COST_ELEMENT_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  COST_ELEMENT,
  DESCRIPTION,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.CST_COST_ELEMENTS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'cst_cost_elements';