DROP TABLE IF EXISTS silver_bec_ods.MSC_COMPANY_RELATIONSHIPS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.MSC_COMPANY_RELATIONSHIPS (
  RELATIONSHIP_ID DECIMAL(15, 0),
  SUBJECT_ID DECIMAL(15, 0),
  OBJECT_ID DECIMAL(15, 0),
  RELATIONSHIP_TYPE DECIMAL(15, 0),
  START_DATE TIMESTAMP,
  END_DATE TIMESTAMP,
  CREATION_DATE TIMESTAMP,
  CREATED_BY DECIMAL(15, 0),
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.MSC_COMPANY_RELATIONSHIPS (
  RELATIONSHIP_ID,
  SUBJECT_ID,
  OBJECT_ID,
  RELATIONSHIP_TYPE,
  START_DATE,
  END_DATE,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  RELATIONSHIP_ID,
  SUBJECT_ID,
  OBJECT_ID,
  RELATIONSHIP_TYPE,
  START_DATE,
  END_DATE,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.MSC_COMPANY_RELATIONSHIPS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'msc_company_relationships';