TRUNCATE table
	table bronze_bec_ods_stg.OKS_LEVEL_ELEMENTS;
INSERT INTO bronze_bec_ods_stg.OKS_LEVEL_ELEMENTS (
  ID,
  SEQUENCE_NUMBER,
  DATE_START,
  AMOUNT,
  DATE_RECEIVABLE_GL,
  DATE_REVENUE_RULE_START,
  DATE_TRANSACTION,
  DATE_DUE,
  DATE_PRINT,
  DATE_TO_INTERFACE,
  DATE_COMPLETED,
  OBJECT_VERSION_NUMBER,
  RUL_ID,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  SECURITY_GROUP_ID,
  CLE_ID,
  DNZ_CHR_ID,
  PARENT_CLE_ID,
  DATE_END,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    ID,
    SEQUENCE_NUMBER,
    DATE_START,
    AMOUNT,
    DATE_RECEIVABLE_GL,
    DATE_REVENUE_RULE_START,
    DATE_TRANSACTION,
    DATE_DUE,
    DATE_PRINT,
    DATE_TO_INTERFACE,
    DATE_COMPLETED,
    OBJECT_VERSION_NUMBER,
    RUL_ID,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_DATE,
    SECURITY_GROUP_ID,
    CLE_ID,
    DNZ_CHR_ID,
    PARENT_CLE_ID,
    DATE_END,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.OKS_LEVEL_ELEMENTS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(ID, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(ID, 'NA') AS ID,
        MAX(kca_seq_id) AS kca_seq_id
      FROM bec_raw_dl_ext.OKS_LEVEL_ELEMENTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(ID, 'NA')
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'oks_level_elements'
    )
);