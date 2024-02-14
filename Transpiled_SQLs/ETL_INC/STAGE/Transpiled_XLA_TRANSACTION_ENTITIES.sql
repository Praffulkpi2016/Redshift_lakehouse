TRUNCATE table bronze_bec_ods_stg.XLA_TRANSACTION_ENTITIES;
INSERT INTO bronze_bec_ods_stg.XLA_TRANSACTION_ENTITIES (
  ENTITY_ID,
  APPLICATION_ID,
  LEGAL_ENTITY_ID,
  ENTITY_CODE,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  SOURCE_ID_INT_1,
  SOURCE_ID_CHAR_1,
  SECURITY_ID_INT_1,
  SECURITY_ID_INT_2,
  SECURITY_ID_INT_3,
  SECURITY_ID_CHAR_1,
  SECURITY_ID_CHAR_2,
  SECURITY_ID_CHAR_3,
  SOURCE_ID_INT_2,
  SOURCE_ID_CHAR_2,
  SOURCE_ID_INT_3,
  SOURCE_ID_CHAR_3,
  SOURCE_ID_INT_4,
  SOURCE_ID_CHAR_4,
  TRANSACTION_NUMBER,
  LEDGER_ID,
  VALUATION_METHOD,
  SOURCE_APPLICATION_ID,
  UPG_BATCH_ID,
  UPG_SOURCE_APPLICATION_ID,
  UPG_VALID_FLAG,
  KCA_OPERATION,
  kca_seq_id,
  KCA_SEQ_DATE
)
(
  SELECT
    ENTITY_ID,
    APPLICATION_ID,
    LEGAL_ENTITY_ID,
    ENTITY_CODE,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    SOURCE_ID_INT_1,
    SOURCE_ID_CHAR_1,
    SECURITY_ID_INT_1,
    SECURITY_ID_INT_2,
    SECURITY_ID_INT_3,
    SECURITY_ID_CHAR_1,
    SECURITY_ID_CHAR_2,
    SECURITY_ID_CHAR_3,
    SOURCE_ID_INT_2,
    SOURCE_ID_CHAR_2,
    SOURCE_ID_INT_3,
    SOURCE_ID_CHAR_3,
    SOURCE_ID_INT_4,
    SOURCE_ID_CHAR_4,
    TRANSACTION_NUMBER,
    LEDGER_ID,
    VALUATION_METHOD,
    SOURCE_APPLICATION_ID,
    UPG_BATCH_ID,
    UPG_SOURCE_APPLICATION_ID,
    UPG_VALID_FLAG,
    KCA_OPERATION,
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.xla_transaction_entities
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (ENTITY_ID, kca_seq_id) IN (
      SELECT
        ENTITY_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.xla_transaction_entities
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        ENTITY_ID
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'xla_transaction_entities'
      )
    )
);