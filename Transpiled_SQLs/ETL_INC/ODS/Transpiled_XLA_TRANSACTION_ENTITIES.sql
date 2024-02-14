/* Delete Records */
DELETE FROM silver_bec_ods.xla_transaction_entities
WHERE
  ENTITY_ID IN (
    SELECT
      stg.ENTITY_ID
    FROM silver_bec_ods.xla_transaction_entities AS ods, bronze_bec_ods_stg.xla_transaction_entities AS stg
    WHERE
      ods.ENTITY_ID = stg.ENTITY_ID AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.xla_transaction_entities (
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
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
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
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.xla_transaction_entities
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (ENTITY_ID, kca_seq_id) IN (
      SELECT
        ENTITY_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.xla_transaction_entities
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        ENTITY_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.xla_transaction_entities SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.xla_transaction_entities SET IS_DELETED_FLG = 'Y'
WHERE
  (
    ENTITY_ID
  ) IN (
    SELECT
      ENTITY_ID
    FROM bec_raw_dl_ext.xla_transaction_entities
    WHERE
      (ENTITY_ID, KCA_SEQ_ID) IN (
        SELECT
          ENTITY_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.xla_transaction_entities
        GROUP BY
          ENTITY_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'xla_transaction_entities';