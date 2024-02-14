/* Delete Records */
DELETE FROM silver_bec_ods.IBY_PMT_INSTR_USES_ALL
WHERE
  COALESCE(INSTRUMENT_PAYMENT_USE_ID, 0) IN (
    SELECT
      COALESCE(stg.INSTRUMENT_PAYMENT_USE_ID, 0)
    FROM silver_bec_ods.IBY_PMT_INSTR_USES_ALL AS ods, bronze_bec_ods_stg.IBY_PMT_INSTR_USES_ALL AS stg
    WHERE
      ods.INSTRUMENT_PAYMENT_USE_ID = stg.INSTRUMENT_PAYMENT_USE_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.IBY_PMT_INSTR_USES_ALL (
  INSTRUMENT_PAYMENT_USE_ID,
  PAYMENT_FLOW,
  EXT_PMT_PARTY_ID,
  INSTRUMENT_TYPE,
  INSTRUMENT_ID,
  PAYMENT_FUNCTION,
  ORDER_OF_PREFERENCE,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  OBJECT_VERSION_NUMBER,
  START_DATE,
  END_DATE,
  DEBIT_AUTH_FLAG,
  DEBIT_AUTH_METHOD,
  DEBIT_AUTH_REFERENCE,
  DEBIT_AUTH_BEGIN,
  DEBIT_AUTH_END,
  ATTRIBUTE_CATEGORY,
  ATTRIBUTE1,
  ATTRIBUTE2,
  ATTRIBUTE3,
  ATTRIBUTE4,
  ATTRIBUTE5,
  ATTRIBUTE6,
  ATTRIBUTE7,
  ATTRIBUTE8,
  ATTRIBUTE9,
  ATTRIBUTE10,
  ATTRIBUTE11,
  ATTRIBUTE12,
  ATTRIBUTE13,
  ATTRIBUTE14,
  ATTRIBUTE15,
  KCA_OPERATION,
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    INSTRUMENT_PAYMENT_USE_ID,
    PAYMENT_FLOW,
    EXT_PMT_PARTY_ID,
    INSTRUMENT_TYPE,
    INSTRUMENT_ID,
    PAYMENT_FUNCTION,
    ORDER_OF_PREFERENCE,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATE_LOGIN,
    OBJECT_VERSION_NUMBER,
    START_DATE,
    END_DATE,
    DEBIT_AUTH_FLAG,
    DEBIT_AUTH_METHOD,
    DEBIT_AUTH_REFERENCE,
    DEBIT_AUTH_BEGIN,
    DEBIT_AUTH_END,
    ATTRIBUTE_CATEGORY,
    ATTRIBUTE1,
    ATTRIBUTE2,
    ATTRIBUTE3,
    ATTRIBUTE4,
    ATTRIBUTE5,
    ATTRIBUTE6,
    ATTRIBUTE7,
    ATTRIBUTE8,
    ATTRIBUTE9,
    ATTRIBUTE10,
    ATTRIBUTE11,
    ATTRIBUTE12,
    ATTRIBUTE13,
    ATTRIBUTE14,
    ATTRIBUTE15,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.IBY_PMT_INSTR_USES_ALL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (INSTRUMENT_PAYMENT_USE_ID, kca_seq_id) IN (
      SELECT
        INSTRUMENT_PAYMENT_USE_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.IBY_PMT_INSTR_USES_ALL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        INSTRUMENT_PAYMENT_USE_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.IBY_PMT_INSTR_USES_ALL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.IBY_PMT_INSTR_USES_ALL SET IS_DELETED_FLG = 'Y'
WHERE
  (
    INSTRUMENT_PAYMENT_USE_ID
  ) IN (
    SELECT
      INSTRUMENT_PAYMENT_USE_ID
    FROM bec_raw_dl_ext.IBY_PMT_INSTR_USES_ALL
    WHERE
      (INSTRUMENT_PAYMENT_USE_ID, KCA_SEQ_ID) IN (
        SELECT
          INSTRUMENT_PAYMENT_USE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.IBY_PMT_INSTR_USES_ALL
        GROUP BY
          INSTRUMENT_PAYMENT_USE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'iby_pmt_instr_uses_all';