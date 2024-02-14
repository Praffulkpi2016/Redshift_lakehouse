/* Delete Records */
DELETE FROM silver_bec_ods.OE_TRANSACTION_TYPES_TL
WHERE
  (COALESCE(TRANSACTION_TYPE_ID, 0), COALESCE(language, 'NA')) IN (
    SELECT
      COALESCE(stg.TRANSACTION_TYPE_ID, 0) AS TRANSACTION_TYPE_ID,
      COALESCE(stg.LANGUAGE, 'NA') AS language
    FROM silver_bec_ods.OE_TRANSACTION_TYPES_TL AS ods, bronze_bec_ods_stg.OE_TRANSACTION_TYPES_TL AS stg
    WHERE
      COALESCE(ods.TRANSACTION_TYPE_ID, 0) = COALESCE(stg.TRANSACTION_TYPE_ID, 0)
      AND COALESCE(ods.LANGUAGE, 'NA') = COALESCE(stg.LANGUAGE, 'NA')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.OE_TRANSACTION_TYPES_TL (
  transaction_type_id,
  `language`,
  source_lang,
  `name`,
  description,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  last_update_login,
  program_application_id,
  program_id,
  request_id,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    transaction_type_id,
    `language`,
    source_lang,
    `name`,
    description,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    program_application_id,
    program_id,
    request_id,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.OE_TRANSACTION_TYPES_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(TRANSACTION_TYPE_ID, 0), COALESCE(language, 'NA'), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(TRANSACTION_TYPE_ID, 0) AS TRANSACTION_TYPE_ID,
        COALESCE(language, 'NA') AS language,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.OE_TRANSACTION_TYPES_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        TRANSACTION_TYPE_ID,
        language
    )
);
/* Soft delete */
UPDATE silver_bec_ods.OE_TRANSACTION_TYPES_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.OE_TRANSACTION_TYPES_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(TRANSACTION_TYPE_ID, 0), COALESCE(language, 'NA')) IN (
    SELECT
      COALESCE(TRANSACTION_TYPE_ID, 0),
      COALESCE(language, 'NA')
    FROM bec_raw_dl_ext.OE_TRANSACTION_TYPES_TL
    WHERE
      (COALESCE(TRANSACTION_TYPE_ID, 0), COALESCE(language, 'NA'), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(TRANSACTION_TYPE_ID, 0),
          COALESCE(language, 'NA'),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.OE_TRANSACTION_TYPES_TL
        GROUP BY
          COALESCE(TRANSACTION_TYPE_ID, 0),
          COALESCE(language, 'NA')
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'oe_transaction_types_tl';