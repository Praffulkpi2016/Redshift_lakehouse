/* Delete Records */
DELETE FROM silver_bec_ods.IBY_PAYMENT_METHODS_TL
WHERE
  (COALESCE(PAYMENT_METHOD_CODE, 'NA'), COALESCE(LANGUAGE, 'NA')) IN (
    SELECT
      COALESCE(stg.PAYMENT_METHOD_CODE, 'NA') AS PAYMENT_METHOD_CODE,
      COALESCE(stg.LANGUAGE, 'NA') AS LANGUAGE
    FROM silver_bec_ods.IBY_PAYMENT_METHODS_TL AS ods, bronze_bec_ods_stg.IBY_PAYMENT_METHODS_TL AS stg
    WHERE
      COALESCE(ods.PAYMENT_METHOD_CODE, 'NA') = COALESCE(stg.PAYMENT_METHOD_CODE, 'NA')
      AND COALESCE(ods.LANGUAGE, 'NA') = COALESCE(stg.LANGUAGE, 'NA')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.IBY_PAYMENT_METHODS_TL (
  payment_method_code,
  `language`,
  source_lang,
  payment_method_name,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  object_version_number,
  description,
  zd_edition_name,
  zd_sync,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    payment_method_code,
    `language`,
    source_lang,
    payment_method_name,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    object_version_number,
    description,
    zd_edition_name,
    zd_sync,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.IBY_PAYMENT_METHODS_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(PAYMENT_METHOD_CODE, 'NA'), COALESCE(LANGUAGE, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(PAYMENT_METHOD_CODE, 'NA') AS PAYMENT_METHOD_CODE,
        COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.IBY_PAYMENT_METHODS_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(PAYMENT_METHOD_CODE, 'NA'),
        COALESCE(LANGUAGE, 'NA')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.IBY_PAYMENT_METHODS_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.IBY_PAYMENT_METHODS_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(PAYMENT_METHOD_CODE, 'NA'), COALESCE(LANGUAGE, 'NA')) IN (
    SELECT
      COALESCE(PAYMENT_METHOD_CODE, 'NA'),
      COALESCE(LANGUAGE, 'NA')
    FROM bec_raw_dl_ext.IBY_PAYMENT_METHODS_TL
    WHERE
      (COALESCE(PAYMENT_METHOD_CODE, 'NA'), COALESCE(LANGUAGE, 'NA'), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(PAYMENT_METHOD_CODE, 'NA'),
          COALESCE(LANGUAGE, 'NA'),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.IBY_PAYMENT_METHODS_TL
        GROUP BY
          COALESCE(PAYMENT_METHOD_CODE, 'NA'),
          COALESCE(LANGUAGE, 'NA')
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'iby_payment_methods_tl';