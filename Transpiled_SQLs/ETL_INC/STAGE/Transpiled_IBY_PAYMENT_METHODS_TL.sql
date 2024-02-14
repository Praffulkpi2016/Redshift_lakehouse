TRUNCATE table bronze_bec_ods_stg.IBY_PAYMENT_METHODS_TL;
INSERT INTO bronze_bec_ods_stg.IBY_PAYMENT_METHODS_TL (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.IBY_PAYMENT_METHODS_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(PAYMENT_METHOD_CODE, 'NA'), COALESCE(LANGUAGE, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(PAYMENT_METHOD_CODE, 'NA') AS PAYMENT_METHOD_CODE,
        COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.IBY_PAYMENT_METHODS_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(PAYMENT_METHOD_CODE, 'NA'),
        COALESCE(LANGUAGE, 'NA')
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'iby_payment_methods_tl'
    )
);