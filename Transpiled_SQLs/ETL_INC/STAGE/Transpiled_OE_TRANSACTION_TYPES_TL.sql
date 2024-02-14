TRUNCATE table
	table bronze_bec_ods_stg.OE_TRANSACTION_TYPES_TL;
INSERT INTO bronze_bec_ods_stg.OE_TRANSACTION_TYPES_TL (
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
  KCA_OPERATION,
  KCA_SEQ_ID,
  KCA_SEQ_DATE
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
    KCA_OPERATION,
    KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.OE_TRANSACTION_TYPES_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(TRANSACTION_TYPE_ID, 0), COALESCE(language, 'NA'), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(TRANSACTION_TYPE_ID, 0) AS TRANSACTION_TYPE_ID,
        COALESCE(language, 'NA') AS language,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.OE_TRANSACTION_TYPES_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        TRANSACTION_TYPE_ID,
        language
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'oe_transaction_types_tl'
      )
    )
);