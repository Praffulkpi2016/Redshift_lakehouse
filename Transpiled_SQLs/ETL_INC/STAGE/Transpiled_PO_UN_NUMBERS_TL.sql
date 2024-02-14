TRUNCATE table bronze_bec_ods_stg.PO_UN_NUMBERS_TL;
INSERT INTO bronze_bec_ods_stg.PO_UN_NUMBERS_TL (
  un_number_id,
  `language`,
  source_lang,
  un_number,
  description,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  kca_operation,
  kca_seq_id,
  KCA_SEQ_DATE
)
(
  SELECT
    un_number_id,
    `language`,
    source_lang,
    un_number,
    description,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    kca_operation,
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.PO_UN_NUMBERS_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (un_number_id, language, kca_seq_id) IN (
      SELECT
        un_number_id,
        language,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.PO_UN_NUMBERS_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        un_number_id,
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
          ods_table_name = 'po_un_numbers_tl'
      )
    )
);