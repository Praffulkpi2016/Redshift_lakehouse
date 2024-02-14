TRUNCATE table bronze_bec_ods_stg.PO_LINE_TYPES_TL;
INSERT INTO bronze_bec_ods_stg.PO_LINE_TYPES_TL (
  line_type_id,
  `language`,
  source_lang,
  description,
  line_type,
  last_update_date,
  last_updated_by,
  last_update_login,
  creation_date,
  created_by,
  ZD_EDITION_NAME,
  ZD_SYNC,
  kca_operation,
  kca_seq_id,
  KCA_SEQ_DATE
)
(
  SELECT
    line_type_id,
    `language`,
    source_lang,
    description,
    line_type,
    last_update_date,
    last_updated_by,
    last_update_login,
    creation_date,
    created_by,
    ZD_EDITION_NAME,
    ZD_SYNC,
    kca_operation,
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.PO_LINE_TYPES_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (LINE_TYPE_ID, LANGUAGE, kca_seq_id) IN (
      SELECT
        LINE_TYPE_ID,
        LANGUAGE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.PO_LINE_TYPES_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        LINE_TYPE_ID,
        LANGUAGE
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'po_line_types_tl'
      )
    )
);