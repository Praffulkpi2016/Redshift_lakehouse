TRUNCATE table bronze_bec_ods_stg.QP_LIST_HEADERS_TL;
INSERT INTO bronze_bec_ods_stg.QP_LIST_HEADERS_TL (
  list_header_id,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  last_update_login,
  `language`,
  source_lang,
  `name`,
  description,
  version_no,
  KCA_OPERATION,
  KCA_SEQ_ID,
  KCA_SEQ_DATE
)
(
  SELECT
    list_header_id,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    `language`,
    source_lang,
    `name`,
    description,
    version_no,
    KCA_OPERATION,
    KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.QP_LIST_HEADERS_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (LANGUAGE, LIST_HEADER_ID, KCA_SEQ_ID) IN (
      SELECT
        LANGUAGE,
        LIST_HEADER_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.QP_LIST_HEADERS_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        LANGUAGE,
        LIST_HEADER_ID
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'qp_list_headers_tl'
      )
    )
);