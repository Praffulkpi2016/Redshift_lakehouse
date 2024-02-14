TRUNCATE table bronze_bec_ods_stg.OKC_K_HEADERS_TL;
INSERT INTO bronze_bec_ods_stg.OKC_K_HEADERS_TL (
  id,
  `language`,
  source_lang,
  sfwt_flag,
  short_description,
  comments,
  description,
  cognomen,
  non_response_reason,
  non_response_explain,
  set_aside_reason,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  security_group_id,
  kca_operation,
  kca_seq_id,
  KCA_SEQ_DATE
)
(
  SELECT
    id,
    `language`,
    source_lang,
    sfwt_flag,
    short_description,
    comments,
    description,
    cognomen,
    non_response_reason,
    non_response_explain,
    set_aside_reason,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    security_group_id,
    kca_operation,
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.OKC_K_HEADERS_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (ID, LANGUAGE, kca_seq_id) IN (
      SELECT
        ID,
        LANGUAGE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.OKC_K_HEADERS_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        ID,
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
          ods_table_name = 'okc_k_headers_tl'
      )
    )
);