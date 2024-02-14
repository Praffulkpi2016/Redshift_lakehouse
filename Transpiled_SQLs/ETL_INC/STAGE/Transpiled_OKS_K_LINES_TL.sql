TRUNCATE table bronze_bec_ods_stg.OKS_K_LINES_TL;
INSERT INTO bronze_bec_ods_stg.OKS_K_LINES_TL (
  id,
  `language`,
  source_lang,
  sfwt_flag,
  invoice_text,
  ib_trx_details,
  status_text,
  react_time_name,
  security_group_id,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    id,
    `language`,
    source_lang,
    sfwt_flag,
    invoice_text,
    ib_trx_details,
    status_text,
    react_time_name,
    security_group_id,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.OKS_K_LINES_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (ID, LANGUAGE, kca_seq_id) IN (
      SELECT
        ID,
        LANGUAGE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.OKS_K_LINES_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        ID,
        LANGUAGE
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'oks_k_lines_tl'
    )
);