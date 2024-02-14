TRUNCATE table bronze_bec_ods_stg.AP_TERMS_TL;
INSERT INTO bronze_bec_ods_stg.AP_TERMS_TL (
  term_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  name,
  enabled_flag,
  due_cutoff_day,
  description,
  type,
  start_date_active,
  end_date_active,
  rank,
  attribute_category,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  language,
  source_lang,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    term_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    name,
    enabled_flag,
    due_cutoff_day,
    description,
    type,
    start_date_active,
    end_date_active,
    rank,
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    language,
    source_lang,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.AP_TERMS_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (TERM_ID, language, kca_seq_id) IN (
      SELECT
        TERM_ID,
        language,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.AP_TERMS_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        TERM_ID,
        language
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ap_terms_tl'
    )
);