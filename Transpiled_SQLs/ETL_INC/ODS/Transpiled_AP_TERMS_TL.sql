/* Delete Records */
DELETE FROM silver_bec_ods.AP_TERMS_TL
WHERE
  (TERM_ID, language) IN (
    SELECT
      stg.term_id,
      stg.language
    FROM silver_bec_ods.AP_TERMS_TL AS ods, bronze_bec_ods_stg.AP_TERMS_TL AS stg
    WHERE
      ods.term_id = stg.term_id
      AND ods.language = stg.language
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.ap_terms_tl (
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
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    term_id,
    TO_DATE(DATE_FORMAT(last_update_date, 'yyyy-MM-dd HH:mm:ss'), 'YYYY-MM-DD HH24:MI:SS'),
    last_updated_by,
    TO_DATE(DATE_FORMAT(creation_date, 'yyyy-MM-dd HH:mm:ss'), 'YYYY-MM-DD HH24:MI:SS'),
    created_by,
    last_update_login,
    name,
    enabled_flag,
    due_cutoff_day,
    description,
    type,
    TO_DATE(DATE_FORMAT(start_date_active, 'yyyy-MM-dd HH:mm:ss'), 'YYYY-MM-DD HH24:MI:SS'),
    TO_DATE(DATE_FORMAT(end_date_active, 'yyyy-MM-dd HH:mm:ss'), 'YYYY-MM-DD HH24:MI:SS'),
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
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.AP_TERMS_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (TERM_ID, language, kca_seq_id) IN (
      SELECT
        TERM_ID,
        language,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.AP_TERMS_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        TERM_ID,
        language
    )
);
/* Soft delete */
UPDATE silver_bec_ods.AP_TERMS_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.AP_TERMS_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (TERM_ID, language) IN (
    SELECT
      TERM_ID,
      language
    FROM bec_raw_dl_ext.AP_TERMS_TL
    WHERE
      (TERM_ID, language, KCA_SEQ_ID) IN (
        SELECT
          TERM_ID,
          language,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.AP_TERMS_TL
        GROUP BY
          TERM_ID,
          language
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_terms_tl';