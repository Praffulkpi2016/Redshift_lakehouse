TRUNCATE table
	table bronze_bec_ods_stg.ap_terms;
INSERT INTO bronze_bec_ods_stg.ap_terms (
  TERM_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  ENABLED_FLAG,
  DUE_CUTOFF_DAY,
  TYPE,
  START_DATE_ACTIVE,
  END_DATE_ACTIVE,
  RANK,
  ATTRIBUTE_CATEGORY,
  ATTRIBUTE1,
  ATTRIBUTE2,
  ATTRIBUTE3,
  ATTRIBUTE4,
  ATTRIBUTE5,
  ATTRIBUTE6,
  ATTRIBUTE7,
  ATTRIBUTE8,
  ATTRIBUTE9,
  ATTRIBUTE10,
  ATTRIBUTE11,
  ATTRIBUTE12,
  ATTRIBUTE13,
  ATTRIBUTE14,
  ATTRIBUTE15,
  NAME,
  DESCRIPTION,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    B.TERM_ID,
    B.LAST_UPDATE_DATE,
    B.LAST_UPDATED_BY,
    B.CREATION_DATE,
    B.CREATED_BY,
    B.LAST_UPDATE_LOGIN,
    B.ENABLED_FLAG,
    B.DUE_CUTOFF_DAY,
    B.TYPE,
    B.START_DATE_ACTIVE,
    B.END_DATE_ACTIVE,
    B.RANK,
    B.ATTRIBUTE_CATEGORY,
    B.ATTRIBUTE1,
    B.ATTRIBUTE2,
    B.ATTRIBUTE3,
    B.ATTRIBUTE4,
    B.ATTRIBUTE5,
    B.ATTRIBUTE6,
    B.ATTRIBUTE7,
    B.ATTRIBUTE8,
    B.ATTRIBUTE9,
    B.ATTRIBUTE10,
    B.ATTRIBUTE11,
    B.ATTRIBUTE12,
    B.ATTRIBUTE13,
    B.ATTRIBUTE14,
    B.ATTRIBUTE15,
    B.NAME,
    B.DESCRIPTION,
    B.KCA_OPERATION,
    B.kca_seq_id,
    B.kca_seq_date
  FROM bec_raw_dl_ext.AP_TERMS_TL AS B
  WHERE
    B.LANGUAGE = 'US'
    AND B.kca_operation <> 'DELETE'
    AND COALESCE(B.kca_seq_id, '') <> ''
    AND (B.TERM_ID, B.LANGUAGE, B.kca_seq_id) IN (
      SELECT
        TERM_ID,
        LANGUAGE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.AP_TERMS_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        TERM_ID,
        LANGUAGE
    )
    AND B.kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ap_terms'
    )
);