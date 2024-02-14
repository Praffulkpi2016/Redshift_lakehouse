DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_TERMS;
CREATE TABLE bronze_bec_ods_stg.AP_TERMS AS
(
  SELECT
    B.TERM_ID,
    B.LANGUAGE,
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
    B.kca_operation,
    B.kca_seq_id,
    B.kca_seq_date
  FROM bec_raw_dl_ext.AP_TERMS_TL AS B
  WHERE
    B.LANGUAGE = 'US'
    AND B.kca_operation <> 'DELETE'
    AND (B.term_id, B.LANGUAGE, B.last_update_date) IN (
      SELECT
        term_id,
        LANGUAGE,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.AP_TERMS_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        term_id,
        LANGUAGE
    )
);