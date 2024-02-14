/* Delete Records */
DELETE FROM silver_bec_ods.RA_TERMS_TL
WHERE
  (TERM_ID, LANGUAGE) IN (
    SELECT
      stg.TERM_ID,
      stg.LANGUAGE
    FROM silver_bec_ods.RA_TERMS_TL AS ods, bronze_bec_ods_stg.RA_TERMS_TL AS stg
    WHERE
      ods.TERM_ID = stg.TERM_ID
      AND ods.LANGUAGE = stg.LANGUAGE
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.RA_TERMS_TL (
  TERM_ID,
  DESCRIPTION,
  NAME,
  LANGUAGE,
  SOURCE_LANG,
  LAST_UPDATE_DATE,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    TERM_ID,
    DESCRIPTION,
    NAME,
    LANGUAGE,
    SOURCE_LANG,
    LAST_UPDATE_DATE,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.RA_TERMS_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (TERM_ID, LANGUAGE, kca_seq_id) IN (
      SELECT
        TERM_ID,
        LANGUAGE,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.RA_TERMS_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        TERM_ID,
        LANGUAGE
    )
);
/* Soft delete */
UPDATE silver_bec_ods.RA_TERMS_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.RA_TERMS_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (TERM_ID, LANGUAGE) IN (
    SELECT
      TERM_ID,
      LANGUAGE
    FROM bec_raw_dl_ext.RA_TERMS_TL
    WHERE
      (TERM_ID, LANGUAGE, KCA_SEQ_ID) IN (
        SELECT
          TERM_ID,
          LANGUAGE,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.RA_TERMS_TL
        GROUP BY
          TERM_ID,
          LANGUAGE
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP(), load_type = 'I'
WHERE
  ods_table_name = 'ra_terms_tl';