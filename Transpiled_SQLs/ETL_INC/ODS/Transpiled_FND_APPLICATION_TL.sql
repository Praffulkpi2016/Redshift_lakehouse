/* Delete Records */
DELETE FROM silver_bec_ods.FND_APPLICATION_TL
WHERE
  (APPLICATION_ID, LANGUAGE) IN (
    SELECT
      stg.APPLICATION_ID,
      stg.LANGUAGE
    FROM silver_bec_ods.FND_APPLICATION_TL AS ods, bronze_bec_ods_stg.FND_APPLICATION_TL AS stg
    WHERE
      ods.APPLICATION_ID = stg.APPLICATION_ID
      AND ods.LANGUAGE = stg.LANGUAGE
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.FND_APPLICATION_TL (
  APPLICATION_ID,
  LANGUAGE,
  APPLICATION_NAME,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  DESCRIPTION,
  SOURCE_LANG,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    APPLICATION_ID,
    LANGUAGE,
    APPLICATION_NAME,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATE_LOGIN,
    DESCRIPTION,
    SOURCE_LANG,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.FND_APPLICATION_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (APPLICATION_ID, LANGUAGE, kca_seq_id) IN (
      SELECT
        APPLICATION_ID,
        LANGUAGE,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.FND_APPLICATION_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        APPLICATION_ID,
        LANGUAGE
    )
);
/* Soft delete */
UPDATE silver_bec_ods.FND_APPLICATION_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.FND_APPLICATION_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (APPLICATION_ID, LANGUAGE) IN (
    SELECT
      APPLICATION_ID,
      LANGUAGE
    FROM bec_raw_dl_ext.FND_APPLICATION_TL
    WHERE
      (APPLICATION_ID, LANGUAGE, KCA_SEQ_ID) IN (
        SELECT
          APPLICATION_ID,
          LANGUAGE,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.FND_APPLICATION_TL
        GROUP BY
          APPLICATION_ID,
          LANGUAGE
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fnd_application_tl';