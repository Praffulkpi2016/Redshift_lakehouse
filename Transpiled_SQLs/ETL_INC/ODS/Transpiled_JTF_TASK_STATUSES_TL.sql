/* Delete Records */
DELETE FROM silver_bec_ods.JTF_TASK_STATUSES_TL
WHERE
  (COALESCE(TASK_STATUS_ID, 0), COALESCE(LANGUAGE, 'NA')) IN (
    SELECT
      COALESCE(stg.TASK_STATUS_ID, 0) AS TASK_STATUS_ID,
      COALESCE(stg.LANGUAGE, 'NA') AS LANGUAGE
    FROM silver_bec_ods.JTF_TASK_STATUSES_TL AS ods, bronze_bec_ods_stg.JTF_TASK_STATUSES_TL AS stg
    WHERE
      COALESCE(ods.TASK_STATUS_ID, 0) = COALESCE(stg.TASK_STATUS_ID, 0)
      AND COALESCE(ods.LANGUAGE, 'NA') = COALESCE(stg.LANGUAGE, 'NA')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.JTF_TASK_STATUSES_TL (
  TASK_STATUS_ID,
  LANGUAGE,
  SOURCE_LANG,
  NAME,
  DESCRIPTION,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  SECURITY_GROUP_ID,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_ID,
  kca_seq_date
)
(
  SELECT
    TASK_STATUS_ID,
    LANGUAGE,
    SOURCE_LANG,
    NAME,
    DESCRIPTION,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATE_LOGIN,
    SECURITY_GROUP_ID,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.JTF_TASK_STATUSES_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(TASK_STATUS_ID, 0), COALESCE(LANGUAGE, 'NA'), kca_seq_ID) IN (
      SELECT
        COALESCE(TASK_STATUS_ID, 0) AS TASK_STATUS_ID,
        COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
        MAX(kca_seq_ID) AS kca_seq_ID
      FROM bronze_bec_ods_stg.JTF_TASK_STATUSES_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(TASK_STATUS_ID, 0),
        COALESCE(LANGUAGE, 'NA')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.JTF_TASK_STATUSES_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.JTF_TASK_STATUSES_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (TASK_STATUS_ID, LANGUAGE) IN (
    SELECT
      TASK_STATUS_ID,
      LANGUAGE
    FROM bec_raw_dl_ext.JTF_TASK_STATUSES_TL
    WHERE
      (TASK_STATUS_ID, LANGUAGE, KCA_SEQ_ID) IN (
        SELECT
          TASK_STATUS_ID,
          LANGUAGE,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.JTF_TASK_STATUSES_TL
        GROUP BY
          TASK_STATUS_ID,
          LANGUAGE
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'jtf_task_statuses_tl';