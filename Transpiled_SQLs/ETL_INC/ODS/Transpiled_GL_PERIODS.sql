/* Delete Records */
DELETE FROM silver_bec_ods.gl_periods
WHERE
  (PERIOD_SET_NAME, PERIOD_NAME) IN (
    SELECT
      stg.PERIOD_SET_NAME,
      stg.PERIOD_NAME
    FROM silver_bec_ods.gl_periods AS ods, bronze_bec_ods_stg.gl_periods AS stg
    WHERE
      ods.PERIOD_SET_NAME = stg.PERIOD_SET_NAME
      AND ods.PERIOD_NAME = stg.PERIOD_NAME
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.gl_periods (
  PERIOD_SET_NAME,
  PERIOD_NAME,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  START_DATE,
  END_DATE,
  PERIOD_TYPE,
  PERIOD_YEAR,
  PERIOD_NUM,
  QUARTER_NUM,
  ENTERED_PERIOD_NAME,
  ADJUSTMENT_PERIOD_FLAG,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  DESCRIPTION,
  ATTRIBUTE1,
  ATTRIBUTE2,
  ATTRIBUTE3,
  ATTRIBUTE4,
  ATTRIBUTE5,
  ATTRIBUTE6,
  ATTRIBUTE7,
  ATTRIBUTE8,
  CONTEXT,
  YEAR_START_DATE,
  QUARTER_START_DATE,
  ZD_EDITION_NAME,
  ZD_SYNC,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    PERIOD_SET_NAME,
    PERIOD_NAME,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    START_DATE,
    END_DATE,
    PERIOD_TYPE,
    PERIOD_YEAR,
    PERIOD_NUM,
    QUARTER_NUM,
    ENTERED_PERIOD_NAME,
    ADJUSTMENT_PERIOD_FLAG,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    DESCRIPTION,
    ATTRIBUTE1,
    ATTRIBUTE2,
    ATTRIBUTE3,
    ATTRIBUTE4,
    ATTRIBUTE5,
    ATTRIBUTE6,
    ATTRIBUTE7,
    ATTRIBUTE8,
    CONTEXT,
    YEAR_START_DATE,
    QUARTER_START_DATE,
    ZD_EDITION_NAME,
    ZD_SYNC,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.gl_periods
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (PERIOD_SET_NAME, PERIOD_NAME, kca_seq_id) IN (
      SELECT
        PERIOD_SET_NAME,
        PERIOD_NAME,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.gl_periods
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        PERIOD_SET_NAME,
        PERIOD_NAME
    )
);
/* Soft Delete */
UPDATE silver_bec_ods.gl_periods SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.gl_periods SET IS_DELETED_FLG = 'Y'
WHERE
  (PERIOD_SET_NAME, PERIOD_NAME) IN (
    SELECT
      PERIOD_SET_NAME,
      PERIOD_NAME
    FROM bec_raw_dl_ext.gl_periods
    WHERE
      (PERIOD_SET_NAME, PERIOD_NAME, KCA_SEQ_ID) IN (
        SELECT
          PERIOD_SET_NAME,
          PERIOD_NAME,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.gl_periods
        GROUP BY
          PERIOD_SET_NAME,
          PERIOD_NAME
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_periods';