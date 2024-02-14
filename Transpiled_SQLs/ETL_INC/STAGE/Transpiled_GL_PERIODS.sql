TRUNCATE table
	table bronze_bec_ods_stg.gl_periods;
INSERT INTO bronze_bec_ods_stg.gl_periods (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.gl_periods
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (PERIOD_SET_NAME, PERIOD_NAME, kca_seq_id) IN (
      SELECT
        PERIOD_SET_NAME,
        PERIOD_NAME,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.GL_PERIODS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        PERIOD_SET_NAME,
        PERIOD_NAME
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'gl_periods'
      )
    )
);