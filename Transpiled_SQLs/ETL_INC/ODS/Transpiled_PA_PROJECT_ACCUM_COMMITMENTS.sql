/* Delete Records */
DELETE FROM silver_bec_ods.pa_project_accum_commitments
WHERE
  (
    COALESCE(PROJECT_ACCUM_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.PROJECT_ACCUM_ID, 0) AS PROJECT_ACCUM_ID
    FROM silver_bec_ods.pa_project_accum_commitments AS ods, bronze_bec_ods_stg.pa_project_accum_commitments AS stg
    WHERE
      COALESCE(ods.PROJECT_ACCUM_ID, 0) = COALESCE(stg.PROJECT_ACCUM_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.pa_project_accum_commitments (
  PROJECT_ACCUM_ID,
  CMT_RAW_COST_ITD,
  CMT_RAW_COST_YTD,
  CMT_RAW_COST_PP,
  CMT_RAW_COST_PTD,
  CMT_BURDENED_COST_ITD,
  CMT_BURDENED_COST_YTD,
  CMT_BURDENED_COST_PP,
  CMT_BURDENED_COST_PTD,
  CMT_QUANTITY_ITD,
  CMT_QUANTITY_YTD,
  CMT_QUANTITY_PP,
  CMT_QUANTITY_PTD,
  CMT_UNIT_OF_MEASURE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    PROJECT_ACCUM_ID,
    CMT_RAW_COST_ITD,
    CMT_RAW_COST_YTD,
    CMT_RAW_COST_PP,
    CMT_RAW_COST_PTD,
    CMT_BURDENED_COST_ITD,
    CMT_BURDENED_COST_YTD,
    CMT_BURDENED_COST_PP,
    CMT_BURDENED_COST_PTD,
    CMT_QUANTITY_ITD,
    CMT_QUANTITY_YTD,
    CMT_QUANTITY_PP,
    CMT_QUANTITY_PTD,
    CMT_UNIT_OF_MEASURE,
    LAST_UPDATED_BY,
    LAST_UPDATE_DATE,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    REQUEST_ID,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.pa_project_accum_commitments
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(PROJECT_ACCUM_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(PROJECT_ACCUM_ID, 0) AS PROJECT_ACCUM_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.pa_project_accum_commitments
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(PROJECT_ACCUM_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.pa_project_accum_commitments SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.pa_project_accum_commitments SET IS_DELETED_FLG = 'Y'
WHERE
  (
    PROJECT_ACCUM_ID
  ) IN (
    SELECT
      PROJECT_ACCUM_ID
    FROM bec_raw_dl_ext.pa_project_accum_commitments
    WHERE
      (PROJECT_ACCUM_ID, KCA_SEQ_ID) IN (
        SELECT
          PROJECT_ACCUM_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.pa_project_accum_commitments
        GROUP BY
          PROJECT_ACCUM_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'pa_project_accum_commitments';