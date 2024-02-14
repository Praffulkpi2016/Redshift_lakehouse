TRUNCATE table bronze_bec_ods_stg.pa_project_accum_commitments;
INSERT INTO bronze_bec_ods_stg.pa_project_accum_commitments (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.pa_project_accum_commitments
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(PROJECT_ACCUM_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(PROJECT_ACCUM_ID, 0) AS PROJECT_ACCUM_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.pa_project_accum_commitments
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(PROJECT_ACCUM_ID, 0)
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'pa_project_accum_commitments'
      )
    )
);