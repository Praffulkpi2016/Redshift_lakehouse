TRUNCATE table
	table bronze_bec_ods_stg.ALR_OUTPUT_HISTORY;
INSERT INTO bronze_bec_ods_stg.alr_output_history (
  APPLICATION_ID,
  NAME,
  CHECK_ID,
  ROW_NUMBER,
  DATA_TYPE,
  VALUE,
  SECURITY_GROUP_ID,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    APPLICATION_ID,
    NAME,
    CHECK_ID,
    ROW_NUMBER,
    DATA_TYPE,
    VALUE,
    SECURITY_GROUP_ID,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.alr_output_history
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(APPLICATION_ID, 0), COALESCE(CHECK_ID, 0), COALESCE(ROW_NUMBER, 0), COALESCE(NAME, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(APPLICATION_ID, 0) AS APPLICATION_ID,
        COALESCE(CHECK_ID, 0) AS CHECK_ID,
        COALESCE(ROW_NUMBER, 0) AS ROW_NUMBER,
        COALESCE(NAME, 'NA') AS NAME,
        MAX(kca_seq_id) AS kca_seq_id
      FROM bec_raw_dl_ext.alr_output_history
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(APPLICATION_ID, 0),
        COALESCE(CHECK_ID, 0),
        COALESCE(ROW_NUMBER, 0),
        COALESCE(NAME, 'NA')
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'alr_output_history'
    )
);