/* Delete Records */
DELETE FROM silver_bec_ods.ALR_OUTPUT_HISTORY
WHERE
  (COALESCE(APPLICATION_ID, 0), COALESCE(CHECK_ID, 0), COALESCE(ROW_NUMBER, 0), COALESCE(NAME, 'NA')) IN (
    SELECT
      COALESCE(stg.APPLICATION_ID, 0) AS APPLICATION_ID,
      COALESCE(stg.CHECK_ID, 0) AS CHECK_ID,
      COALESCE(stg.ROW_NUMBER, 0) AS ROW_NUMBER,
      COALESCE(stg.NAME, 'NA') AS NAME
    FROM silver_bec_ods.alr_output_history AS ods, bronze_bec_ods_stg.alr_output_history AS stg
    WHERE
      COALESCE(ods.APPLICATION_ID, 0) = COALESCE(stg.APPLICATION_ID, 0)
      AND COALESCE(ods.CHECK_ID, 0) = COALESCE(stg.CHECK_ID, 0)
      AND COALESCE(ods.ROW_NUMBER, 0) = COALESCE(stg.ROW_NUMBER, 0)
      AND COALESCE(ods.NAME, 'NA') = COALESCE(stg.NAME, 'NA')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.alr_output_history (
  APPLICATION_ID,
  NAME,
  CHECK_ID,
  ROW_NUMBER,
  DATA_TYPE,
  VALUE,
  SECURITY_GROUP_ID,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_ID,
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
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.alr_output_history
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(APPLICATION_ID, 0), COALESCE(CHECK_ID, 0), COALESCE(ROW_NUMBER, 0), COALESCE(NAME, 'NA'), kca_seq_ID) IN (
      SELECT
        COALESCE(APPLICATION_ID, 0) AS APPLICATION_ID,
        COALESCE(CHECK_ID, 0) AS CHECK_ID,
        COALESCE(ROW_NUMBER, 0) AS ROW_NUMBER,
        COALESCE(NAME, 'NA') AS NAME,
        MAX(kca_seq_ID) AS kca_seq_ID
      FROM bronze_bec_ods_stg.alr_output_history
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(APPLICATION_ID, 0),
        COALESCE(CHECK_ID, 0),
        COALESCE(ROW_NUMBER, 0),
        COALESCE(NAME, 'NA')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.alr_output_history SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.alr_output_history SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(APPLICATION_ID, 0), COALESCE(CHECK_ID, 0), COALESCE(ROW_NUMBER, 0), COALESCE(NAME, 'NA')) IN (
    SELECT
      APPLICATION_ID,
      CHECK_ID,
      row_number,
      NAME
    FROM bec_raw_dl_ext.alr_output_history
    WHERE
      (APPLICATION_ID, CHECK_ID, row_number, NAME, KCA_SEQ_ID) IN (
        SELECT
          COALESCE(APPLICATION_ID, 0) AS APPLICATION_ID,
          COALESCE(CHECK_ID, 0) AS CHECK_ID,
          COALESCE(ROW_NUMBER, 0) AS ROW_NUMBER,
          COALESCE(NAME, 'NA') AS NAME,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.alr_output_history
        GROUP BY
          COALESCE(APPLICATION_ID, 0),
          COALESCE(CHECK_ID, 0),
          COALESCE(ROW_NUMBER, 0),
          COALESCE(NAME, 'NA')
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'alr_output_history';