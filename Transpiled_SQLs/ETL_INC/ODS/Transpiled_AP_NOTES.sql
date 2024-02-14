/* Delete Records */
DELETE FROM silver_bec_ods.ap_notes
WHERE
  NOTE_ID IN (
    SELECT
      stg.NOTE_ID
    FROM silver_bec_ods.ap_notes AS ods, bronze_bec_ods_stg.ap_notes AS stg
    WHERE
      ods.NOTE_ID = stg.NOTE_ID AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.AP_NOTES (
  NOTE_ID,
  SOURCE_OBJECT_CODE,
  SOURCE_OBJECT_ID,
  NOTE_TYPE,
  NOTES_DETAIL,
  ENTERED_BY,
  ENTERED_DATE,
  SOURCE_LANG,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  NOTE_SOURCE,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    note_id,
    source_object_code,
    source_object_id,
    note_type,
    notes_detail,
    entered_by,
    entered_date,
    source_lang,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    NOTE_SOURCE,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.ap_notes
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (NOTE_ID, kca_seq_id) IN (
      SELECT
        NOTE_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.ap_notes
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        NOTE_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.ap_notes SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.ap_notes SET IS_DELETED_FLG = 'Y'
WHERE
  (
    NOTE_ID
  ) IN (
    SELECT
      NOTE_ID
    FROM bec_raw_dl_ext.ap_notes
    WHERE
      (NOTE_ID, KCA_SEQ_ID) IN (
        SELECT
          NOTE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.ap_notes
        GROUP BY
          NOTE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_notes';