TRUNCATE table bronze_bec_ods_stg.AP_NOTES;
INSERT INTO bronze_bec_ods_stg.AP_NOTES (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.AP_NOTES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (NOTE_ID, kca_seq_id) IN (
      SELECT
        NOTE_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.AP_NOTES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        NOTE_ID
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ap_notes'
    )
);