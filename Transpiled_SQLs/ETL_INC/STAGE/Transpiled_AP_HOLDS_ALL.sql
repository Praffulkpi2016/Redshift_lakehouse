TRUNCATE table bronze_bec_ods_stg.ap_holds_all;
INSERT INTO bronze_bec_ods_stg.ap_holds_all (
  invoice_id,
  line_location_id,
  hold_lookup_code,
  last_update_date,
  last_updated_by,
  held_by,
  hold_date,
  hold_reason,
  release_lookup_code,
  release_reason,
  status_flag,
  last_update_login,
  creation_date,
  created_by,
  attribute_category,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  org_id,
  responsibility_id,
  rcv_transaction_id,
  hold_details,
  line_number,
  hold_id,
  wf_status,
  validation_request_id,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    invoice_id,
    line_location_id,
    hold_lookup_code,
    last_update_date,
    last_updated_by,
    held_by,
    hold_date,
    hold_reason,
    release_lookup_code,
    release_reason,
    status_flag,
    last_update_login,
    creation_date,
    created_by,
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    org_id,
    responsibility_id,
    rcv_transaction_id,
    hold_details,
    line_number,
    hold_id,
    wf_status,
    validation_request_id,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.ap_holds_all
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (hold_id, KCA_SEQ_ID) IN (
      SELECT
        hold_id,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.ap_holds_all
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        hold_id
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ap_holds_all'
    )
);