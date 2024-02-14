/* Delete Records */
DELETE FROM silver_bec_ods.ap_holds_all
WHERE
  hold_id IN (
    SELECT
      stg.hold_id
    FROM silver_bec_ods.ap_holds_all AS ods, bronze_bec_ods_stg.ap_holds_all AS stg
    WHERE
      ods.hold_id = stg.hold_id AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.ap_holds_all (
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
  IS_DELETED_FLG,
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
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.ap_holds_all
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (hold_id, KCA_SEQ_ID) IN (
      SELECT
        hold_id,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.ap_holds_all
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        hold_id
    )
);
/* Soft delete */
UPDATE silver_bec_ods.ap_holds_all SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.ap_holds_all SET IS_DELETED_FLG = 'Y'
WHERE
  (
    hold_id
  ) IN (
    SELECT
      hold_id
    FROM bec_raw_dl_ext.ap_holds_all
    WHERE
      (hold_id, KCA_SEQ_ID) IN (
        SELECT
          hold_id,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.ap_holds_all
        GROUP BY
          hold_id
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_holds_all';