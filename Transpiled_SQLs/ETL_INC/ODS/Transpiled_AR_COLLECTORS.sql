/* Delete Records */
DELETE FROM silver_bec_ods.AR_COLLECTORS
WHERE
  collector_id IN (
    SELECT
      stg.collector_id
    FROM silver_bec_ods.AR_COLLECTORS AS ods, bronze_bec_ods_stg.AR_COLLECTORS AS stg
    WHERE
      ods.collector_id = stg.collector_id AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.AR_COLLECTORS (
  collector_id,
  last_updated_by,
  last_update_date,
  last_update_login,
  creation_date,
  created_by,
  `name`,
  employee_id,
  description,
  status,
  inactive_date,
  alias,
  telephone_number,
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
  resource_id,
  resource_type,
  zd_edition_name,
  zd_sync,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    collector_id,
    last_updated_by,
    last_update_date,
    last_update_login,
    creation_date,
    created_by,
    `name`,
    employee_id,
    description,
    status,
    inactive_date,
    alias,
    telephone_number,
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
    resource_id,
    resource_type,
    zd_edition_name,
    zd_sync,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.AR_COLLECTORS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (collector_id, kca_seq_id) IN (
      SELECT
        collector_id,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.AR_COLLECTORS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        collector_id
    )
);
/* Soft delete */
UPDATE silver_bec_ods.AR_COLLECTORS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.AR_COLLECTORS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    collector_id
  ) IN (
    SELECT
      collector_id
    FROM bec_raw_dl_ext.AR_COLLECTORS
    WHERE
      (collector_id, KCA_SEQ_ID) IN (
        SELECT
          collector_id,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.AR_COLLECTORS
        GROUP BY
          collector_id
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ar_collectors';