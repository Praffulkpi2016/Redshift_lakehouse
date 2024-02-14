/* Delete Records */
DELETE FROM silver_bec_ods.GL_CURRENCIES
WHERE
  currency_code IN (
    SELECT
      stg.currency_code
    FROM silver_bec_ods.GL_CURRENCIES AS ods, bronze_bec_ods_stg.GL_CURRENCIES AS stg
    WHERE
      ods.currency_code = stg.currency_code AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.GL_CURRENCIES (
  currency_code,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  enabled_flag,
  currency_flag,
  description,
  issuing_territory_code,
  `precision`,
  extended_precision,
  symbol,
  start_date_active,
  end_date_active,
  minimum_accountable_unit,
  context,
  iso_flag,
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
  global_attribute_category,
  global_attribute1,
  global_attribute2,
  global_attribute3,
  global_attribute4,
  global_attribute5,
  global_attribute6,
  global_attribute7,
  global_attribute8,
  global_attribute9,
  global_attribute10,
  global_attribute11,
  global_attribute12,
  global_attribute13,
  global_attribute14,
  global_attribute15,
  global_attribute16,
  global_attribute17,
  global_attribute18,
  global_attribute19,
  global_attribute20,
  derive_effective,
  derive_type,
  derive_factor,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  currency_code,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  enabled_flag,
  currency_flag,
  description,
  issuing_territory_code,
  `precision`,
  extended_precision,
  symbol,
  start_date_active,
  end_date_active,
  minimum_accountable_unit,
  context,
  iso_flag,
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
  global_attribute_category,
  global_attribute1,
  global_attribute2,
  global_attribute3,
  global_attribute4,
  global_attribute5,
  global_attribute6,
  global_attribute7,
  global_attribute8,
  global_attribute9,
  global_attribute10,
  global_attribute11,
  global_attribute12,
  global_attribute13,
  global_attribute14,
  global_attribute15,
  global_attribute16,
  global_attribute17,
  global_attribute18,
  global_attribute19,
  global_attribute20,
  derive_effective,
  derive_type,
  derive_factor,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.GL_CURRENCIES
WHERE
  kca_operation IN ('INSERT', 'UPDATE')
  AND (currency_code, kca_seq_id) IN (
    SELECT
      currency_code,
      MAX(kca_seq_id)
    FROM bronze_bec_ods_stg.GL_CURRENCIES
    WHERE
      kca_operation IN ('INSERT', 'UPDATE')
    GROUP BY
      currency_code
  );
/* Soft Delete */
UPDATE silver_bec_ods.GL_CURRENCIES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.GL_CURRENCIES SET IS_DELETED_FLG = 'Y'
WHERE
  (
    currency_code
  ) IN (
    SELECT
      currency_code
    FROM bec_raw_dl_ext.fnd_currencies
    WHERE
      (currency_code, KCA_SEQ_ID) IN (
        SELECT
          currency_code,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.fnd_currencies
        GROUP BY
          currency_code
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_currencies';