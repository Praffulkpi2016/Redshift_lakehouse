/* Delete Records */
DELETE FROM silver_bec_ods.jtf_rs_salesreps
WHERE
  (SALESREP_ID, COALESCE(ORG_ID, 0)) IN (
    SELECT
      stg.SALESREP_ID,
      COALESCE(stg.ORG_ID, 0)
    FROM silver_bec_ods.jtf_rs_salesreps AS ods, bronze_bec_ods_stg.jtf_rs_salesreps AS stg
    WHERE
      ods.SALESREP_ID = stg.SALESREP_ID
      AND COALESCE(ods.ORG_ID, 0) = COALESCE(stg.ORG_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.jtf_rs_salesreps (
  salesrep_id,
  resource_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  sales_credit_type_id,
  `name`,
  status,
  start_date_active,
  end_date_active,
  gl_id_rev,
  gl_id_freight,
  gl_id_rec,
  set_of_books_id,
  salesrep_number,
  org_id,
  email_address,
  wh_update_date,
  person_id,
  sales_tax_geocode,
  sales_tax_inside_city_limits,
  object_version_number,
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
  security_group_id,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  salesrep_id,
  resource_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  sales_credit_type_id,
  `name`,
  status,
  start_date_active,
  end_date_active,
  gl_id_rev,
  gl_id_freight,
  gl_id_rec,
  set_of_books_id,
  salesrep_number,
  org_id,
  email_address,
  wh_update_date,
  person_id,
  sales_tax_geocode,
  sales_tax_inside_city_limits,
  object_version_number,
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
  security_group_id,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.jtf_rs_salesreps
WHERE
  kca_operation IN ('INSERT', 'UPDATE')
  AND (SALESREP_ID, COALESCE(ORG_ID, 0), kca_seq_id) IN (
    SELECT
      SALESREP_ID,
      COALESCE(ORG_ID, 0),
      MAX(kca_seq_id)
    FROM bronze_bec_ods_stg.jtf_rs_salesreps
    WHERE
      kca_operation IN ('INSERT', 'UPDATE')
    GROUP BY
      SALESREP_ID,
      COALESCE(ORG_ID, 0)
  );
/* Soft delete */
UPDATE silver_bec_ods.jtf_rs_salesreps SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.jtf_rs_salesreps SET IS_DELETED_FLG = 'Y'
WHERE
  (SALESREP_ID, COALESCE(ORG_ID, 0)) IN (
    SELECT
      SALESREP_ID,
      COALESCE(ORG_ID, 0)
    FROM bec_raw_dl_ext.jtf_rs_salesreps
    WHERE
      (SALESREP_ID, COALESCE(ORG_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          SALESREP_ID,
          COALESCE(ORG_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.jtf_rs_salesreps
        GROUP BY
          SALESREP_ID,
          COALESCE(ORG_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'jtf_rs_salesreps';