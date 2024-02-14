TRUNCATE table bronze_bec_ods_stg.jtf_rs_salesreps;
INSERT INTO bronze_bec_ods_stg.jtf_rs_salesreps (
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
  KCA_SEQ_ID, /* 'N' as IS_DELETED_FLG, */
  kca_seq_date
FROM bec_raw_dl_ext.JTF_RS_SALESREPS
WHERE
  kca_operation <> 'DELETE'
  AND COALESCE(kca_seq_id, '') <> ''
  AND (SALESREP_ID, COALESCE(ORG_ID, 0), kca_seq_id) IN (
    SELECT
      SALESREP_ID,
      COALESCE(ORG_ID, 0),
      MAX(kca_seq_id)
    FROM bec_raw_dl_ext.jtf_rs_salesreps
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
    GROUP BY
      SALESREP_ID,
      COALESCE(ORG_ID, 0)
  )
  AND (
    kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'jtf_rs_salesreps'
    )
  );