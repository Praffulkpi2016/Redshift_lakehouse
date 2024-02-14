TRUNCATE table bronze_bec_ods_stg.ap_distribution_sets_all;
INSERT INTO bronze_bec_ods_stg.ap_distribution_sets_all (
  distribution_set_id,
  distribution_set_name,
  last_update_date,
  last_updated_by,
  description,
  total_percent_distribution,
  inactive_date,
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
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    distribution_set_id,
    distribution_set_name,
    last_update_date,
    last_updated_by,
    description,
    total_percent_distribution,
    inactive_date,
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
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.ap_distribution_sets_all
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (distribution_set_id, KCA_SEQ_ID) IN (
      SELECT
        distribution_set_id,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.AP_DISTRIBUTION_SETS_ALL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        distribution_set_id
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ap_distribution_sets_all'
    )
);