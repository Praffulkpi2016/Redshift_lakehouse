TRUNCATE table
	table bronze_bec_ods_stg.AP_PAY_GROUP;
INSERT INTO bronze_bec_ods_stg.AP_PAY_GROUP (
  pay_group_id,
  vendor_pay_group,
  template_id,
  checkrun_id,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  last_update_login,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    pay_group_id,
    vendor_pay_group,
    template_id,
    checkrun_id,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.AP_PAY_GROUP
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (pay_group_id, KCA_SEQ_ID) IN (
      SELECT
        pay_group_id,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.AP_PAY_GROUP
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        pay_group_id
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ap_pay_group'
    )
);