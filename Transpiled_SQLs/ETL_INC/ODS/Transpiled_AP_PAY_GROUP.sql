/* Delete Records */
DELETE FROM silver_bec_ods.ap_pay_group
WHERE
  pay_group_id IN (
    SELECT
      stg.pay_group_id
    FROM silver_bec_ods.ap_pay_group AS ods, bronze_bec_ods_stg.ap_pay_group AS stg
    WHERE
      ods.pay_group_id = stg.pay_group_id AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.ap_pay_group (
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
  IS_DELETED_FLG,
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
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.ap_pay_group
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (pay_group_id, kca_seq_id) IN (
      SELECT
        pay_group_id,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.ap_pay_group
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        pay_group_id
    )
);
/* Soft delete */
UPDATE silver_bec_ods.ap_pay_group SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.ap_pay_group SET IS_DELETED_FLG = 'Y'
WHERE
  (
    pay_group_id
  ) IN (
    SELECT
      pay_group_id
    FROM bec_raw_dl_ext.ap_pay_group
    WHERE
      (pay_group_id, KCA_SEQ_ID) IN (
        SELECT
          pay_group_id,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.ap_pay_group
        GROUP BY
          pay_group_id
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_pay_group';