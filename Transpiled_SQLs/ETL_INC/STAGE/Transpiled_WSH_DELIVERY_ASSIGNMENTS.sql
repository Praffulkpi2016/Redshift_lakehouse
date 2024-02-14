TRUNCATE table
	table bronze_bec_ods_stg.WSH_DELIVERY_ASSIGNMENTS;
INSERT INTO bronze_bec_ods_stg.WSH_DELIVERY_ASSIGNMENTS (
  delivery_assignment_id,
  delivery_id,
  parent_delivery_id,
  delivery_detail_id,
  parent_delivery_detail_id,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  last_update_login,
  program_application_id,
  program_id,
  program_update_date,
  request_id,
  active_flag,
  `type`,
  KCA_OPERATION,
  KCA_SEQ_ID,
  KCA_SEQ_DATE
)
(
  SELECT
    delivery_assignment_id,
    delivery_id,
    parent_delivery_id,
    delivery_detail_id,
    parent_delivery_detail_id,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    program_application_id,
    program_id,
    program_update_date,
    request_id,
    active_flag,
    `type`,
    KCA_OPERATION,
    KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.WSH_DELIVERY_ASSIGNMENTS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(DELIVERY_ASSIGNMENT_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(DELIVERY_ASSIGNMENT_ID, 0) AS DELIVERY_ASSIGNMENT_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.WSH_DELIVERY_ASSIGNMENTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        DELIVERY_ASSIGNMENT_ID
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'wsh_delivery_assignments'
      )
    )
);