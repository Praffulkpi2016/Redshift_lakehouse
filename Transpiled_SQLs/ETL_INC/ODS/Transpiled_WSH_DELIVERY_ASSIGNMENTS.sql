/* Delete Records */
DELETE FROM silver_bec_ods.WSH_DELIVERY_ASSIGNMENTS
WHERE
  (
    COALESCE(DELIVERY_ASSIGNMENT_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.DELIVERY_ASSIGNMENT_ID, 0) AS DELIVERY_ASSIGNMENT_ID
    FROM silver_bec_ods.WSH_DELIVERY_ASSIGNMENTS AS ods, bronze_bec_ods_stg.WSH_DELIVERY_ASSIGNMENTS AS stg
    WHERE
      COALESCE(ods.DELIVERY_ASSIGNMENT_ID, 0) = COALESCE(stg.DELIVERY_ASSIGNMENT_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.WSH_DELIVERY_ASSIGNMENTS (
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
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
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
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.WSH_DELIVERY_ASSIGNMENTS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(DELIVERY_ASSIGNMENT_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(DELIVERY_ASSIGNMENT_ID, 0) AS DELIVERY_ASSIGNMENT_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.WSH_DELIVERY_ASSIGNMENTS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        DELIVERY_ASSIGNMENT_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.WSH_DELIVERY_ASSIGNMENTS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.WSH_DELIVERY_ASSIGNMENTS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    DELIVERY_ASSIGNMENT_ID
  ) IN (
    SELECT
      DELIVERY_ASSIGNMENT_ID
    FROM bec_raw_dl_ext.WSH_DELIVERY_ASSIGNMENTS
    WHERE
      (DELIVERY_ASSIGNMENT_ID, KCA_SEQ_ID) IN (
        SELECT
          DELIVERY_ASSIGNMENT_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.WSH_DELIVERY_ASSIGNMENTS
        GROUP BY
          DELIVERY_ASSIGNMENT_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'wsh_delivery_assignments';