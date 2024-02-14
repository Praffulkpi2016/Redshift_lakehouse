/* Delete Records */
DELETE FROM silver_bec_ods.ORG_ACCESS
WHERE
  (COALESCE(RESP_APPLICATION_ID, 0), COALESCE(RESPONSIBILITY_ID, 0), COALESCE(ORGANIZATION_ID, 0)) IN (
    SELECT
      COALESCE(stg.RESP_APPLICATION_ID, 0) AS RESP_APPLICATION_ID,
      COALESCE(stg.RESPONSIBILITY_ID, 0) AS RESPONSIBILITY_ID,
      COALESCE(stg.ORGANIZATION_ID, 0) AS ORGANIZATION_ID
    FROM silver_bec_ods.ORG_ACCESS AS ods, bronze_bec_ods_stg.ORG_ACCESS AS stg
    WHERE
      COALESCE(ods.RESP_APPLICATION_ID, 0) = COALESCE(stg.RESP_APPLICATION_ID, 0)
      AND COALESCE(ods.RESPONSIBILITY_ID, 0) = COALESCE(stg.RESPONSIBILITY_ID, 0)
      AND COALESCE(ods.ORGANIZATION_ID, 0) = COALESCE(stg.ORGANIZATION_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.ORG_ACCESS (
  organization_id,
  resp_application_id,
  responsibility_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  disable_date,
  comments,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    organization_id,
    resp_application_id,
    responsibility_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    disable_date,
    comments,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.ORG_ACCESS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(RESP_APPLICATION_ID, 0), COALESCE(RESPONSIBILITY_ID, 0), COALESCE(ORGANIZATION_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(RESP_APPLICATION_ID, 0) AS RESP_APPLICATION_ID,
        COALESCE(RESPONSIBILITY_ID, 0) AS RESPONSIBILITY_ID,
        COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.ORG_ACCESS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(RESP_APPLICATION_ID, 0),
        COALESCE(RESPONSIBILITY_ID, 0),
        COALESCE(ORGANIZATION_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.ORG_ACCESS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.ORG_ACCESS SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(RESP_APPLICATION_ID, 0), COALESCE(RESPONSIBILITY_ID, 0), COALESCE(ORGANIZATION_ID, 0)) IN (
    SELECT
      COALESCE(RESP_APPLICATION_ID, 0),
      COALESCE(RESPONSIBILITY_ID, 0),
      COALESCE(ORGANIZATION_ID, 0)
    FROM bec_raw_dl_ext.ORG_ACCESS
    WHERE
      (COALESCE(RESP_APPLICATION_ID, 0), COALESCE(RESPONSIBILITY_ID, 0), COALESCE(ORGANIZATION_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(RESP_APPLICATION_ID, 0),
          COALESCE(RESPONSIBILITY_ID, 0),
          COALESCE(ORGANIZATION_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.ORG_ACCESS
        GROUP BY
          COALESCE(RESP_APPLICATION_ID, 0),
          COALESCE(RESPONSIBILITY_ID, 0),
          COALESCE(ORGANIZATION_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'org_access';