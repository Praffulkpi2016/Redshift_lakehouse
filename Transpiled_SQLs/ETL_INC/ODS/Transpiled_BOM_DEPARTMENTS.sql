/* Delete Records */
DELETE FROM silver_bec_ods.bom_departments
WHERE
  DEPARTMENT_ID IN (
    SELECT
      stg.DEPARTMENT_ID
    FROM silver_bec_ods.bom_departments AS ods, bronze_bec_ods_stg.bom_departments AS stg
    WHERE
      ods.DEPARTMENT_ID = stg.DEPARTMENT_ID AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.bom_departments (
  DEPARTMENT_ID,
  DEPARTMENT_CODE,
  ORGANIZATION_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  DESCRIPTION,
  DISABLE_DATE,
  DEPARTMENT_CLASS_CODE,
  ATTRIBUTE_CATEGORY,
  ATTRIBUTE1,
  ATTRIBUTE2,
  ATTRIBUTE3,
  ATTRIBUTE4,
  ATTRIBUTE5,
  ATTRIBUTE6,
  ATTRIBUTE7,
  ATTRIBUTE8,
  ATTRIBUTE9,
  ATTRIBUTE10,
  ATTRIBUTE11,
  ATTRIBUTE12,
  ATTRIBUTE13,
  ATTRIBUTE14,
  ATTRIBUTE15,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  LOCATION_ID,
  PA_EXPENDITURE_ORG_ID,
  SCRAP_ACCOUNT,
  EST_ABSORPTION_ACCOUNT,
  MAINT_COST_CATEGORY,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    DEPARTMENT_ID,
    DEPARTMENT_CODE,
    ORGANIZATION_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    DESCRIPTION,
    DISABLE_DATE,
    DEPARTMENT_CLASS_CODE,
    ATTRIBUTE_CATEGORY,
    ATTRIBUTE1,
    ATTRIBUTE2,
    ATTRIBUTE3,
    ATTRIBUTE4,
    ATTRIBUTE5,
    ATTRIBUTE6,
    ATTRIBUTE7,
    ATTRIBUTE8,
    ATTRIBUTE9,
    ATTRIBUTE10,
    ATTRIBUTE11,
    ATTRIBUTE12,
    ATTRIBUTE13,
    ATTRIBUTE14,
    ATTRIBUTE15,
    REQUEST_ID,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    LOCATION_ID,
    PA_EXPENDITURE_ORG_ID,
    SCRAP_ACCOUNT,
    EST_ABSORPTION_ACCOUNT,
    MAINT_COST_CATEGORY,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.bom_departments
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (DEPARTMENT_ID, kca_seq_id) IN (
      SELECT
        DEPARTMENT_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.bom_departments
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        DEPARTMENT_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.bom_departments SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.bom_departments SET IS_DELETED_FLG = 'Y'
WHERE
  (
    DEPARTMENT_ID
  ) IN (
    SELECT
      DEPARTMENT_ID
    FROM bec_raw_dl_ext.bom_departments
    WHERE
      (DEPARTMENT_ID, KCA_SEQ_ID) IN (
        SELECT
          DEPARTMENT_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.bom_departments
        GROUP BY
          DEPARTMENT_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'bom_departments';