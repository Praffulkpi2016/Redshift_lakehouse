/* Delete Records */
DELETE FROM gold_bec_dwh.dim_bom_departments
WHERE
  (
    COALESCE(DEPARTMENT_ID, 0)
  ) IN (
    SELECT
      ods.DEPARTMENT_ID
    FROM gold_bec_dwh.dim_bom_departments AS dw, (
      SELECT
        COALESCE(DEPARTMENT_ID, 0) AS DEPARTMENT_ID
      FROM silver_bec_ods.BOM_DEPARTMENTS
      WHERE
        1 = 1
        AND (
          kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_bom_departments' AND batch_name = 'wip'
          )
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.DEPARTMENT_ID, 0)
  );
/* Insert Records */
INSERT INTO gold_bec_dwh.dim_bom_departments (
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
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
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
    'N' AS is_deleted_flg, /* Audit COLUMNS */
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) AS source_app_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(DEPARTMENT_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM BEC_ODS.BOM_DEPARTMENTS
  WHERE
    1 = 1
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_bom_departments' AND batch_name = 'wip'
      )
    )
);
/* Soft Delete */
UPDATE gold_bec_dwh.dim_bom_departments SET is_deleted_flg = 'Y'
WHERE
  NOT (
    COALESCE(DEPARTMENT_ID, 0)
  ) IN (
    SELECT
      COALESCE(ods.DEPARTMENT_ID, 0) AS DEPARTMENT_ID
    FROM gold_bec_dwh.dim_bom_departments AS dw, silver_bec_ods.BOM_DEPARTMENTS AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.DEPARTMENT_ID, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_bom_departments' AND batch_name = 'wip';