/* Delete Records */
DELETE FROM silver_bec_ods.MRP_ASSIGNMENT_SETS
WHERE
  ASSIGNMENT_SET_ID IN (
    SELECT
      stg.ASSIGNMENT_SET_ID
    FROM silver_bec_ods.MRP_ASSIGNMENT_SETS AS ods, bronze_bec_ods_stg.MRP_ASSIGNMENT_SETS AS stg
    WHERE
      ods.ASSIGNMENT_SET_ID = stg.ASSIGNMENT_SET_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MRP_ASSIGNMENT_SETS (
  ASSIGNMENT_SET_ID,
  ASSIGNMENT_SET_NAME,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  DESCRIPTION,
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
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    ASSIGNMENT_SET_ID,
    ASSIGNMENT_SET_NAME,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    REQUEST_ID,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    DESCRIPTION,
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
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MRP_ASSIGNMENT_SETS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (ASSIGNMENT_SET_ID, kca_seq_id) IN (
      SELECT
        ASSIGNMENT_SET_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.MRP_ASSIGNMENT_SETS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        ASSIGNMENT_SET_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MRP_ASSIGNMENT_SETS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MRP_ASSIGNMENT_SETS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    ASSIGNMENT_SET_ID
  ) IN (
    SELECT
      ASSIGNMENT_SET_ID
    FROM bec_raw_dl_ext.MRP_ASSIGNMENT_SETS
    WHERE
      (ASSIGNMENT_SET_ID, KCA_SEQ_ID) IN (
        SELECT
          ASSIGNMENT_SET_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MRP_ASSIGNMENT_SETS
        GROUP BY
          ASSIGNMENT_SET_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mrp_assignment_sets';