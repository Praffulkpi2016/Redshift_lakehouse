/* Delete Records */
DELETE FROM silver_bec_ods.oe_hold_releases
WHERE
  (
    COALESCE(HOLD_RELEASE_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.HOLD_RELEASE_ID, 0) AS HOLD_RELEASE_ID
    FROM silver_bec_ods.oe_hold_releases AS ods, bronze_bec_ods_stg.oe_hold_releases AS stg
    WHERE
      COALESCE(ods.HOLD_RELEASE_ID, 0) = COALESCE(stg.HOLD_RELEASE_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.oe_hold_releases (
  HOLD_RELEASE_ID,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  REQUEST_ID,
  HOLD_SOURCE_ID,
  RELEASE_REASON_CODE,
  RELEASE_COMMENT,
  CONTEXT,
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
  ORDER_HOLD_ID,
  RELEASED_ORDER_AMOUNT,
  RELEASED_CURR_CODE,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    HOLD_RELEASE_ID,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    REQUEST_ID,
    HOLD_SOURCE_ID,
    RELEASE_REASON_CODE,
    RELEASE_COMMENT,
    CONTEXT,
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
    ORDER_HOLD_ID,
    RELEASED_ORDER_AMOUNT,
    RELEASED_CURR_CODE,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.oe_hold_releases
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(HOLD_RELEASE_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(HOLD_RELEASE_ID, 0) AS HOLD_RELEASE_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.oe_hold_releases
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(HOLD_RELEASE_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.oe_hold_releases SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.oe_hold_releases SET IS_DELETED_FLG = 'Y'
WHERE
  (
    HOLD_RELEASE_ID
  ) IN (
    SELECT
      HOLD_RELEASE_ID
    FROM bec_raw_dl_ext.oe_hold_releases
    WHERE
      (HOLD_RELEASE_ID, KCA_SEQ_ID) IN (
        SELECT
          HOLD_RELEASE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.oe_hold_releases
        GROUP BY
          HOLD_RELEASE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'oe_hold_releases';