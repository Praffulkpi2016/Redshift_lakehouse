/* Delete Records */
DELETE FROM silver_bec_ods.oe_hold_definitions
WHERE
  (
    COALESCE(HOLD_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.HOLD_ID, 0) AS HOLD_ID
    FROM silver_bec_ods.oe_hold_definitions AS ods, bronze_bec_ods_stg.oe_hold_definitions AS stg
    WHERE
      COALESCE(ods.HOLD_ID, 0) = COALESCE(stg.HOLD_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.oe_hold_definitions (
  HOLD_ID,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  NAME,
  TYPE_CODE,
  DESCRIPTION,
  START_DATE_ACTIVE,
  END_DATE_ACTIVE,
  ITEM_TYPE,
  ACTIVITY_NAME,
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
  HOLD_INCLUDED_ITEMS_FLAG,
  APPLY_TO_ORDER_AND_LINE_FLAG,
  PROGRESS_WF_ON_RELEASE_FLAG,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    HOLD_ID,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    NAME,
    TYPE_CODE,
    DESCRIPTION,
    START_DATE_ACTIVE,
    END_DATE_ACTIVE,
    ITEM_TYPE,
    ACTIVITY_NAME,
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
    HOLD_INCLUDED_ITEMS_FLAG,
    APPLY_TO_ORDER_AND_LINE_FLAG,
    PROGRESS_WF_ON_RELEASE_FLAG,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.oe_hold_definitions
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(HOLD_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(HOLD_ID, 0) AS HOLD_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.oe_hold_definitions
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(HOLD_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.oe_hold_definitions SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.oe_hold_definitions SET IS_DELETED_FLG = 'Y'
WHERE
  (
    HOLD_ID
  ) IN (
    SELECT
      HOLD_ID
    FROM bec_raw_dl_ext.oe_hold_definitions
    WHERE
      (HOLD_ID, KCA_SEQ_ID) IN (
        SELECT
          HOLD_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.oe_hold_definitions
        GROUP BY
          HOLD_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'oe_hold_definitions';