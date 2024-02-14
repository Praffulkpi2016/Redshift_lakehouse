TRUNCATE table bronze_bec_ods_stg.oe_hold_definitions;
INSERT INTO bronze_bec_ods_stg.oe_hold_definitions (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.oe_hold_definitions
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(HOLD_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(HOLD_ID, 0) AS HOLD_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.oe_hold_definitions
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(HOLD_ID, 0)
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'oe_hold_definitions'
      )
    )
);