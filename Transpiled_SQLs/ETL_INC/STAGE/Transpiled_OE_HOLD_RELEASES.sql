TRUNCATE table bronze_bec_ods_stg.oe_hold_releases;
INSERT INTO bronze_bec_ods_stg.oe_hold_releases (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.oe_hold_releases
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(HOLD_RELEASE_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(HOLD_RELEASE_ID, 0) AS HOLD_RELEASE_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.oe_hold_releases
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(HOLD_RELEASE_ID, 0)
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'oe_hold_releases'
      )
    )
);