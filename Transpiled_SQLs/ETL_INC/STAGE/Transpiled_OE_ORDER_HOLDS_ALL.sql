TRUNCATE table bronze_bec_ods_stg.OE_ORDER_HOLDS_ALL;
INSERT INTO bronze_bec_ods_stg.OE_ORDER_HOLDS_ALL (
  ORDER_HOLD_ID,
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
  HOLD_RELEASE_ID,
  HEADER_ID,
  LINE_ID,
  ORG_ID,
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
  RELEASED_FLAG,
  INST_ID,
  CREDIT_PROFILE_LEVEL,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    ORDER_HOLD_ID,
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
    HOLD_RELEASE_ID,
    HEADER_ID,
    LINE_ID,
    ORG_ID,
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
    RELEASED_FLAG,
    INST_ID,
    CREDIT_PROFILE_LEVEL,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.OE_ORDER_HOLDS_ALL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(ORDER_HOLD_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(ORDER_HOLD_ID, 0),
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.OE_ORDER_HOLDS_ALL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(ORDER_HOLD_ID, 0)
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'oe_order_holds_all'
      )
    )
);