TRUNCATE table bronze_bec_ods_stg.GL_LEGAL_ENTITIES_BSVS;
INSERT INTO bronze_bec_ods_stg.GL_LEGAL_ENTITIES_BSVS (
  LEGAL_ENTITY_ID,
  FLEX_VALUE_SET_ID,
  FLEX_SEGMENT_VALUE,
  START_DATE,
  END_DATE,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  CREATION_DATE,
  CREATED_BY,
  KCA_OPERATION,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    LEGAL_ENTITY_ID,
    FLEX_VALUE_SET_ID,
    FLEX_SEGMENT_VALUE,
    START_DATE,
    END_DATE,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    CREATION_DATE,
    CREATED_BY,
    KCA_OPERATION,
    KCA_SEQ_ID,
    kca_seq_date
  FROM bec_raw_dl_ext.GL_LEGAL_ENTITIES_BSVS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (LEGAL_ENTITY_ID, FLEX_VALUE_SET_ID, FLEX_SEGMENT_VALUE, KCA_SEQ_ID) IN (
      SELECT
        LEGAL_ENTITY_ID,
        FLEX_VALUE_SET_ID,
        FLEX_SEGMENT_VALUE,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.GL_LEGAL_ENTITIES_BSVS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        LEGAL_ENTITY_ID,
        FLEX_VALUE_SET_ID,
        FLEX_SEGMENT_VALUE
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'gl_legal_entities_bsvs'
      )
    )
);