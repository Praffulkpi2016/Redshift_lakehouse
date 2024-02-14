TRUNCATE table bronze_bec_ods_stg.GL_LE_VALUE_SETS;
INSERT INTO bronze_bec_ods_stg.GL_LE_VALUE_SETS (
  LEGAL_ENTITY_ID,
  FLEX_VALUE_SET_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  CREATION_DATE,
  CREATED_BY,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    LEGAL_ENTITY_ID,
    FLEX_VALUE_SET_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    CREATION_DATE,
    CREATED_BY,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.GL_LE_VALUE_SETS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(LEGAL_ENTITY_ID, 0), COALESCE(FLEX_VALUE_SET_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(LEGAL_ENTITY_ID, 0),
        COALESCE(FLEX_VALUE_SET_ID, 0),
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.GL_LE_VALUE_SETS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(LEGAL_ENTITY_ID, 0),
        COALESCE(FLEX_VALUE_SET_ID, 0)
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'gl_le_value_sets'
      )
    )
);