TRUNCATE table bronze_bec_ods_stg.GL_ENCUMBRANCE_TYPES;
INSERT INTO bronze_bec_ods_stg.GL_ENCUMBRANCE_TYPES (
  encumbrance_type_id,
  encumbrance_type,
  enabled_flag,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  description,
  encumbrance_type_key,
  zd_edition_name,
  zd_sync,
  kca_operation,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    encumbrance_type_id,
    encumbrance_type,
    enabled_flag,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    description,
    encumbrance_type_key,
    zd_edition_name,
    zd_sync,
    kca_operation,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.GL_ENCUMBRANCE_TYPES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (ENCUMBRANCE_TYPE_ID, kca_seq_id) IN (
      SELECT
        ENCUMBRANCE_TYPE_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.GL_ENCUMBRANCE_TYPES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        ENCUMBRANCE_TYPE_ID
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'gl_encumbrance_types'
      )
    )
);