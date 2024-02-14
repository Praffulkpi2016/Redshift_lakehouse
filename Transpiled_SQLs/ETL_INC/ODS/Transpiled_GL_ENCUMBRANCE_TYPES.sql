/* Delete Records */
DELETE FROM silver_bec_ods.GL_ENCUMBRANCE_TYPES
WHERE
  (
    ENCUMBRANCE_TYPE_ID
  ) IN (
    SELECT
      stg.ENCUMBRANCE_TYPE_ID
    FROM silver_bec_ods.GL_ENCUMBRANCE_TYPES AS ods, bronze_bec_ods_stg.GL_ENCUMBRANCE_TYPES AS stg
    WHERE
      ods.ENCUMBRANCE_TYPE_ID = stg.ENCUMBRANCE_TYPE_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.GL_ENCUMBRANCE_TYPES (
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
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
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
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.GL_ENCUMBRANCE_TYPES
WHERE
  kca_operation IN ('INSERT', 'UPDATE')
  AND (ENCUMBRANCE_TYPE_ID, kca_seq_id) IN (
    SELECT
      ENCUMBRANCE_TYPE_ID,
      MAX(kca_seq_id)
    FROM bronze_bec_ods_stg.GL_ENCUMBRANCE_TYPES
    WHERE
      kca_operation IN ('INSERT', 'UPDATE')
    GROUP BY
      ENCUMBRANCE_TYPE_ID
  );
/* Soft delete */
UPDATE silver_bec_ods.GL_ENCUMBRANCE_TYPES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.GL_ENCUMBRANCE_TYPES SET IS_DELETED_FLG = 'Y'
WHERE
  (
    ENCUMBRANCE_TYPE_ID
  ) IN (
    SELECT
      ENCUMBRANCE_TYPE_ID
    FROM bec_raw_dl_ext.GL_ENCUMBRANCE_TYPES
    WHERE
      (ENCUMBRANCE_TYPE_ID, KCA_SEQ_ID) IN (
        SELECT
          ENCUMBRANCE_TYPE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.GL_ENCUMBRANCE_TYPES
        GROUP BY
          ENCUMBRANCE_TYPE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_encumbrance_types';