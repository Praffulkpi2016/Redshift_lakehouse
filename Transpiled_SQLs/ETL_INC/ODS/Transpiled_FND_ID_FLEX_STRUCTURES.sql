/* Delete Records */
DELETE FROM silver_bec_ods.FND_ID_FLEX_STRUCTURES
WHERE
  (APPLICATION_ID, ID_FLEX_CODE, ID_FLEX_NUM) IN (
    SELECT
      stg.APPLICATION_ID,
      stg.ID_FLEX_CODE,
      stg.ID_FLEX_NUM
    FROM silver_bec_ods.FND_ID_FLEX_STRUCTURES AS ods, bronze_bec_ods_stg.FND_ID_FLEX_STRUCTURES AS stg
    WHERE
      ods.APPLICATION_ID = stg.APPLICATION_ID
      AND ods.ID_FLEX_CODE = stg.ID_FLEX_CODE
      AND ods.ID_FLEX_NUM = stg.ID_FLEX_NUM
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.FND_ID_FLEX_STRUCTURES (
  application_id,
  id_flex_code,
  id_flex_num,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  concatenated_segment_delimiter,
  cross_segment_validation_flag,
  dynamic_inserts_allowed_flag,
  enabled_flag,
  freeze_flex_definition_flag,
  freeze_structured_hier_flag,
  shorthand_enabled_flag,
  shorthand_length,
  structure_view_name,
  id_flex_structure_code,
  ZD_EDITION_NAME,
  ZD_SYNC,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  application_id,
  id_flex_code,
  id_flex_num,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  concatenated_segment_delimiter,
  cross_segment_validation_flag,
  dynamic_inserts_allowed_flag,
  enabled_flag,
  freeze_flex_definition_flag,
  freeze_structured_hier_flag,
  shorthand_enabled_flag,
  shorthand_length,
  structure_view_name,
  id_flex_structure_code,
  ZD_EDITION_NAME,
  ZD_SYNC,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.FND_ID_FLEX_STRUCTURES
WHERE
  kca_operation IN ('INSERT', 'UPDATE')
  AND (APPLICATION_ID, ID_FLEX_CODE, ID_FLEX_NUM, kca_seq_id) IN (
    SELECT
      APPLICATION_ID,
      ID_FLEX_CODE,
      ID_FLEX_NUM,
      MAX(kca_seq_id)
    FROM bronze_bec_ods_stg.FND_ID_FLEX_STRUCTURES
    WHERE
      kca_operation IN ('INSERT', 'UPDATE')
    GROUP BY
      APPLICATION_ID,
      ID_FLEX_CODE,
      ID_FLEX_NUM
  );
/* Soft delete */
UPDATE silver_bec_ods.FND_ID_FLEX_STRUCTURES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.FND_ID_FLEX_STRUCTURES SET IS_DELETED_FLG = 'Y'
WHERE
  (APPLICATION_ID, ID_FLEX_CODE, ID_FLEX_NUM) IN (
    SELECT
      APPLICATION_ID,
      ID_FLEX_CODE,
      ID_FLEX_NUM
    FROM bec_raw_dl_ext.FND_ID_FLEX_STRUCTURES
    WHERE
      (APPLICATION_ID, ID_FLEX_CODE, ID_FLEX_NUM, KCA_SEQ_ID) IN (
        SELECT
          APPLICATION_ID,
          ID_FLEX_CODE,
          ID_FLEX_NUM,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.FND_ID_FLEX_STRUCTURES
        GROUP BY
          APPLICATION_ID,
          ID_FLEX_CODE,
          ID_FLEX_NUM
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fnd_id_flex_structures';