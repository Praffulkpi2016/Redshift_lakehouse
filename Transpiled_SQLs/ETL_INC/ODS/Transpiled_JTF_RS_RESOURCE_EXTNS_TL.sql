/* Delete Records */
DELETE FROM silver_bec_ods.jtf_rs_resource_extns_tl
WHERE
  (RESOURCE_ID, CATEGORY, LANGUAGE) IN (
    SELECT
      stg.RESOURCE_ID,
      stg.CATEGORY,
      stg.LANGUAGE
    FROM silver_bec_ods.jtf_rs_resource_extns_tl AS ods, bronze_bec_ods_stg.jtf_rs_resource_extns_tl AS stg
    WHERE
      ods.RESOURCE_ID = stg.RESOURCE_ID
      AND stg.CATEGORY = ods.CATEGORY
      AND stg.LANGUAGE = ods.LANGUAGE
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.jtf_rs_resource_extns_tl (
  created_by,
  resource_id,
  category,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  `language`,
  resource_name,
  source_lang,
  security_group_id,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  created_by,
  resource_id,
  category,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  `language`,
  resource_name,
  source_lang,
  security_group_id,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.jtf_rs_resource_extns_tl
WHERE
  kca_operation IN ('INSERT', 'UPDATE')
  AND (RESOURCE_ID, CATEGORY, LANGUAGE, kca_seq_id) IN (
    SELECT
      RESOURCE_ID,
      CATEGORY,
      LANGUAGE,
      MAX(kca_seq_id)
    FROM bronze_bec_ods_stg.jtf_rs_resource_extns_tl
    WHERE
      kca_operation IN ('INSERT', 'UPDATE')
    GROUP BY
      RESOURCE_ID,
      CATEGORY,
      LANGUAGE
  );
/* Soft delete */
UPDATE silver_bec_ods.jtf_rs_resource_extns_tl SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.jtf_rs_resource_extns_tl SET IS_DELETED_FLG = 'Y'
WHERE
  (RESOURCE_ID, CATEGORY, LANGUAGE) IN (
    SELECT
      RESOURCE_ID,
      CATEGORY,
      LANGUAGE
    FROM bec_raw_dl_ext.jtf_rs_resource_extns_tl
    WHERE
      (RESOURCE_ID, CATEGORY, LANGUAGE, KCA_SEQ_ID) IN (
        SELECT
          RESOURCE_ID,
          CATEGORY,
          LANGUAGE,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.jtf_rs_resource_extns_tl
        GROUP BY
          RESOURCE_ID,
          CATEGORY,
          LANGUAGE
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'jtf_rs_resource_extns_tl';