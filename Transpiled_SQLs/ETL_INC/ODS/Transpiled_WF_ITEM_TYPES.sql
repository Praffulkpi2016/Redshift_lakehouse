/* Delete Records */
DELETE FROM silver_bec_ods.wf_item_types
WHERE
  (
    COALESCE(NAME, 'NA')
  ) IN (
    SELECT
      COALESCE(stg.NAME, 'NA') AS NAME
    FROM silver_bec_ods.wf_item_types AS ods, bronze_bec_ods_stg.wf_item_types AS stg
    WHERE
      COALESCE(ods.NAME, 'NA') = COALESCE(stg.NAME, 'NA')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.wf_item_types (
  NAME,
  PROTECT_LEVEL,
  CUSTOM_LEVEL,
  WF_SELECTOR,
  READ_ROLE,
  WRITE_ROLE,
  EXECUTE_ROLE,
  PERSISTENCE_TYPE,
  PERSISTENCE_DAYS,
  SECURITY_GROUP_ID,
  NUM_ACTIVE,
  NUM_ERROR,
  NUM_DEFER,
  NUM_SUSPEND,
  NUM_COMPLETE,
  NUM_PURGEABLE,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    NAME,
    PROTECT_LEVEL,
    CUSTOM_LEVEL,
    WF_SELECTOR,
    READ_ROLE,
    WRITE_ROLE,
    EXECUTE_ROLE,
    PERSISTENCE_TYPE,
    PERSISTENCE_DAYS,
    SECURITY_GROUP_ID,
    NUM_ACTIVE,
    NUM_ERROR,
    NUM_DEFER,
    NUM_SUSPEND,
    NUM_COMPLETE,
    NUM_PURGEABLE,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.wf_item_types
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(NAME, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(NAME, 'NA') AS NAME,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.wf_item_types
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(NAME, 'NA')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.wf_item_types SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.wf_item_types SET IS_DELETED_FLG = 'Y'
WHERE
  (
    NAME
  ) IN (
    SELECT
      NAME
    FROM bec_raw_dl_ext.wf_item_types
    WHERE
      (NAME, KCA_SEQ_ID) IN (
        SELECT
          NAME,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.wf_item_types
        GROUP BY
          NAME
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP(), load_type = 'I'
WHERE
  ods_table_name = 'wf_item_types';