/* Delete Records */
DELETE FROM silver_bec_ods.WF_ITEM_TYPES_TL
WHERE
  (COALESCE(NAME, 'NA'), COALESCE(LANGUAGE, 'NA')) IN (
    SELECT
      COALESCE(stg.NAME, 'NA') AS NAME,
      COALESCE(stg.LANGUAGE, 'NA') AS LANGUAGE
    FROM silver_bec_ods.WF_ITEM_TYPES_TL AS ods, bronze_bec_ods_stg.WF_ITEM_TYPES_TL AS stg
    WHERE
      COALESCE(ods.NAME, 'NA') = COALESCE(stg.NAME, 'NA')
      AND COALESCE(ods.LANGUAGE, 'NA') = COALESCE(stg.LANGUAGE, 'NA')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.WF_ITEM_TYPES_TL (
  `name`,
  `language`,
  display_name,
  protect_level,
  custom_level,
  description,
  source_lang,
  security_group_id,
  zd_edition_name,
  zd_sync,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    `name`,
    `language`,
    display_name,
    protect_level,
    custom_level,
    description,
    source_lang,
    security_group_id,
    zd_edition_name,
    zd_sync,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.WF_ITEM_TYPES_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(NAME, 'NA'), COALESCE(LANGUAGE, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(NAME, 'NA') AS NAME,
        COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.WF_ITEM_TYPES_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(NAME, 'NA'),
        COALESCE(LANGUAGE, 'NA')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.WF_ITEM_TYPES_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.WF_ITEM_TYPES_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(NAME, 'NA'), COALESCE(LANGUAGE, 'NA')) IN (
    SELECT
      COALESCE(NAME, 'NA'),
      COALESCE(LANGUAGE, 'NA')
    FROM bec_raw_dl_ext.WF_ITEM_TYPES_TL
    WHERE
      (COALESCE(NAME, 'NA'), COALESCE(LANGUAGE, 'NA'), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(NAME, 'NA'),
          COALESCE(LANGUAGE, 'NA'),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.WF_ITEM_TYPES_TL
        GROUP BY
          COALESCE(NAME, 'NA'),
          COALESCE(LANGUAGE, 'NA')
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'wf_item_types_tl';