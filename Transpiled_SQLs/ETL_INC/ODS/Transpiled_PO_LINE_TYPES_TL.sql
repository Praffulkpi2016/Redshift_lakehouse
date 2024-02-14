/* Delete Records */
DELETE FROM silver_bec_ods.PO_LINE_TYPES_TL
WHERE
  (LINE_TYPE_ID, language) IN (
    SELECT
      stg.LINE_TYPE_ID,
      stg.language
    FROM silver_bec_ods.PO_LINE_TYPES_TL AS ods, bronze_bec_ods_stg.PO_LINE_TYPES_TL AS stg
    WHERE
      ods.LINE_TYPE_ID = stg.LINE_TYPE_ID
      AND ods.language = stg.language
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.PO_LINE_TYPES_TL (
  line_type_id,
  `language`,
  source_lang,
  description,
  line_type,
  last_update_date,
  last_updated_by,
  last_update_login,
  creation_date,
  created_by,
  ZD_EDITION_NAME,
  ZD_SYNC,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    line_type_id,
    `language`,
    source_lang,
    description,
    line_type,
    last_update_date,
    last_updated_by,
    last_update_login,
    creation_date,
    created_by,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.PO_LINE_TYPES_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (LINE_TYPE_ID, language, kca_seq_id) IN (
      SELECT
        LINE_TYPE_ID,
        language,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.PO_LINE_TYPES_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        LINE_TYPE_ID,
        language
    )
);
/* Soft delete */
UPDATE silver_bec_ods.PO_LINE_TYPES_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.PO_LINE_TYPES_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (LINE_TYPE_ID, language) IN (
    SELECT
      LINE_TYPE_ID,
      language
    FROM bec_raw_dl_ext.PO_LINE_TYPES_TL
    WHERE
      (LINE_TYPE_ID, language, KCA_SEQ_ID) IN (
        SELECT
          LINE_TYPE_ID,
          language,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.PO_LINE_TYPES_TL
        GROUP BY
          LINE_TYPE_ID,
          language
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'po_line_types_tl';