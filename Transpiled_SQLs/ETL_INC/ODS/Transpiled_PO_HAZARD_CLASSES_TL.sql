/* Delete Records */
DELETE FROM silver_bec_ods.PO_HAZARD_CLASSES_TL
WHERE
  (hazard_class_id, language) IN (
    SELECT
      stg.hazard_class_id,
      stg.language
    FROM silver_bec_ods.PO_HAZARD_CLASSES_TL AS ods, bronze_bec_ods_stg.PO_HAZARD_CLASSES_TL AS stg
    WHERE
      ods.hazard_class_id = stg.hazard_class_id
      AND ods.language = stg.language
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.PO_HAZARD_CLASSES_TL (
  hazard_class_id,
  `language`,
  source_lang,
  hazard_class,
  description,
  creation_date,
  created_by,
  last_updated_by,
  last_update_date,
  last_update_login,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    hazard_class_id,
    `language`,
    source_lang,
    hazard_class,
    description,
    creation_date,
    created_by,
    last_updated_by,
    last_update_date,
    last_update_login,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.PO_HAZARD_CLASSES_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (hazard_class_id, language, kca_seq_id) IN (
      SELECT
        hazard_class_id,
        language,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.PO_HAZARD_CLASSES_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        hazard_class_id,
        language
    )
);
/* Soft delete */
UPDATE silver_bec_ods.PO_HAZARD_CLASSES_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.PO_HAZARD_CLASSES_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (hazard_class_id, language) IN (
    SELECT
      hazard_class_id,
      language
    FROM bec_raw_dl_ext.PO_HAZARD_CLASSES_TL
    WHERE
      (hazard_class_id, language, KCA_SEQ_ID) IN (
        SELECT
          hazard_class_id,
          language,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.PO_HAZARD_CLASSES_TL
        GROUP BY
          hazard_class_id,
          language
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'po_hazard_classes_tl';