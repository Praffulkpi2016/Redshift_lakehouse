TRUNCATE table bronze_bec_ods_stg.PO_HAZARD_CLASSES_TL;
INSERT INTO bronze_bec_ods_stg.PO_HAZARD_CLASSES_TL (
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
  kca_seq_id,
  KCA_SEQ_DATE
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
    kca_operation,
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.PO_HAZARD_CLASSES_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (hazard_class_id, language, kca_seq_id) IN (
      SELECT
        hazard_class_id,
        language,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.PO_HAZARD_CLASSES_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        hazard_class_id,
        language
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'po_hazard_classes_tl'
      )
    )
);