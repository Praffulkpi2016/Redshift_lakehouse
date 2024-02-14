/* Delete Records */
DELETE FROM silver_bec_ods.PO_UN_NUMBERS_TL
WHERE
  (un_number_id, language) IN (
    SELECT
      stg.un_number_id,
      stg.language
    FROM silver_bec_ods.PO_UN_NUMBERS_TL AS ods, bronze_bec_ods_stg.PO_UN_NUMBERS_TL AS stg
    WHERE
      ods.un_number_id = stg.un_number_id
      AND ods.language = stg.language
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.PO_UN_NUMBERS_TL (
  un_number_id,
  `language`,
  source_lang,
  un_number,
  description,
  created_by,
  creation_date,
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
    un_number_id,
    `language`,
    source_lang,
    un_number,
    description,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.PO_UN_NUMBERS_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (un_number_id, language, kca_seq_id) IN (
      SELECT
        un_number_id,
        language,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.PO_UN_NUMBERS_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        un_number_id,
        language
    )
);
/* Soft delete */
UPDATE silver_bec_ods.PO_UN_NUMBERS_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.PO_UN_NUMBERS_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (un_number_id, language) IN (
    SELECT
      un_number_id,
      language
    FROM bec_raw_dl_ext.PO_UN_NUMBERS_TL
    WHERE
      (un_number_id, language, KCA_SEQ_ID) IN (
        SELECT
          un_number_id,
          language,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.PO_UN_NUMBERS_TL
        GROUP BY
          un_number_id,
          language
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'po_un_numbers_tl';