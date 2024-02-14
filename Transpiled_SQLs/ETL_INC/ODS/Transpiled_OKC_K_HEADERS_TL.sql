/* Delete Records */
DELETE FROM silver_bec_ods.OKC_K_HEADERS_TL
WHERE
  (ID, language) IN (
    SELECT
      stg.ID,
      stg.language
    FROM silver_bec_ods.OKC_K_HEADERS_TL AS ods, bronze_bec_ods_stg.OKC_K_HEADERS_TL AS stg
    WHERE
      ods.ID = stg.ID
      AND ods.language = stg.language
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.OKC_K_HEADERS_TL (
  id,
  `language`,
  source_lang,
  sfwt_flag,
  short_description,
  comments,
  description,
  cognomen,
  non_response_reason,
  non_response_explain,
  set_aside_reason,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  security_group_id,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    id,
    `language`,
    source_lang,
    sfwt_flag,
    short_description,
    comments,
    description,
    cognomen,
    non_response_reason,
    non_response_explain,
    set_aside_reason,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    security_group_id,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.OKC_K_HEADERS_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (ID, language, kca_seq_id) IN (
      SELECT
        ID,
        language,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.OKC_K_HEADERS_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        ID,
        language
    )
);
/* Soft delete */
UPDATE silver_bec_ods.OKC_K_HEADERS_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.OKC_K_HEADERS_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(ID, 0), COALESCE(LANGUAGE, 'NA')) IN (
    SELECT
      COALESCE(ID, 0),
      COALESCE(LANGUAGE, 'NA')
    FROM bec_raw_dl_ext.OKC_K_HEADERS_TL
    WHERE
      (COALESCE(ID, 0), COALESCE(LANGUAGE, 'NA'), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(ID, 0),
          COALESCE(LANGUAGE, 'NA'),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.OKC_K_HEADERS_TL
        GROUP BY
          COALESCE(ID, 0),
          COALESCE(LANGUAGE, 'NA')
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'okc_k_headers_tl';