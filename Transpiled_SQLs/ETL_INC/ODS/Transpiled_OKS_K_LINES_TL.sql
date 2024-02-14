/* Delete Records */
DELETE FROM silver_bec_ods.OKS_K_LINES_TL
WHERE
  (COALESCE(ID, 'NA'), COALESCE(LANGUAGE, 'NA')) IN (
    SELECT
      COALESCE(stg.ID, 'NA'),
      COALESCE(stg.LANGUAGE, 'NA')
    FROM silver_bec_ods.OKS_K_LINES_TL AS ods, bronze_bec_ods_stg.OKS_K_LINES_TL AS stg
    WHERE
      ods.ID = stg.ID
      AND ods.LANGUAGE = stg.LANGUAGE
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.OKS_K_LINES_TL (
  id,
  `language`,
  source_lang,
  sfwt_flag,
  invoice_text,
  ib_trx_details,
  status_text,
  react_time_name,
  security_group_id,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    id,
    `language`,
    source_lang,
    sfwt_flag,
    invoice_text,
    ib_trx_details,
    status_text,
    react_time_name,
    security_group_id,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.OKS_K_LINES_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (ID, LANGUAGE, kca_seq_id) IN (
      SELECT
        ID,
        LANGUAGE,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.OKS_K_LINES_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        ID,
        LANGUAGE
    )
);
/* Soft delete */
UPDATE silver_bec_ods.OKS_K_LINES_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.OKS_K_LINES_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(ID, 'NA'), COALESCE(LANGUAGE, 'NA')) IN (
    SELECT
      COALESCE(ID, 'NA'),
      COALESCE(LANGUAGE, 'NA')
    FROM bec_raw_dl_ext.OKS_K_LINES_TL
    WHERE
      (COALESCE(ID, 'NA'), COALESCE(LANGUAGE, 'NA'), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(ID, 'NA'),
          COALESCE(LANGUAGE, 'NA'),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.OKS_K_LINES_TL
        GROUP BY
          COALESCE(ID, 'NA'),
          COALESCE(LANGUAGE, 'NA')
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'oks_k_lines_tl';