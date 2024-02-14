/* Delete Records */
DELETE FROM silver_bec_ods.QP_LIST_HEADERS_TL
WHERE
  (COALESCE(LANGUAGE, 'NA'), COALESCE(LIST_HEADER_ID, 0)) IN (
    SELECT
      COALESCE(stg.LANGUAGE, 'NA') AS LANGUAGE,
      COALESCE(stg.LIST_HEADER_ID, 0) AS LIST_HEADER_ID
    FROM silver_bec_ods.QP_LIST_HEADERS_TL AS ods, bronze_bec_ods_stg.QP_LIST_HEADERS_TL AS stg
    WHERE
      COALESCE(ods.LANGUAGE, 'NA') = COALESCE(stg.LANGUAGE, 'NA')
      AND COALESCE(ods.LIST_HEADER_ID, 0) = COALESCE(stg.LIST_HEADER_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.QP_LIST_HEADERS_TL (
  list_header_id,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  last_update_login,
  `language`,
  source_lang,
  `name`,
  description,
  version_no,
  KCA_OPERATION,
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    list_header_id,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    `language`,
    source_lang,
    `name`,
    description,
    version_no,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.QP_LIST_HEADERS_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(LANGUAGE, 'NA'), COALESCE(LIST_HEADER_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
        COALESCE(LIST_HEADER_ID, 0) AS LIST_HEADER_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.QP_LIST_HEADERS_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(LANGUAGE, 'NA'),
        COALESCE(LIST_HEADER_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.QP_LIST_HEADERS_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.QP_LIST_HEADERS_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(LANGUAGE, 'NA'), COALESCE(LIST_HEADER_ID, 0)) IN (
    SELECT
      COALESCE(LANGUAGE, 'NA'),
      COALESCE(LIST_HEADER_ID, 0)
    FROM bec_raw_dl_ext.QP_LIST_HEADERS_TL
    WHERE
      (COALESCE(LANGUAGE, 'NA'), COALESCE(LIST_HEADER_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(LANGUAGE, 'NA'),
          COALESCE(LIST_HEADER_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.QP_LIST_HEADERS_TL
        GROUP BY
          COALESCE(LANGUAGE, 'NA'),
          COALESCE(LIST_HEADER_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'qp_list_headers_tl';