/* Delete Records */
DELETE FROM silver_bec_ods.OKC_K_GROUPS_TL
WHERE
  (COALESCE(ID, 0), COALESCE(LANGUAGE, 'NA')) IN (
    SELECT
      COALESCE(stg.ID, 0) AS ID,
      COALESCE(stg.LANGUAGE, 'NA') AS LANGUAGE
    FROM silver_bec_ods.OKC_K_GROUPS_TL AS ods, bronze_bec_ods_stg.OKC_K_GROUPS_TL AS stg
    WHERE
      COALESCE(ods.ID, 0) = COALESCE(stg.ID, 0)
      AND COALESCE(ods.LANGUAGE, 'NA') = COALESCE(stg.LANGUAGE, 'NA')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.OKC_K_GROUPS_TL (
  ID,
  LANGUAGE,
  SOURCE_LANG,
  SFWT_FLAG,
  NAME,
  SHORT_DESCRIPTION,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  SECURITY_GROUP_ID,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    ID,
    LANGUAGE,
    SOURCE_LANG,
    SFWT_FLAG,
    NAME,
    SHORT_DESCRIPTION,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATE_LOGIN,
    SECURITY_GROUP_ID,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.OKC_K_GROUPS_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(ID, 0), COALESCE(LANGUAGE, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(ID, 0) AS ID,
        COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
        MAX(kca_seq_id) AS kca_seq_id
      FROM bronze_bec_ods_stg.OKC_K_GROUPS_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(ID, 0),
        COALESCE(LANGUAGE, 'NA')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.OKC_K_GROUPS_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.OKC_K_GROUPS_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(ID, 0), COALESCE(LANGUAGE, 'NA')) IN (
    SELECT
      COALESCE(ID, 0),
      COALESCE(LANGUAGE, 'NA')
    FROM bec_raw_dl_ext.OKC_K_GROUPS_TL
    WHERE
      (COALESCE(ID, 0), COALESCE(LANGUAGE, 'NA'), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(ID, 0),
          COALESCE(LANGUAGE, 'NA'),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.OKC_K_GROUPS_TL
        GROUP BY
          COALESCE(ID, 0),
          COALESCE(LANGUAGE, 'NA')
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'okc_k_groups_tl';