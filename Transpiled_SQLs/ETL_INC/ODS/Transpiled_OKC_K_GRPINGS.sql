/* Delete Records */
DELETE FROM silver_bec_ods.OKC_K_GRPINGS
WHERE
  (
    COALESCE(ID, 'NA')
  ) IN (
    SELECT
      COALESCE(stg.ID, 'NA') AS ID
    FROM silver_bec_ods.OKC_K_GRPINGS AS ods, bronze_bec_ods_stg.OKC_K_GRPINGS AS stg
    WHERE
      COALESCE(ods.ID, 'NA') = COALESCE(stg.ID, 'NA')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.OKC_K_GRPINGS (
  ID,
  CGP_PARENT_ID,
  INCLUDED_CGP_ID,
  INCLUDED_CHR_ID,
  OBJECT_VERSION_NUMBER,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  SECURITY_GROUP_ID,
  SCS_CODE,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    ID,
    CGP_PARENT_ID,
    INCLUDED_CGP_ID,
    INCLUDED_CHR_ID,
    OBJECT_VERSION_NUMBER,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATE_LOGIN,
    SECURITY_GROUP_ID,
    SCS_CODE,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.OKC_K_GRPINGS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(ID, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(ID, 'NA') AS ID,
        MAX(kca_seq_id) AS kca_seq_id
      FROM bronze_bec_ods_stg.OKC_K_GRPINGS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(ID, 'NA')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.OKC_K_GRPINGS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.OKC_K_GRPINGS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    COALESCE(ID, 'NA')
  ) IN (
    SELECT
      COALESCE(ID, 'NA')
    FROM bec_raw_dl_ext.OKC_K_GRPINGS
    WHERE
      (COALESCE(ID, 'NA'), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(ID, 'NA'),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.OKC_K_GRPINGS
        GROUP BY
          COALESCE(ID, 'NA')
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'okc_k_grpings';