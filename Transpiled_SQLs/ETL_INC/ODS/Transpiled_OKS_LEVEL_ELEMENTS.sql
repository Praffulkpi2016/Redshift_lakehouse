/* Delete Records */
DELETE FROM silver_bec_ods.OKS_LEVEL_ELEMENTS
WHERE
  (
    COALESCE(ID, 'NA')
  ) IN (
    SELECT
      COALESCE(stg.ID, 'NA') AS ID
    FROM silver_bec_ods.OKS_LEVEL_ELEMENTS AS ods, bronze_bec_ods_stg.OKS_LEVEL_ELEMENTS AS stg
    WHERE
      COALESCE(ods.ID, 'NA') = COALESCE(stg.ID, 'NA')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.OKS_LEVEL_ELEMENTS (
  ID,
  SEQUENCE_NUMBER,
  DATE_START,
  AMOUNT,
  DATE_RECEIVABLE_GL,
  DATE_REVENUE_RULE_START,
  DATE_TRANSACTION,
  DATE_DUE,
  DATE_PRINT,
  DATE_TO_INTERFACE,
  DATE_COMPLETED,
  OBJECT_VERSION_NUMBER,
  RUL_ID,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  SECURITY_GROUP_ID,
  CLE_ID,
  DNZ_CHR_ID,
  PARENT_CLE_ID,
  DATE_END,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    ID,
    SEQUENCE_NUMBER,
    DATE_START,
    AMOUNT,
    DATE_RECEIVABLE_GL,
    DATE_REVENUE_RULE_START,
    DATE_TRANSACTION,
    DATE_DUE,
    DATE_PRINT,
    DATE_TO_INTERFACE,
    DATE_COMPLETED,
    OBJECT_VERSION_NUMBER,
    RUL_ID,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_DATE,
    SECURITY_GROUP_ID,
    CLE_ID,
    DNZ_CHR_ID,
    PARENT_CLE_ID,
    DATE_END,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.OKS_LEVEL_ELEMENTS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(ID, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(ID, 'NA') AS ID,
        MAX(kca_seq_id) AS kca_seq_id
      FROM bronze_bec_ods_stg.OKS_LEVEL_ELEMENTS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(ID, 'NA')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.OKS_LEVEL_ELEMENTS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.OKS_LEVEL_ELEMENTS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    ID
  ) IN (
    SELECT
      ID
    FROM bec_raw_dl_ext.OKS_LEVEL_ELEMENTS
    WHERE
      (ID, KCA_SEQ_ID) IN (
        SELECT
          ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.OKS_LEVEL_ELEMENTS
        GROUP BY
          ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'oks_level_elements';