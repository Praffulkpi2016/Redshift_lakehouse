/* Delete Records */
DELETE FROM silver_bec_ods.CST_COST_ELEMENTS
WHERE
  (
    COALESCE(COST_ELEMENT_ID, '0')
  ) IN (
    SELECT
      COALESCE(stg.COST_ELEMENT_ID, '0') AS COST_ELEMENT_ID
    FROM silver_bec_ods.CST_COST_ELEMENTS AS ods, bronze_bec_ods_stg.CST_COST_ELEMENTS AS stg
    WHERE
      COALESCE(ods.COST_ELEMENT_ID, '0') = COALESCE(stg.COST_ELEMENT_ID, '0')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.CST_COST_ELEMENTS (
  COST_ELEMENT_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  COST_ELEMENT,
  DESCRIPTION,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    COST_ELEMENT_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    COST_ELEMENT,
    DESCRIPTION,
    REQUEST_ID,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.CST_COST_ELEMENTS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(COST_ELEMENT_ID, '0'), kca_seq_id) IN (
      SELECT
        COALESCE(COST_ELEMENT_ID, '0') AS COST_ELEMENT_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.CST_COST_ELEMENTS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(COST_ELEMENT_ID, '0')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.CST_COST_ELEMENTS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.CST_COST_ELEMENTS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    COST_ELEMENT_ID
  ) IN (
    SELECT
      COST_ELEMENT_ID
    FROM bec_raw_dl_ext.CST_COST_ELEMENTS
    WHERE
      (COST_ELEMENT_ID, KCA_SEQ_ID) IN (
        SELECT
          COST_ELEMENT_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.CST_COST_ELEMENTS
        GROUP BY
          COST_ELEMENT_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'cst_cost_elements';