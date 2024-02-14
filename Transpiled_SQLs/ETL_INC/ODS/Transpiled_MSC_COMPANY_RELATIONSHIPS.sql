/* Delete Records */
DELETE FROM silver_bec_ods.MSC_COMPANY_RELATIONSHIPS
WHERE
  RELATIONSHIP_ID IN (
    SELECT
      stg.RELATIONSHIP_ID
    FROM silver_bec_ods.MSC_COMPANY_RELATIONSHIPS AS ods, bronze_bec_ods_stg.MSC_COMPANY_RELATIONSHIPS AS stg
    WHERE
      ods.RELATIONSHIP_ID = stg.RELATIONSHIP_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MSC_COMPANY_RELATIONSHIPS (
  RELATIONSHIP_ID,
  SUBJECT_ID,
  OBJECT_ID,
  RELATIONSHIP_TYPE,
  START_DATE,
  END_DATE,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    RELATIONSHIP_ID,
    SUBJECT_ID,
    OBJECT_ID,
    RELATIONSHIP_TYPE,
    START_DATE,
    END_DATE,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MSC_COMPANY_RELATIONSHIPS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (RELATIONSHIP_ID, kca_seq_id) IN (
      SELECT
        RELATIONSHIP_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.MSC_COMPANY_RELATIONSHIPS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        RELATIONSHIP_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MSC_COMPANY_RELATIONSHIPS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MSC_COMPANY_RELATIONSHIPS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    RELATIONSHIP_ID
  ) IN (
    SELECT
      RELATIONSHIP_ID
    FROM bec_raw_dl_ext.MSC_COMPANY_RELATIONSHIPS
    WHERE
      (RELATIONSHIP_ID, KCA_SEQ_ID) IN (
        SELECT
          RELATIONSHIP_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MSC_COMPANY_RELATIONSHIPS
        GROUP BY
          RELATIONSHIP_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'msc_company_relationships';