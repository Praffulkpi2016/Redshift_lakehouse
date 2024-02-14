TRUNCATE table bronze_bec_ods_stg.MSC_COMPANY_RELATIONSHIPS;
INSERT INTO bronze_bec_ods_stg.MSC_COMPANY_RELATIONSHIPS (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MSC_COMPANY_RELATIONSHIPS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (RELATIONSHIP_ID, kca_seq_id) IN (
      SELECT
        RELATIONSHIP_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.MSC_COMPANY_RELATIONSHIPS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        RELATIONSHIP_ID
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'msc_company_relationships'
      )
    )
);