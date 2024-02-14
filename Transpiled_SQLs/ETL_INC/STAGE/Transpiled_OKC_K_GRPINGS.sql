TRUNCATE table
	table bronze_bec_ods_stg.OKC_K_GRPINGS;
INSERT INTO bronze_bec_ods_stg.OKC_K_GRPINGS (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.OKC_K_GRPINGS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(ID, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(ID, 'NA') AS ID,
        MAX(kca_seq_id) AS kca_seq_id
      FROM bec_raw_dl_ext.OKC_K_GRPINGS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(ID, 'NA')
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'okc_k_grpings'
    )
);