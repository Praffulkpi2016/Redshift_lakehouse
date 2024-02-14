TRUNCATE table
	table bronze_bec_ods_stg.OKC_K_ITEMS;
INSERT INTO bronze_bec_ods_stg.OKC_K_ITEMS (
  ID,
  CLE_ID,
  CHR_ID,
  CLE_ID_FOR,
  DNZ_CHR_ID,
  OBJECT1_ID1,
  OBJECT1_ID2,
  JTOT_OBJECT1_CODE,
  UOM_CODE,
  EXCEPTION_YN,
  NUMBER_OF_ITEMS,
  PRICED_ITEM_YN,
  OBJECT_VERSION_NUMBER,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  SECURITY_GROUP_ID,
  UPG_ORIG_SYSTEM_REF,
  UPG_ORIG_SYSTEM_REF_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  REQUEST_ID,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    ID,
    CLE_ID,
    CHR_ID,
    CLE_ID_FOR,
    DNZ_CHR_ID,
    OBJECT1_ID1,
    OBJECT1_ID2,
    JTOT_OBJECT1_CODE,
    UOM_CODE,
    EXCEPTION_YN,
    NUMBER_OF_ITEMS,
    PRICED_ITEM_YN,
    OBJECT_VERSION_NUMBER,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATE_LOGIN,
    SECURITY_GROUP_ID,
    UPG_ORIG_SYSTEM_REF,
    UPG_ORIG_SYSTEM_REF_ID,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    REQUEST_ID,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.OKC_K_ITEMS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(ID, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(ID, 'NA') AS ID,
        MAX(kca_seq_id) AS kca_seq_id
      FROM bec_raw_dl_ext.OKC_K_ITEMS
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
        ods_table_name = 'okc_k_items'
    )
);