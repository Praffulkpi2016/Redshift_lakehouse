TRUNCATE table
	table bronze_bec_ods_stg.MTL_MATERIAL_STATUSES_TL;
INSERT INTO bronze_bec_ods_stg.MTL_MATERIAL_STATUSES_TL (
  STATUS_ID,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATE_LOGIN,
  STATUS_CODE,
  DESCRIPTION,
  LANGUAGE,
  SOURCE_LANG,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    STATUS_ID,
    LAST_UPDATED_BY,
    LAST_UPDATE_DATE,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATE_LOGIN,
    STATUS_CODE,
    DESCRIPTION,
    LANGUAGE,
    SOURCE_LANG,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_MATERIAL_STATUSES_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(STATUS_ID, 0), COALESCE(LANGUAGE, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(STATUS_ID, 0) AS STATUS_ID,
        COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
        MAX(kca_seq_id) AS kca_seq_id
      FROM bec_raw_dl_ext.MTL_MATERIAL_STATUSES_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(STATUS_ID, 0),
        COALESCE(LANGUAGE, 'NA')
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'mtl_material_statuses_tl'
    )
);