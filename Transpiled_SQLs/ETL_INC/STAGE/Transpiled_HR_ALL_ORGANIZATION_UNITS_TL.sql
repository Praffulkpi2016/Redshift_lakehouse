TRUNCATE table bronze_bec_ods_stg.HR_ALL_ORGANIZATION_UNITS_TL;
INSERT INTO bronze_bec_ods_stg.HR_ALL_ORGANIZATION_UNITS_TL (
  ORGANIZATION_ID,
  LANGUAGE,
  SOURCE_LANG,
  NAME,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  CREATED_BY,
  CREATION_DATE,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
SELECT
  ORGANIZATION_ID,
  LANGUAGE,
  SOURCE_LANG,
  NAME,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  CREATED_BY,
  CREATION_DATE,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
FROM bec_raw_dl_ext.HR_ALL_ORGANIZATION_UNITS_TL
WHERE
  kca_operation <> 'DELETE'
  AND COALESCE(kca_seq_id, '') <> ''
  AND (ORGANIZATION_ID, LANGUAGE, kca_seq_id) IN (
    SELECT
      ORGANIZATION_ID,
      LANGUAGE,
      MAX(kca_seq_id)
    FROM bec_raw_dl_ext.hr_all_organization_units_tl
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
    GROUP BY
      ORGANIZATION_ID,
      LANGUAGE
  )
  AND (
    kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'hr_all_organization_units_tl'
    )
  );