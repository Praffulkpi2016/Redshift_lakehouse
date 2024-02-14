TRUNCATE table
	table bronze_bec_ods_stg.CS_BILLING_TYPE_CATEGORIES;
INSERT INTO bronze_bec_ods_stg.CS_BILLING_TYPE_CATEGORIES (
  BILLING_TYPE,
  BILLING_CATEGORY,
  ROLLUP_ITEM_ID,
  START_DATE_ACTIVE,
  END_DATE_ACTIVE,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  SECURITY_GROUP_ID,
  SEEDED_FLAG,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    BILLING_TYPE,
    BILLING_CATEGORY,
    ROLLUP_ITEM_ID,
    START_DATE_ACTIVE,
    END_DATE_ACTIVE,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    SECURITY_GROUP_ID,
    SEEDED_FLAG,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.CS_BILLING_TYPE_CATEGORIES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(BILLING_TYPE, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(BILLING_TYPE, 'NA') AS BILLING_TYPE,
        MAX(kca_seq_id) AS kca_seq_id
      FROM bec_raw_dl_ext.CS_BILLING_TYPE_CATEGORIES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(BILLING_TYPE, 'NA')
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'cs_billing_type_categories'
    )
);