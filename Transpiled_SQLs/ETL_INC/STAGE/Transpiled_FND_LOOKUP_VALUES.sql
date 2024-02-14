TRUNCATE table
	table bronze_bec_ods_stg.fnd_lookup_values;
INSERT INTO bronze_bec_ods_stg.fnd_lookup_values (
  LOOKUP_TYPE,
  LANGUAGE,
  LOOKUP_CODE,
  MEANING,
  DESCRIPTION,
  ENABLED_FLAG,
  START_DATE_ACTIVE,
  END_DATE_ACTIVE,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  LAST_UPDATE_DATE,
  SOURCE_LANG,
  SECURITY_GROUP_ID,
  VIEW_APPLICATION_ID,
  TERRITORY_CODE,
  ATTRIBUTE_CATEGORY,
  ATTRIBUTE1,
  ATTRIBUTE2,
  ATTRIBUTE3,
  ATTRIBUTE4,
  ATTRIBUTE5,
  ATTRIBUTE6,
  ATTRIBUTE7,
  ATTRIBUTE8,
  ATTRIBUTE9,
  ATTRIBUTE10,
  ATTRIBUTE11,
  ATTRIBUTE12,
  ATTRIBUTE13,
  ATTRIBUTE14,
  ATTRIBUTE15,
  `TAG`,
  LEAF_NODE,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    LOOKUP_TYPE,
    LANGUAGE,
    LOOKUP_CODE,
    MEANING,
    DESCRIPTION,
    ENABLED_FLAG,
    START_DATE_ACTIVE,
    END_DATE_ACTIVE,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    LAST_UPDATE_DATE,
    SOURCE_LANG,
    SECURITY_GROUP_ID,
    VIEW_APPLICATION_ID,
    TERRITORY_CODE,
    ATTRIBUTE_CATEGORY,
    ATTRIBUTE1,
    ATTRIBUTE2,
    ATTRIBUTE3,
    ATTRIBUTE4,
    ATTRIBUTE5,
    ATTRIBUTE6,
    ATTRIBUTE7,
    ATTRIBUTE8,
    ATTRIBUTE9,
    ATTRIBUTE10,
    ATTRIBUTE11,
    ATTRIBUTE12,
    ATTRIBUTE13,
    ATTRIBUTE14,
    ATTRIBUTE15,
    `TAG`,
    LEAF_NODE,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.fnd_lookup_values
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (LOOKUP_TYPE, LANGUAGE, LOOKUP_CODE, SECURITY_GROUP_ID, VIEW_APPLICATION_ID, kca_seq_id) IN (
      SELECT
        LOOKUP_TYPE,
        LANGUAGE,
        LOOKUP_CODE,
        SECURITY_GROUP_ID,
        VIEW_APPLICATION_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.FND_LOOKUP_VALUES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        LOOKUP_TYPE,
        LANGUAGE,
        LOOKUP_CODE,
        SECURITY_GROUP_ID,
        VIEW_APPLICATION_ID
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'fnd_lookup_values'
    )
);