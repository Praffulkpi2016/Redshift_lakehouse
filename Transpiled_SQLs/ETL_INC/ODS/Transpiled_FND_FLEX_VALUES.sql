/* Delete Records */
DELETE FROM silver_bec_ods.fnd_flex_values
WHERE
  FLEX_VALUE_ID IN (
    SELECT
      stg.FLEX_VALUE_ID
    FROM silver_bec_ods.fnd_flex_values AS ods, bronze_bec_ods_stg.fnd_flex_values AS stg
    WHERE
      ods.FLEX_VALUE_ID = stg.FLEX_VALUE_ID AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.fnd_flex_values (
  FLEX_VALUE_SET_ID,
  FLEX_VALUE_ID,
  FLEX_VALUE,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  ENABLED_FLAG,
  SUMMARY_FLAG,
  START_DATE_ACTIVE,
  END_DATE_ACTIVE,
  PARENT_FLEX_VALUE_LOW,
  PARENT_FLEX_VALUE_HIGH,
  STRUCTURED_HIERARCHY_LEVEL,
  HIERARCHY_LEVEL,
  COMPILED_VALUE_ATTRIBUTES,
  VALUE_CATEGORY,
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
  ATTRIBUTE16,
  ATTRIBUTE17,
  ATTRIBUTE18,
  ATTRIBUTE19,
  ATTRIBUTE20,
  ATTRIBUTE21,
  ATTRIBUTE22,
  ATTRIBUTE23,
  ATTRIBUTE24,
  ATTRIBUTE25,
  ATTRIBUTE26,
  ATTRIBUTE27,
  ATTRIBUTE28,
  ATTRIBUTE29,
  ATTRIBUTE30,
  ATTRIBUTE31,
  ATTRIBUTE32,
  ATTRIBUTE33,
  ATTRIBUTE34,
  ATTRIBUTE35,
  ATTRIBUTE36,
  ATTRIBUTE37,
  ATTRIBUTE38,
  ATTRIBUTE39,
  ATTRIBUTE40,
  ATTRIBUTE41,
  ATTRIBUTE42,
  ATTRIBUTE43,
  ATTRIBUTE44,
  ATTRIBUTE45,
  ATTRIBUTE46,
  ATTRIBUTE47,
  ATTRIBUTE48,
  ATTRIBUTE49,
  ATTRIBUTE50,
  ATTRIBUTE_SORT_ORDER,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    FLEX_VALUE_SET_ID,
    FLEX_VALUE_ID,
    FLEX_VALUE,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    ENABLED_FLAG,
    SUMMARY_FLAG,
    START_DATE_ACTIVE,
    END_DATE_ACTIVE,
    PARENT_FLEX_VALUE_LOW,
    PARENT_FLEX_VALUE_HIGH,
    STRUCTURED_HIERARCHY_LEVEL,
    HIERARCHY_LEVEL,
    COMPILED_VALUE_ATTRIBUTES,
    VALUE_CATEGORY,
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
    ATTRIBUTE16,
    ATTRIBUTE17,
    ATTRIBUTE18,
    ATTRIBUTE19,
    ATTRIBUTE20,
    ATTRIBUTE21,
    ATTRIBUTE22,
    ATTRIBUTE23,
    ATTRIBUTE24,
    ATTRIBUTE25,
    ATTRIBUTE26,
    ATTRIBUTE27,
    ATTRIBUTE28,
    ATTRIBUTE29,
    ATTRIBUTE30,
    ATTRIBUTE31,
    ATTRIBUTE32,
    ATTRIBUTE33,
    ATTRIBUTE34,
    ATTRIBUTE35,
    ATTRIBUTE36,
    ATTRIBUTE37,
    ATTRIBUTE38,
    ATTRIBUTE39,
    ATTRIBUTE40,
    ATTRIBUTE41,
    ATTRIBUTE42,
    ATTRIBUTE43,
    ATTRIBUTE44,
    ATTRIBUTE45,
    ATTRIBUTE46,
    ATTRIBUTE47,
    ATTRIBUTE48,
    ATTRIBUTE49,
    ATTRIBUTE50,
    ATTRIBUTE_SORT_ORDER,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.fnd_flex_values
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (FLEX_VALUE_ID, KCA_SEQ_ID) IN (
      SELECT
        FLEX_VALUE_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.fnd_flex_values
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        FLEX_VALUE_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.fnd_flex_values SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.fnd_flex_values SET IS_DELETED_FLG = 'Y'
WHERE
  (
    FLEX_VALUE_ID
  ) IN (
    SELECT
      FLEX_VALUE_ID
    FROM bec_raw_dl_ext.fnd_flex_values
    WHERE
      (FLEX_VALUE_ID, KCA_SEQ_ID) IN (
        SELECT
          FLEX_VALUE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.fnd_flex_values
        GROUP BY
          FLEX_VALUE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fnd_flex_values';