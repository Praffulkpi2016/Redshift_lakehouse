/* Delete Records */
DELETE FROM silver_bec_ods.FA_CATEGORIES_B
WHERE
  (
    COALESCE(CATEGORY_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.CATEGORY_ID, 0) AS CATEGORY_ID
    FROM silver_bec_ods.FA_CATEGORIES_B AS ods, bronze_bec_ods_stg.FA_CATEGORIES_B AS stg
    WHERE
      COALESCE(ods.CATEGORY_ID, 0) = COALESCE(stg.CATEGORY_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.FA_CATEGORIES_B (
  CATEGORY_ID,
  SUMMARY_FLAG,
  ENABLED_FLAG,
  OWNED_LEASED,
  PRODUCTION_CAPACITY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CATEGORY_TYPE,
  CAPITALIZE_FLAG,
  SEGMENT1,
  SEGMENT2,
  SEGMENT3,
  SEGMENT4,
  SEGMENT5,
  SEGMENT6,
  SEGMENT7,
  START_DATE_ACTIVE,
  END_DATE_ACTIVE,
  PROPERTY_TYPE_CODE,
  PROPERTY_1245_1250_CODE,
  DATE_INEFFECTIVE,
  INVENTORIAL,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATE_LOGIN,
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
  ATTRIBUTE_CATEGORY_CODE,
  GLOBAL_ATTRIBUTE1,
  GLOBAL_ATTRIBUTE2,
  GLOBAL_ATTRIBUTE3,
  GLOBAL_ATTRIBUTE4,
  GLOBAL_ATTRIBUTE5,
  GLOBAL_ATTRIBUTE6,
  GLOBAL_ATTRIBUTE7,
  GLOBAL_ATTRIBUTE8,
  GLOBAL_ATTRIBUTE9,
  GLOBAL_ATTRIBUTE10,
  GLOBAL_ATTRIBUTE11,
  GLOBAL_ATTRIBUTE12,
  GLOBAL_ATTRIBUTE13,
  GLOBAL_ATTRIBUTE14,
  GLOBAL_ATTRIBUTE15,
  GLOBAL_ATTRIBUTE16,
  GLOBAL_ATTRIBUTE17,
  GLOBAL_ATTRIBUTE18,
  GLOBAL_ATTRIBUTE19,
  GLOBAL_ATTRIBUTE20,
  GLOBAL_ATTRIBUTE_CATEGORY,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    CATEGORY_ID,
    SUMMARY_FLAG,
    ENABLED_FLAG,
    OWNED_LEASED,
    PRODUCTION_CAPACITY,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CATEGORY_TYPE,
    CAPITALIZE_FLAG,
    SEGMENT1,
    SEGMENT2,
    SEGMENT3,
    SEGMENT4,
    SEGMENT5,
    SEGMENT6,
    SEGMENT7,
    START_DATE_ACTIVE,
    END_DATE_ACTIVE,
    PROPERTY_TYPE_CODE,
    PROPERTY_1245_1250_CODE,
    DATE_INEFFECTIVE,
    INVENTORIAL,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATE_LOGIN,
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
    ATTRIBUTE_CATEGORY_CODE,
    GLOBAL_ATTRIBUTE1,
    GLOBAL_ATTRIBUTE2,
    GLOBAL_ATTRIBUTE3,
    GLOBAL_ATTRIBUTE4,
    GLOBAL_ATTRIBUTE5,
    GLOBAL_ATTRIBUTE6,
    GLOBAL_ATTRIBUTE7,
    GLOBAL_ATTRIBUTE8,
    GLOBAL_ATTRIBUTE9,
    GLOBAL_ATTRIBUTE10,
    GLOBAL_ATTRIBUTE11,
    GLOBAL_ATTRIBUTE12,
    GLOBAL_ATTRIBUTE13,
    GLOBAL_ATTRIBUTE14,
    GLOBAL_ATTRIBUTE15,
    GLOBAL_ATTRIBUTE16,
    GLOBAL_ATTRIBUTE17,
    GLOBAL_ATTRIBUTE18,
    GLOBAL_ATTRIBUTE19,
    GLOBAL_ATTRIBUTE20,
    GLOBAL_ATTRIBUTE_CATEGORY,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.FA_CATEGORIES_B
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(CATEGORY_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(CATEGORY_ID, 0) AS CATEGORY_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.FA_CATEGORIES_B
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(CATEGORY_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.FA_CATEGORIES_B SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.FA_CATEGORIES_B SET IS_DELETED_FLG = 'Y'
WHERE
  (
    CATEGORY_ID
  ) IN (
    SELECT
      CATEGORY_ID
    FROM bec_raw_dl_ext.FA_CATEGORIES_B
    WHERE
      (CATEGORY_ID, KCA_SEQ_ID) IN (
        SELECT
          CATEGORY_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.FA_CATEGORIES_B
        GROUP BY
          CATEGORY_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fa_categories_b';