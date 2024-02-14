/* Delete Records */
DELETE FROM silver_bec_ods.RA_RULES
WHERE
  RULE_ID IN (
    SELECT
      stg.RULE_ID
    FROM silver_bec_ods.RA_RULES AS ods, bronze_bec_ods_stg.RA_RULES AS stg
    WHERE
      ods.RULE_ID = stg.RULE_ID AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.RA_RULES (
  RULE_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  NAME,
  TYPE,
  STATUS,
  FREQUENCY,
  OCCURRENCES,
  DESCRIPTION,
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
  DEFERRED_REVENUE_FLAG,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    RULE_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    NAME,
    TYPE,
    STATUS,
    FREQUENCY,
    OCCURRENCES,
    DESCRIPTION,
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
    DEFERRED_REVENUE_FLAG,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.RA_RULES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (RULE_ID, kca_seq_id) IN (
      SELECT
        RULE_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.RA_RULES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        RULE_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.RA_RULES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.RA_RULES SET IS_DELETED_FLG = 'Y'
WHERE
  (
    RULE_ID
  ) IN (
    SELECT
      RULE_ID
    FROM bec_raw_dl_ext.RA_RULES
    WHERE
      (RULE_ID, KCA_SEQ_ID) IN (
        SELECT
          RULE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.RA_RULES
        GROUP BY
          RULE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP(), load_type = 'I'
WHERE
  ods_table_name = 'ra_rules';