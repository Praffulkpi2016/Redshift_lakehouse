/* Delete Records */
DELETE FROM silver_bec_ods.JTF_RS_GROUPS_TL
WHERE
  (COALESCE(GROUP_ID, 0), COALESCE(LANGUAGE, 'NA')) IN (
    SELECT
      COALESCE(stg.GROUP_ID, 0) AS GROUP_ID,
      COALESCE(stg.LANGUAGE, 'NA') AS LANGUAGE
    FROM silver_bec_ods.JTF_RS_GROUPS_TL AS ods, bronze_bec_ods_stg.JTF_RS_GROUPS_TL AS stg
    WHERE
      COALESCE(ods.GROUP_ID, 0) = COALESCE(stg.GROUP_ID, 0)
      AND COALESCE(ods.LANGUAGE, 'NA') = COALESCE(stg.LANGUAGE, 'NA')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.JTF_RS_GROUPS_TL (
  GROUP_ID,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  GROUP_NAME,
  GROUP_DESC,
  LANGUAGE,
  SOURCE_LANG,
  SECURITY_GROUP_ID,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_ID,
  kca_seq_date
)
(
  SELECT
    GROUP_ID,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATE_LOGIN,
    GROUP_NAME,
    GROUP_DESC,
    LANGUAGE,
    SOURCE_LANG,
    SECURITY_GROUP_ID,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.JTF_RS_GROUPS_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(GROUP_ID, 0), COALESCE(LANGUAGE, 'NA'), kca_seq_ID) IN (
      SELECT
        COALESCE(GROUP_ID, 0) AS GROUP_ID,
        COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
        MAX(kca_seq_ID) AS kca_seq_ID
      FROM bronze_bec_ods_stg.JTF_RS_GROUPS_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(GROUP_ID, 0),
        COALESCE(LANGUAGE, 'NA')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.JTF_RS_GROUPS_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.JTF_RS_GROUPS_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(GROUP_ID, 0), COALESCE(LANGUAGE, 'NA')) IN (
    SELECT
      COALESCE(GROUP_ID, 0),
      COALESCE(LANGUAGE, 'NA')
    FROM bec_raw_dl_ext.JTF_RS_GROUPS_TL
    WHERE
      (COALESCE(GROUP_ID, 0), COALESCE(LANGUAGE, 'NA'), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(GROUP_ID, 0),
          COALESCE(LANGUAGE, 'NA'),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.JTF_RS_GROUPS_TL
        GROUP BY
          COALESCE(GROUP_ID, 0),
          COALESCE(LANGUAGE, 'NA')
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'jtf_rs_groups_tl';