/* Delete Records */
DELETE FROM silver_bec_ods.fnd_user
WHERE
  USER_ID IN (
    SELECT
      stg.USER_ID
    FROM silver_bec_ods.fnd_user AS ods, bronze_bec_ods_stg.fnd_user AS stg
    WHERE
      ods.USER_ID = stg.USER_ID AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* DELETE FROM silver_bec_ods.fnd_user WHERE USER_ID IN (SELECT USER_ID FROM bronze_bec_ods_stg.fnd_user); */
INSERT INTO silver_bec_ods.fnd_user (
  USER_ID,
  USER_NAME,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  ENCRYPTED_FOUNDATION_PASSWORD,
  ENCRYPTED_USER_PASSWORD,
  START_DATE,
  END_DATE,
  DESCRIPTION,
  PASSWORD_DATE,
  PASSWORD_ACCESSES_LEFT,
  PASSWORD_LIFESPAN_ACCESSES,
  PASSWORD_LIFESPAN_DAYS,
  EMPLOYEE_ID,
  EMAIL_ADDRESS,
  FAX,
  CUSTOMER_ID,
  SUPPLIER_ID,
  WEB_PASSWORD,
  GCN_CODE_COMBINATION_ID,
  PERSON_PARTY_ID,
  KCA_OPERATION,
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT DISTINCT
    USER_ID,
    USER_NAME,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    ENCRYPTED_FOUNDATION_PASSWORD, /* LAST_UPDATE_LOGIN, */
    ENCRYPTED_USER_PASSWORD,
    START_DATE, /* SESSION_NUMBER, */
    END_DATE,
    DESCRIPTION,
    PASSWORD_DATE, /* LAST_LOGON_DATE, */
    PASSWORD_ACCESSES_LEFT,
    PASSWORD_LIFESPAN_ACCESSES,
    PASSWORD_LIFESPAN_DAYS,
    EMPLOYEE_ID,
    EMAIL_ADDRESS,
    FAX,
    CUSTOMER_ID,
    SUPPLIER_ID,
    WEB_PASSWORD,
    GCN_CODE_COMBINATION_ID,
    PERSON_PARTY_ID,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.fnd_user
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (USER_ID, kca_seq_id) IN (
      SELECT
        USER_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.fnd_user
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        USER_ID
    )
);
/* soft delete */
UPDATE silver_bec_ods.fnd_user SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.fnd_user SET IS_DELETED_FLG = 'Y'
WHERE
  (
    USER_ID
  ) IN (
    SELECT
      USER_ID
    FROM bec_raw_dl_ext.fnd_user
    WHERE
      (USER_ID, KCA_SEQ_ID) IN (
        SELECT
          USER_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.fnd_user
        GROUP BY
          USER_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fnd_user';