TRUNCATE table
	table bronze_bec_ods_stg.fnd_user;
INSERT INTO bronze_bec_ods_stg.fnd_user (
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
  kca_operation,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
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
    kca_operation,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.fnd_user
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (user_id, kca_seq_id) IN (
      SELECT
        user_id,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.FND_USER
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        user_id
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'fnd_user'
      )
    )
);