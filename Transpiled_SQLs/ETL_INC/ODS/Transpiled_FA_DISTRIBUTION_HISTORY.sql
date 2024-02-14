/* Delete Records */
DELETE FROM silver_bec_ods.fa_distribution_history
WHERE
  (
    COALESCE(DISTRIBUTION_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.DISTRIBUTION_ID, 0) AS DISTRIBUTION_ID
    FROM silver_bec_ods.fa_distribution_history AS ods, bronze_bec_ods_stg.fa_distribution_history AS stg
    WHERE
      COALESCE(ods.DISTRIBUTION_ID, 0) = COALESCE(stg.DISTRIBUTION_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.fa_distribution_history (
  DISTRIBUTION_ID,
  BOOK_TYPE_CODE,
  ASSET_ID,
  UNITS_ASSIGNED,
  DATE_EFFECTIVE,
  CODE_COMBINATION_ID,
  LOCATION_ID,
  TRANSACTION_HEADER_ID_IN,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  DATE_INEFFECTIVE,
  ASSIGNED_TO,
  TRANSACTION_HEADER_ID_OUT,
  TRANSACTION_UNITS,
  RETIREMENT_ID,
  LAST_UPDATE_LOGIN,
  CAPITAL_ADJ_ACCOUNT_CCID,
  GENERAL_FUND_ACCOUNT_CCID,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    DISTRIBUTION_ID,
    BOOK_TYPE_CODE,
    ASSET_ID,
    UNITS_ASSIGNED,
    DATE_EFFECTIVE,
    CODE_COMBINATION_ID,
    LOCATION_ID,
    TRANSACTION_HEADER_ID_IN,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    DATE_INEFFECTIVE,
    ASSIGNED_TO,
    TRANSACTION_HEADER_ID_OUT,
    TRANSACTION_UNITS,
    RETIREMENT_ID,
    LAST_UPDATE_LOGIN,
    CAPITAL_ADJ_ACCOUNT_CCID,
    GENERAL_FUND_ACCOUNT_CCID,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.fa_distribution_history
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(DISTRIBUTION_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(DISTRIBUTION_ID, 0) AS DISTRIBUTION_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.fa_distribution_history
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(DISTRIBUTION_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.fa_distribution_history SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.fa_distribution_history SET IS_DELETED_FLG = 'Y'
WHERE
  (
    DISTRIBUTION_ID
  ) IN (
    SELECT
      DISTRIBUTION_ID
    FROM bec_raw_dl_ext.fa_distribution_history
    WHERE
      (DISTRIBUTION_ID, KCA_SEQ_ID) IN (
        SELECT
          DISTRIBUTION_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.fa_distribution_history
        GROUP BY
          DISTRIBUTION_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fa_distribution_history';