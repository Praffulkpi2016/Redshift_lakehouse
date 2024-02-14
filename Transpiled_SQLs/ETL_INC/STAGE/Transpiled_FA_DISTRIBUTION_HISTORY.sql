TRUNCATE table bronze_bec_ods_stg.fa_distribution_history;
INSERT INTO bronze_bec_ods_stg.fa_distribution_history (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.fa_distribution_history
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(DISTRIBUTION_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(DISTRIBUTION_ID, 0) AS DISTRIBUTION_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.fa_distribution_history
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(DISTRIBUTION_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'fa_distribution_history'
    )
);