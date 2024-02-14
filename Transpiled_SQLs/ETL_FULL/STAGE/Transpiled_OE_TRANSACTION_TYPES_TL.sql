DROP table IF EXISTS bronze_bec_ods_stg.OE_TRANSACTION_TYPES_TL;
CREATE TABLE bronze_bec_ods_stg.OE_TRANSACTION_TYPES_TL AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.OE_TRANSACTION_TYPES_TL
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(TRANSACTION_TYPE_ID, 0), COALESCE(language, 'NA'), last_update_date) IN (
      SELECT
        COALESCE(TRANSACTION_TYPE_ID, 0) AS TRANSACTION_TYPE_ID,
        COALESCE(language, 'NA') AS language,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.OE_TRANSACTION_TYPES_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        TRANSACTION_TYPE_ID,
        language
    )
);