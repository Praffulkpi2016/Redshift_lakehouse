DROP TABLE IF EXISTS bronze_bec_ods_stg.wsh_carriers;
CREATE TABLE bronze_bec_ods_stg.wsh_carriers AS
SELECT
  *
FROM bec_raw_dl_ext.wsh_carriers
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(CARRIER_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(CARRIER_ID, 0) AS CARRIER_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.wsh_carriers
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(CARRIER_ID, 0)
  );