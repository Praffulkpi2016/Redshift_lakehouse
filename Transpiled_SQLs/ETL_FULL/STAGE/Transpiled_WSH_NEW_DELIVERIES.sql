DROP TABLE IF EXISTS bronze_bec_ods_stg.WSH_NEW_DELIVERIES;
CREATE TABLE bronze_bec_ods_stg.WSH_NEW_DELIVERIES AS
SELECT
  *
FROM bec_raw_dl_ext.WSH_NEW_DELIVERIES
WHERE
  kca_operation <> 'DELETE'
  AND (DELIVERY_ID, last_update_date) IN (
    SELECT
      DELIVERY_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.WSH_NEW_DELIVERIES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      DELIVERY_ID
  );