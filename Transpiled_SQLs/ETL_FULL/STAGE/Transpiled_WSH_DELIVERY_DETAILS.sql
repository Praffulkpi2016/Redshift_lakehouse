DROP TABLE IF EXISTS bronze_bec_ods_stg.WSH_DELIVERY_DETAILS;
CREATE TABLE bronze_bec_ods_stg.WSH_DELIVERY_DETAILS AS
SELECT
  *
FROM bec_raw_dl_ext.WSH_DELIVERY_DETAILS
WHERE
  kca_operation <> 'DELETE'
  AND (DELIVERY_DETAIL_ID, last_update_date) IN (
    SELECT
      DELIVERY_DETAIL_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.WSH_DELIVERY_DETAILS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      DELIVERY_DETAIL_ID
  );