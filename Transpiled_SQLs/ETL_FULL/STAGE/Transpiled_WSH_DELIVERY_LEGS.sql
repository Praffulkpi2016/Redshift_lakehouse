DROP TABLE IF EXISTS bronze_bec_ods_stg.WSH_DELIVERY_LEGS;
CREATE TABLE bronze_bec_ods_stg.WSH_DELIVERY_LEGS AS
SELECT
  *
FROM bec_raw_dl_ext.WSH_DELIVERY_LEGS
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(DELIVERY_LEG_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(DELIVERY_LEG_ID, 0) AS DELIVERY_LEG_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.WSH_DELIVERY_LEGS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(DELIVERY_LEG_ID, 0)
  );