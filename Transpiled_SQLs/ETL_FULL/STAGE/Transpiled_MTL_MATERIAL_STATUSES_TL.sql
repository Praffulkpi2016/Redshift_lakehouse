DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_MATERIAL_STATUSES_TL;
CREATE TABLE bronze_bec_ods_stg.MTL_MATERIAL_STATUSES_TL AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_MATERIAL_STATUSES_TL
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(STATUS_ID, 0), COALESCE(LANGUAGE, 'NA'), last_update_date) IN (
    SELECT
      COALESCE(STATUS_ID, 0) AS STATUS_ID,
      COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
      MAX(last_update_date) AS last_update_date
    FROM bec_raw_dl_ext.MTL_MATERIAL_STATUSES_TL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(STATUS_ID, 0),
      COALESCE(LANGUAGE, 'NA')
  );