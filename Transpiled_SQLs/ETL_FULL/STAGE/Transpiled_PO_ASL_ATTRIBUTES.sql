DROP TABLE IF EXISTS bronze_bec_ods_stg.PO_ASL_ATTRIBUTES;
CREATE TABLE bronze_bec_ods_stg.PO_ASL_ATTRIBUTES AS
SELECT
  *
FROM bec_raw_dl_ext.PO_ASL_ATTRIBUTES
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(ASL_ID, 0), COALESCE(USING_ORGANIZATION_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(ASL_ID, 0) AS ASL_ID,
      COALESCE(USING_ORGANIZATION_ID, 0) AS USING_ORGANIZATION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.PO_ASL_ATTRIBUTES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(ASL_ID, 0),
      COALESCE(USING_ORGANIZATION_ID, 0)
  );