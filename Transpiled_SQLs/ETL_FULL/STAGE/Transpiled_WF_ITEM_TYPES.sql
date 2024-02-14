DROP TABLE IF EXISTS bronze_bec_ods_stg.wf_item_types;
CREATE TABLE bronze_bec_ods_stg.wf_item_types AS
SELECT
  *
FROM bec_raw_dl_ext.wf_item_types
WHERE
  kca_operation <> 'DELETE'
  AND (
    COALESCE(NAME, 'NA')
  ) IN (
    SELECT
      COALESCE(NAME, 'NA') AS NAME
    FROM bec_raw_dl_ext.wf_item_types
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(NAME, 'NA')
  );