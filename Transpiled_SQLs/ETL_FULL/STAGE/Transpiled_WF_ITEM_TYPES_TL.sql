DROP TABLE IF EXISTS bronze_bec_ods_stg.WF_ITEM_TYPES_TL;
CREATE TABLE bronze_bec_ods_stg.WF_ITEM_TYPES_TL AS
SELECT
  *
FROM bec_raw_dl_ext.WF_ITEM_TYPES_TL
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(NAME, 'NA'), COALESCE(LANGUAGE, 'NA')) IN (
    SELECT
      COALESCE(NAME, 'NA') AS NAME,
      COALESCE(LANGUAGE, 'NA') AS LANGUAGE
    FROM bec_raw_dl_ext.WF_ITEM_TYPES_TL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(NAME, 'NA'),
      COALESCE(LANGUAGE, 'NA')
  );