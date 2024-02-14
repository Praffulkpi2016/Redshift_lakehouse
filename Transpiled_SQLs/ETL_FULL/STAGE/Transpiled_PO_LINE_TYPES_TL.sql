DROP TABLE IF EXISTS bronze_bec_ods_stg.PO_LINE_TYPES_TL;
CREATE TABLE bronze_bec_ods_stg.PO_LINE_TYPES_TL AS
SELECT
  *
FROM bec_raw_dl_ext.PO_LINE_TYPES_TL
WHERE
  kca_operation <> 'DELETE'
  AND (LINE_TYPE_ID, LANGUAGE, last_update_date) IN (
    SELECT
      LINE_TYPE_ID,
      LANGUAGE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.PO_LINE_TYPES_TL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      LINE_TYPE_ID,
      LANGUAGE
  );