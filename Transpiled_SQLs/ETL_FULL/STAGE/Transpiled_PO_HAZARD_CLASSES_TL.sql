DROP TABLE IF EXISTS bronze_bec_ods_stg.PO_HAZARD_CLASSES_TL;
CREATE TABLE bronze_bec_ods_stg.PO_HAZARD_CLASSES_TL AS
SELECT
  *
FROM bec_raw_dl_ext.PO_HAZARD_CLASSES_TL
WHERE
  kca_operation <> 'DELETE'
  AND (HAZARD_CLASS_ID, LANGUAGE, last_update_date) IN (
    SELECT
      HAZARD_CLASS_ID,
      LANGUAGE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.PO_HAZARD_CLASSES_TL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      HAZARD_CLASS_ID,
      LANGUAGE
  );