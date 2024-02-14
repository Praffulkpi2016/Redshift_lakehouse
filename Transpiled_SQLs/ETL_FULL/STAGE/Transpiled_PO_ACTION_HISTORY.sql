DROP TABLE IF EXISTS bronze_bec_ods_stg.PO_ACTION_HISTORY;
CREATE TABLE bronze_bec_ods_stg.PO_ACTION_HISTORY AS
SELECT
  *
FROM bec_raw_dl_ext.PO_ACTION_HISTORY
WHERE
  kca_operation <> 'DELETE'
  AND (OBJECT_ID, OBJECT_TYPE_CODE, OBJECT_SUB_TYPE_CODE, SEQUENCE_NUM, last_update_date) IN (
    SELECT
      OBJECT_ID,
      OBJECT_TYPE_CODE,
      OBJECT_SUB_TYPE_CODE,
      SEQUENCE_NUM,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.PO_ACTION_HISTORY
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      OBJECT_ID,
      OBJECT_TYPE_CODE,
      OBJECT_SUB_TYPE_CODE,
      SEQUENCE_NUM
  );