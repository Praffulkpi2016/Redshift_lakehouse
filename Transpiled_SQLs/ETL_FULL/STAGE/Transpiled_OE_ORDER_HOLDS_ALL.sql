DROP TABLE IF EXISTS bronze_bec_ods_stg.OE_ORDER_HOLDS_ALL;
CREATE TABLE bronze_bec_ods_stg.OE_ORDER_HOLDS_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.OE_ORDER_HOLDS_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(ORDER_HOLD_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(ORDER_HOLD_ID, 0) AS ORDER_HOLD_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.OE_ORDER_HOLDS_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(ORDER_HOLD_ID, 0)
  );