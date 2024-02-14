DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_PAY_GROUP;
CREATE TABLE bronze_bec_ods_stg.AP_PAY_GROUP AS
SELECT
  *
FROM bec_raw_dl_ext.AP_PAY_GROUP
WHERE
  kca_operation <> 'DELETE'
  AND (pay_group_id, last_update_date) IN (
    SELECT
      pay_group_id,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_PAY_GROUP
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      pay_group_id
  );