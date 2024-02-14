DROP TABLE IF EXISTS bronze_bec_ods_stg.mtl_supply;
CREATE TABLE bronze_bec_ods_stg.mtl_supply AS
SELECT
  *
FROM bec_raw_dl_ext.mtl_supply
WHERE
  kca_operation <> 'DELETE'
  AND (SUPPLY_TYPE_CODE, SUPPLY_SOURCE_ID, COALESCE(PO_DISTRIBUTION_ID, 0), last_update_date) IN (
    SELECT
      SUPPLY_TYPE_CODE,
      SUPPLY_SOURCE_ID,
      COALESCE(PO_DISTRIBUTION_ID, 0),
      MAX(last_update_date)
    FROM bec_raw_dl_ext.mtl_supply
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      SUPPLY_TYPE_CODE,
      SUPPLY_SOURCE_ID,
      COALESCE(PO_DISTRIBUTION_ID, 0)
  );