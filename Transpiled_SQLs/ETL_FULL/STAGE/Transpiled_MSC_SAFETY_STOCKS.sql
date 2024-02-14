DROP TABLE IF EXISTS bronze_bec_ods_stg.MSC_SAFETY_STOCKS;
CREATE TABLE bronze_bec_ods_stg.MSC_SAFETY_STOCKS AS
SELECT
  *
FROM bec_raw_dl_ext.MSC_SAFETY_STOCKS
WHERE
  kca_operation <> 'DELETE'
  AND (PLAN_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID, PERIOD_START_DATE, last_update_date) IN (
    SELECT
      PLAN_ID,
      INVENTORY_ITEM_ID,
      ORGANIZATION_ID,
      PERIOD_START_DATE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MSC_SAFETY_STOCKS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      PLAN_ID,
      INVENTORY_ITEM_ID,
      ORGANIZATION_ID,
      PERIOD_START_DATE
  );