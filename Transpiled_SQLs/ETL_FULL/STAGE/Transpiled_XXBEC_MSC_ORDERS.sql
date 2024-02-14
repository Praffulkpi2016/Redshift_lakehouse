DROP table IF EXISTS bronze_bec_ods_stg.XXBEC_MSC_ORDERS;
CREATE TABLE bronze_bec_ods_stg.XXBEC_MSC_ORDERS AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.XXBEC_MSC_ORDERS
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(TRANSACTION_ID, 0), COALESCE(INVENTORY_ITEM_ID, 0)) IN (
      SELECT
        COALESCE(TRANSACTION_ID, 0) AS TRANSACTION_ID,
        COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID
      FROM bec_raw_dl_ext.XXBEC_MSC_ORDERS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        TRANSACTION_ID,
        INVENTORY_ITEM_ID
    )
);