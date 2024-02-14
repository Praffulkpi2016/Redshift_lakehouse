DROP table IF EXISTS bronze_bec_ods_stg.RA_CUST_TRX_TYPES_ALL;
CREATE TABLE bronze_bec_ods_stg.RA_CUST_TRX_TYPES_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.RA_CUST_TRX_TYPES_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (CUST_TRX_TYPE_ID, COALESCE(org_id, 0), last_update_date /* zd_edition_name, */) IN (
    SELECT
      CUST_TRX_TYPE_ID,
      COALESCE(org_id, 0) AS org_id,
      MAX(last_update_date) /*		zd_edition_name, */
    FROM bec_raw_dl_ext.RA_CUST_TRX_TYPES_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      CUST_TRX_TYPE_ID,
      org_id
  ) /* ,zd_edition_name */;