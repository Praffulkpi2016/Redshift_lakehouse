DROP table IF EXISTS bronze_bec_ods_stg.QP_PRICING_ATTRIBUTES;
CREATE TABLE bronze_bec_ods_stg.QP_PRICING_ATTRIBUTES AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.QP_PRICING_ATTRIBUTES
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(PRICING_ATTRIBUTE_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(PRICING_ATTRIBUTE_ID, 0) AS PRICING_ATTRIBUTE_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.QP_PRICING_ATTRIBUTES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        PRICING_ATTRIBUTE_ID
    )
);