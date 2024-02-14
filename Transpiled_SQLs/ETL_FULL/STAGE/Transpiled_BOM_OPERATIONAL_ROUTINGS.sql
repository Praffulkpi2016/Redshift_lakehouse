DROP table IF EXISTS bronze_bec_ods_stg.BOM_OPERATIONAL_ROUTINGS;
CREATE TABLE bronze_bec_ods_stg.bom_operational_routings AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.bom_operational_routings
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(ROUTING_SEQUENCE_ID, '0'), last_update_date) IN (
      SELECT
        COALESCE(ROUTING_SEQUENCE_ID, '0') AS ROUTING_SEQUENCE_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.bom_operational_routings
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(ROUTING_SEQUENCE_ID, '0')
    )
);