DROP table IF EXISTS gold_bec_dwh.DIM_OM_SALES_REPS;
CREATE TABLE gold_bec_dwh.DIM_OM_SALES_REPS AS
(
  SELECT
    RS.ORG_ID,
    RS.SALESREP_ID,
    RS.SALESREP_NUMBER,
    RS.NAME,
    RES.RESOURCE_NAME AS SALESREP_NAME,
    RS.START_DATE_ACTIVE,
    RS.END_DATE_ACTIVE,
    'N' AS is_deleted_flg,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) AS source_app_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(RS.SALESREP_ID, 0) || '-' || COALESCE(RS.ORG_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM BEC_ODS.JTF_RS_SALESREPS AS RS, BEC_ODS.JTF_RS_RESOURCE_EXTNS_TL AS RES
  WHERE
    1 = 1 AND RS.RESOURCE_ID = RES.RESOURCE_ID AND RS.STATUS = 'A' AND RES.`LANGUAGE` = 'US'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_om_sales_reps' AND batch_name = 'om';