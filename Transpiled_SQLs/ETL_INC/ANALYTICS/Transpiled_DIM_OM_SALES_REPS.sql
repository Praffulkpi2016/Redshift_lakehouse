/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_OM_SALES_REPS
WHERE
  (COALESCE(SALESREP_ID, 0), COALESCE(ORG_ID, 0)) IN (
    SELECT
      COALESCE(ods.SALESREP_ID, 0) AS SALESREP_ID,
      COALESCE(ods.ORG_ID, 0) AS ORG_ID
    FROM gold_bec_dwh.DIM_OM_SALES_REPS AS dw, (
      SELECT
        RS.ORG_ID,
        RS.SALESREP_ID,
        RS.SALESREP_NUMBER,
        RS.NAME,
        RES.RESOURCE_NAME AS SALESREP_NAME,
        RS.START_DATE_ACTIVE,
        RS.END_DATE_ACTIVE
      FROM BEC_ODS.JTF_RS_SALESREPS AS RS, BEC_ODS.JTF_RS_RESOURCE_EXTNS_TL AS RES
      WHERE
        1 = 1
        AND RS.RESOURCE_ID = RES.RESOURCE_ID
        AND RS.STATUS = 'A'
        AND RES.`LANGUAGE` = 'US'
        AND (
          RS.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_om_sales_reps' AND batch_name = 'om'
          )
          OR RES.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_om_sales_reps' AND batch_name = 'om'
          )
        )
    ) AS ods
    WHERE
      1 = 1
      AND dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.SALESREP_ID, 0) || '-' || COALESCE(odS.ORG_ID, 0)
  );
INSERT INTO gold_bec_dwh.DIM_OM_SALES_REPS (
  ORG_ID,
  SALESREP_ID,
  SALESREP_NUMBER,
  NAME,
  SALESREP_NAME,
  START_DATE_ACTIVE,
  END_DATE_ACTIVE,
  IS_DELETED_FLG,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
    1 = 1
    AND RS.RESOURCE_ID = RES.RESOURCE_ID
    AND RS.STATUS = 'A'
    AND RES.`LANGUAGE` = 'US'
    AND (
      RS.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_om_sales_reps' AND batch_name = 'om'
      )
      OR RES.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_om_sales_reps' AND batch_name = 'om'
      )
    )
);
/* soft delete */
UPDATE gold_bec_dwh.DIM_OM_SALES_REPS SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(SALESREP_ID, 0), COALESCE(ORG_ID, 0)) IN (
    SELECT
      COALESCE(ods.SALESREP_ID, 0) AS SALESREP_ID,
      COALESCE(ods.ORG_ID, 0) AS ORG_ID
    FROM gold_bec_dwh.DIM_OM_SALES_REPS AS dw, (
      SELECT
        RS.ORG_ID,
        RS.SALESREP_ID
      FROM (
        SELECT
          *
        FROM BEC_ODS.JTF_RS_SALESREPS
        WHERE
          is_deleted_flg <> 'Y'
      ) AS RS, (
        SELECT
          *
        FROM BEC_ODS.JTF_RS_RESOURCE_EXTNS_TL
        WHERE
          is_deleted_flg <> 'Y'
      ) AS RES
      WHERE
        1 = 1 AND RS.RESOURCE_ID = RES.RESOURCE_ID AND RS.STATUS = 'A' AND RES.`LANGUAGE` = 'US'
    ) AS ods
    WHERE
      1 = 1
      AND dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.SALESREP_ID, 0) || '-' || COALESCE(odS.ORG_ID, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_om_sales_reps' AND batch_name = 'om';