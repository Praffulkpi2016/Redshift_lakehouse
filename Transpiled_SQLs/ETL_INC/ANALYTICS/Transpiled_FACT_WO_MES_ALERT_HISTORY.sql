TRUNCATE table gold_bec_dwh.FACT_WO_MES_ALERT_HISTORY;
WITH alerts AS (
  SELECT DISTINCT
    aah.application_id,
    aah.alert_id,
    aah.check_id,
    aah.last_update_date,
    aoh.name,
    aoh.row_number,
    aoh.value,
    aah.action_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.alr_action_history
    WHERE
      is_deleted_flg <> 'Y'
  ) AS aah, (
    SELECT
      *
    FROM silver_bec_ods.alr_output_history
    WHERE
      is_deleted_flg <> 'Y'
  ) AS aoh
  WHERE
    aoh.check_id = aah.check_id
    AND aah.alert_id IN (134021, 136021, 146021)
    AND aah.action_id IN (137181, 159181, 169181)
    AND aoh.NAME IN ('WIP_ENTITY_NAME', 'ERROR_EXPLANATION', 'ERROR_MESSAGE')
)
INSERT INTO gold_bec_dwh.FACT_WO_MES_ALERT_HISTORY
(
  SELECT
    organization_code,
    work_order,
    error_message,
    error_date,
    error_end_date,
    CASE
      WHEN REGEXP_INSTR(error_message, 'Serial number ') = 1
      THEN SUBSTRING(
        error_message,
        REGEXP_INSTR(error_message, 'Serial number ') + 14,
        REGEXP_INSTR(error_message, ' ', 1, 3) - (
          REGEXP_INSTR(error_message, 'Serial number ') + 14
        )
      )
      ELSE NULL
    END AS serial_nummber,
    CASE
      WHEN REGEXP_INSTR(error_message, ':') - 1 < 0
      THEN NULL
      ELSE SUBSTRING(error_message, 1, REGEXP_INSTR(error_message, ':') - 1)
    END AS part_number,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || organization_code AS organization_code_KEY,
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
    ) || '-' || COALESCE(work_order, 'NA') || '-' || COALESCE(error_message, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT DISTINCT
      'NW1' AS organization_code,
      wo.value AS work_order,
      err.value AS error_message,
      MIN(wo.last_update_date) AS error_date,
      MAX(wo.last_update_date) AS error_end_date
    FROM alerts AS wo, alerts AS err
    WHERE
      wo.alert_id IN (134021, 136021)
      AND wo.action_id IN (137181, 159181)
      AND wo.NAME IN ('WIP_ENTITY_NAME')
      AND err.alert_id IN (134021, 136021)
      AND err.action_id IN (137181, 159181)
      AND err.NAME IN ('ERROR_EXPLANATION', 'ERROR_MESSAGE')
      AND wo.check_id = err.check_id
      AND wo.row_number = err.row_number
    GROUP BY
      wo.value,
      err.value
    UNION
    SELECT DISTINCT
      'RF1' AS organization_code,
      wo.value AS work_order,
      err.value AS error_message,
      MIN(wo.last_update_date) AS error_date,
      MAX(wo.last_update_date) AS error_end_date
    FROM alerts AS wo, alerts AS err
    WHERE
      wo.alert_id IN (146021)
      AND wo.action_id IN (169181)
      AND wo.NAME IN ('WIP_ENTITY_NAME')
      AND err.alert_id IN (146021)
      AND err.action_id IN (169181)
      AND err.NAME IN ('ERROR_EXPLANATION')
      AND wo.check_id = err.check_id
      AND wo.row_number = err.row_number
    GROUP BY
      wo.value,
      err.value
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_wo_mes_alert_history' AND batch_name = 'wip';