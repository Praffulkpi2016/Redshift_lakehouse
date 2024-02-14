DROP table IF EXISTS gold_bec_dwh.DIM_SEGMENT1;
CREATE TABLE gold_bec_dwh.DIM_SEGMENT1 AS
(
  SELECT DISTINCT
    s1.chart_of_accounts_id AS chart_of_accounts_id,
    s1.flex_value_set_id AS flex_value_set_id,
    s1.ledger_id AS ledger_id,
    s1.ledger_name AS ledger_name,
    CONCAT(
      COALESCE(s1.chart_of_accounts_id, ''),
      COALESCE(CONCAT(COALESCE('-', ''), COALESCE(f1.flex_value, '')), '')
    ) AS coa_segval,
    f1.flex_value AS level0_value,
    f1.description AS level0_desc,
    CONCAT(
      COALESCE(f1.flex_value, ''),
      COALESCE(CONCAT(COALESCE('-', ''), COALESCE(f1.description, '')), '')
    ) AS seg1_value_desc,
    s1.segment_name AS segment_name,
    f1.creation_date AS creation_date,
    f1.last_update_date AS last_update,
    'N' AS is_deleted_flg, /* audit column */
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
    ) || '-' || COALESCE(s1.ledger_id, 0) || '-' || COALESCE(s1.chart_of_accounts_id, 0) || '-' || COALESCE(s1.flex_value_set_id, 0) || '-' || COALESCE(f1.flex_value, '0') || '-' || COALESCE(s1.segment_name, '0') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      f1.flex_value_set_id,
      f1.flex_value_id,
      f1.flex_value,
      f1.creation_date,
      f1.last_update_date,
      f2.description
    FROM (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS f1
    INNER JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS f2
      ON f1.flex_value_id = f2.flex_value_id AND f2.`language` = 'US'
    WHERE
      f1.is_deleted_flg <> 'Y'
  ) AS f1
  INNER JOIN (
    SELECT
      f1.application_id,
      g1.ledger_id,
      f1.id_flex_num,
      f1.id_flex_structure_code,
      f1.id_flex_structure_name,
      f1.description,
      f2.application_column_name,
      f2.segment_name,
      f2.segment_num,
      f2.flex_value_set_id,
      g1.chart_of_accounts_id,
      g1.name AS ledger_name
    FROM (
      SELECT
        *
      FROM silver_bec_ods.fnd_id_flex_structures_vl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS f1
    INNER JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_id_flex_segments
      WHERE
        is_deleted_flg <> 'Y'
    ) AS f2
      ON f1.id_flex_num = f2.id_flex_num
      AND f1.id_flex_code = f2.id_flex_code
      AND f2.id_flex_code = 'GL#'
    INNER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_ledgers
      WHERE
        is_deleted_flg <> 'Y'
    ) AS g1
      ON f1.id_flex_num = g1.chart_of_accounts_id
    WHERE
      f1.is_deleted_flg <> 'Y'
  ) AS s1
    ON f1.flex_value_set_id = s1.flex_value_set_id
    AND s1.application_column_name = 'SEGMENT1'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_segment1' AND batch_name = 'gl';