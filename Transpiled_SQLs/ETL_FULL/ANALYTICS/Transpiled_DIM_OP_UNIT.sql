DROP table IF EXISTS gold_bec_dwh.DIM_OP_UNIT;
CREATE TABLE gold_bec_dwh.dim_op_unit AS
(
  SELECT
    CASE WHEN hop.business_group_id = 0 THEN NULL ELSE hop.business_group_id END AS business_group_id,
    FLOOR(hop.organization_id) AS organization_id,
    hop.name AS op_unit_name,
    set_of_books_id,
    gl.name AS ledger_name,
    gl.short_name AS ledger_short_name,
    gl.currency_code AS ledger_currency_code,
    hou.name AS legal_entity_name,
    hou.attribute1 AS attribute1,
    hou.attribute2 AS attribute2,
    hou.attribute3 AS attribute3,
    hou.attribute4 AS attribute4,
    hou.attribute5 AS attribute5,
    hop.short_code,
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
    ) || '-' || COALESCE(hop.organization_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.hr_operating_units AS hop
  INNER JOIN silver_bec_ods.hr_all_organization_units AS hou
    ON hop.organization_id = hou.organization_id
  INNER JOIN silver_bec_ods.gl_ledgers AS gl
    ON hop.set_of_books_id = gl.ledger_id
  WHERE
    1 = 1
    AND (
      hop.is_deleted_flg <> 'Y' OR hou.is_deleted_flg <> 'Y' OR gl.is_deleted_flg <> 'Y'
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_op_unit' AND batch_name = 'ap';