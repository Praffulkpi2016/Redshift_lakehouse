/* delete */
DELETE FROM gold_bec_dwh.FACT_OM_IR_WITHNO_ISO
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.po_requisition_lines_all AS prl, silver_bec_ods.po_requisition_headers_all AS prh, silver_bec_ods.oe_lines_iface_all AS oli, silver_bec_ods.hr_operating_units AS hou
    WHERE
      prh.requisition_header_id = prl.requisition_header_id
      AND oli.orig_sys_line_ref = prl.requisition_line_id
      AND hou.ORGANIZATION_ID = prh.org_id
      AND TYPE_LOOKUP_CODE = 'INTERNAL'
      AND AUTHORIZATION_STATUS <> 'CANCELLED'
      AND (
        prl.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_om_ir_withno_iso' AND batch_name = 'om'
        )
      )
      AND FACT_OM_IR_WITHNO_ISO.line_num = prl.line_num
      AND FACT_OM_IR_WITHNO_ISO.IR_Number = prh.segment1
  );
/* insert */
INSERT INTO gold_bec_dwh.FACT_OM_IR_WITHNO_ISO
(
  SELECT
    name,
    IR_Number,
    line_num,
    ITEM_DESCRIPTION,
    UNIT_PRICE,
    QUANTITY,
    IR_Creation_date,
    need_by_date,
    'N' AS is_deleted_flg, /* audit columns */
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
    ) || '-' || COALESCE(line_num, 0) || '-' || COALESCE(IR_Number, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT DISTINCT
      hou.name,
      prh.segment1 AS IR_Number,
      prl.line_num,
      ITEM_DESCRIPTION,
      UNIT_PRICE,
      QUANTITY,
      prh.CREATION_DATE AS IR_Creation_date,
      prl.need_by_date AS need_by_date
    FROM (
      SELECT
        *
      FROM silver_bec_ods.po_requisition_lines_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS prl, (
      SELECT
        *
      FROM silver_bec_ods.po_requisition_headers_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS prh, (
      SELECT
        *
      FROM silver_bec_ods.oe_lines_iface_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS oli, (
      SELECT
        *
      FROM silver_bec_ods.hr_operating_units
      WHERE
        is_deleted_flg <> 'Y'
    ) AS hou
    WHERE
      prh.requisition_header_id = prl.requisition_header_id
      AND oli.orig_sys_line_ref = prl.requisition_line_id
      AND hou.ORGANIZATION_ID = prh.org_id
      AND TYPE_LOOKUP_CODE = 'INTERNAL'
      AND AUTHORIZATION_STATUS <> 'CANCELLED'
      AND (
        prl.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_om_ir_withno_iso' AND batch_name = 'om'
        )
      )
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_om_ir_withno_iso' AND batch_name = 'om';