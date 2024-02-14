/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_ASL_SR_VALIDATION
WHERE
  (COALESCE(item_id, 0), COALESCE(asl_id, 0), COALESCE(document_header_id, 0), COALESCE(document_line_id, 0), COALESCE(using_organization_id, 0), COALESCE(owning_organization_id, 0)) IN (
    SELECT
      COALESCE(ods.item_id, 0) AS item_id,
      COALESCE(ods.asl_id, 0) AS asl_id,
      COALESCE(ods.document_header_id, 0) AS document_header_id,
      COALESCE(ods.document_line_id, 0) AS document_line_id,
      COALESCE(ods.using_organization_id, 0) AS using_organization_id,
      COALESCE(ods.owning_organization_id, 0) AS owning_organization_id
    FROM gold_bec_dwh.FACT_ASL_SR_VALIDATION AS dw, (
      SELECT DISTINCT
        paa.asl_id,
        paa.using_organization_id,
        pasl.item_id,
        pasl.owning_organization_id,
        pad.document_header_id,
        pad.document_line_id
      FROM BEC_ODS.po_asl_attributes AS paa, BEC_ODS.po_approved_supplier_list AS pasl, BEC_ODS.po_asl_documents AS pad, BEC_ODS.po_headers_all AS ph, BEC_ODS.po_lines_all AS pl, BEC_ODS.mtl_system_items_b AS msi, BEC_ODS.fnd_lookup_values AS ml1, BEC_ODS.fnd_lookup_values AS ml2, BEC_ODS.mtl_item_sub_defaults AS dsub, BEC_ODS.fnd_lookup_values AS plc, BEC_ODS.fnd_lookup_values AS plc1, BEC_ODS.fnd_lookup_values AS plc2
      WHERE
        pasl.item_id = msi.inventory_item_id
        AND pasl.owning_organization_id = msi.organization_id
        AND pasl.asl_id = paa.asl_id
        AND pasl.asl_id = pad.asl_id
        AND pad.document_header_id = ph.PO_HEADER_ID()
        AND ph.po_header_id = pl.PO_HEADER_ID()
        AND (
          pad.document_line_id IS NULL OR pad.document_line_id = pl.po_line_id
        )
        AND ml1.LOOKUP_TYPE() = 'MTL_MATERIAL_PLANNING'
        AND ml1.LANGUAGE() = 'US'
        AND msi.inventory_planning_code = ml1.lookup_code
        AND ml2.LOOKUP_TYPE() = 'MRP_PLANNING_CODE'
        AND ml2.LANGUAGE() = 'US'
        AND msi.mrp_planning_code = ml2.lookup_code
        AND msi.organization_id = dsub.ORGANIZATION_ID()
        AND msi.inventory_item_id = dsub.INVENTORY_ITEM_ID()
        AND plc2.LOOKUP_TYPE() = 'SHIP_SCHEDULE_SUBTYPE'
        AND plc2.LANGUAGE() = 'US'
        AND paa.ship_schedule_type = plc2.LOOKUP_CODE()
        AND plc1.LOOKUP_TYPE() = 'PLAN_SCHEDULE_SUBTYPE'
        AND plc1.LANGUAGE() = 'US'
        AND paa.plan_schedule_type = plc1.LOOKUP_CODE()
        AND paa.release_generation_method = plc.LOOKUP_CODE()
        AND plc.LOOKUP_TYPE() = 'DOC GENERATION METHOD'
        AND plc.LANGUAGE() = 'US'
        AND (
          paa.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_asl_sr_validation' AND batch_name = 'po'
          )
          OR paa.is_deleted_flg = 'Y'
          OR pasl.is_deleted_flg = 'Y'
          OR pad.is_deleted_flg = 'Y'
          OR ph.is_deleted_flg = 'Y'
          OR pl.is_deleted_flg = 'Y'
          OR msi.is_deleted_flg = 'Y'
          OR ml1.is_deleted_flg = 'Y'
          OR ml2.is_deleted_flg = 'Y'
          OR dsub.is_deleted_flg = 'Y'
          OR plc.is_deleted_flg = 'Y'
          OR plc1.is_deleted_flg = 'Y'
          OR plc2.is_deleted_flg = 'Y'
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.item_id, 0) || '-' || COALESCE(ods.asl_id, 0) || '-' || COALESCE(ods.document_header_id, 0) || '-' || COALESCE(ods.document_line_id, 0) || '-' || COALESCE(ods.using_organization_id, 0) || '-' || COALESCE(ods.owning_organization_id, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_ASL_SR_VALIDATION (
  asl_id,
  asl_status_id,
  buyer_id,
  using_organization_id,
  item_id,
  owning_organization_id,
  ship_bucket_pattern_id,
  plan_bucket_pattern_id,
  scheduler_id,
  inventory_item_id,
  vendor_id,
  vendor_site_id,
  organization_id,
  global_flag,
  disable_flag,
  part_number,
  description,
  list_price_per_unit,
  fixed_days_supply,
  fixed_lot_multiplier,
  std_lot_size,
  release_time_fence_days,
  inventory_planning_code,
  planner_code,
  mrp_planning_code,
  default_subinventory,
  document_type_code,
  document_header_id,
  document_line_id,
  document_num,
  line_num,
  document_status,
  start_date,
  end_Date,
  purchasing_unit_of_measure,
  release_method,
  document_sourcing_method,
  enable_authorizations_flag,
  enable_autoschedule_flag,
  enable_plan_schedule_flag,
  enable_vmi_auto_replenish_flag,
  enable_vmi_flag,
  vmi_replenishment_approval,
  vmi_max_qty,
  vmi_min_days,
  vmi_min_qty,
  vmi_max_days,
  consigned_from_supplier_flag,
  consigned_billing_cycle,
  consume_on_aging_flag,
  aging_period,
  last_billing_date,
  fixed_lot_multiple,
  fixed_order_quantity,
  forecast_horizon,
  min_order_qty,
  plan_schedule_type,
  ship_schedule_type,
  price_update_tolerance,
  processing_lead_time,
  replenishment_method,
  asl_id_key,
  asl_status_id_key,
  buyer_id_key,
  using_organization_id_key,
  item_id_key,
  owning_organization_id_key,
  ship_bucket_pattern_id_key,
  plan_bucket_pattern_id_key,
  scheduler_id_key,
  inventory_item_id_key,
  vendor_id_key,
  vendor_site_id_key,
  organization_id_key,
  document_header_id_key,
  document_line_id_key,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    asl_id,
    asl_status_id,
    buyer_id,
    using_organization_id,
    item_id,
    owning_organization_id,
    ship_bucket_pattern_id,
    plan_bucket_pattern_id,
    scheduler_id,
    inventory_item_id,
    vendor_id,
    vendor_site_id,
    organization_id,
    global_flag,
    disable_flag,
    part_number,
    description,
    list_price_per_unit,
    fixed_days_supply,
    fixed_lot_multiplier,
    std_lot_size,
    release_time_fence_days,
    inventory_planning_code,
    planner_code,
    mrp_planning_code,
    default_subinventory,
    document_type_code,
    document_header_id,
    document_line_id,
    document_num,
    line_num,
    document_status,
    start_date,
    end_Date,
    purchasing_unit_of_measure,
    release_method,
    document_sourcing_method,
    enable_authorizations_flag,
    enable_autoschedule_flag,
    enable_plan_schedule_flag,
    enable_vmi_auto_replenish_flag,
    enable_vmi_flag,
    vmi_replenishment_approval,
    vmi_max_qty,
    vmi_min_days,
    vmi_min_qty,
    vmi_max_days,
    consigned_from_supplier_flag,
    consigned_billing_cycle,
    consume_on_aging_flag,
    aging_period,
    last_billing_date,
    fixed_lot_multiple,
    fixed_order_quantity,
    forecast_horizon,
    min_order_qty,
    plan_schedule_type,
    ship_schedule_type,
    price_update_tolerance,
    processing_lead_time,
    replenishment_method,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || asl_id AS asl_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || asl_status_id AS asl_status_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || buyer_id AS buyer_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || using_organization_id AS using_organization_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || item_id AS item_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || owning_organization_id AS owning_organization_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || ship_bucket_pattern_id AS ship_bucket_pattern_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || plan_bucket_pattern_id AS plan_bucket_pattern_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || scheduler_id AS scheduler_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || inventory_item_id AS inventory_item_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || vendor_id AS vendor_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || vendor_site_id AS vendor_site_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || organization_id AS organization_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || document_header_id AS document_header_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || document_line_id AS document_line_id_KEY,
    'N' AS IS_DELETED_FLG,
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
    ) || '-' || COALESCE(item_id, 0) || '-' || COALESCE(asl_id, 0) || '-' || COALESCE(document_header_id, 0) || '-' || COALESCE(document_line_id, 0) || '-' || COALESCE(using_organization_id, 0) || '-' || COALESCE(owning_organization_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT DISTINCT
      paa.asl_id,
      pasl.asl_status_id,
      msi.buyer_id,
      paa.using_organization_id,
      pasl.item_id,
      pasl.owning_organization_id,
      paa.ship_bucket_pattern_id,
      paa.plan_bucket_pattern_id,
      paa.scheduler_id,
      msi.inventory_item_id,
      pasl.vendor_id,
      pasl.vendor_site_id,
      msi.organization_id,
      CASE
        WHEN pasl.using_organization_id = -1
        OR (
          pasl.using_organization_id IS NULL AND -1 IS NULL
        )
        THEN 'Y'
        ELSE 'N'
      END AS global_flag, /* ood.organization_code                                                       owning_organization_code, */ /* pas.status, */
      pasl.disable_flag,
      msi.segment1 AS part_number,
      msi.description,
      msi.list_price_per_unit,
      msi.fixed_days_supply,
      msi.fixed_lot_multiplier,
      msi.std_lot_size,
      msi.release_time_fence_days,
      ml1.meaning AS inventory_planning_code,
      msi.planner_code,
      ml2.meaning AS mrp_planning_code,
      dsub.subinventory_code AS default_subinventory,
      pad.document_type_code,
      pad.document_header_id,
      pad.document_line_id,
      ph.clm_document_number AS document_num,
      COALESCE(pl.line_num_display, CAST(pl.line_num AS CHAR)) AS line_num,
      ph.status_lookup_code,
      CASE
        WHEN pad.document_type_code = 'QUOTATION'
        THEN ph.status_lookup_code
        WHEN pad.document_type_code = 'BLANKET'
        THEN ph.authorization_status
        WHEN pad.document_type_code = 'CONTRACT'
        THEN ph.authorization_status
      END AS document_status,
      ph.start_date, /* , po_headers.start_date */
      ph.end_date, /* pad.effective_to,   po_headers.end_date */
      paa.purchasing_unit_of_measure,
      plc.meaning AS release_method,
      paa.document_sourcing_method,
      paa.enable_authorizations_flag,
      paa.enable_autoschedule_flag,
      paa.enable_plan_schedule_flag,
      paa.enable_vmi_auto_replenish_flag,
      paa.enable_vmi_flag,
      paa.vmi_replenishment_approval,
      paa.vmi_max_qty,
      paa.vmi_min_days,
      paa.vmi_min_qty,
      paa.vmi_max_days,
      paa.consigned_from_supplier_flag,
      paa.consigned_billing_cycle,
      paa.consume_on_aging_flag,
      paa.aging_period,
      paa.last_billing_date,
      paa.fixed_lot_multiple,
      paa.fixed_order_quantity,
      paa.forecast_horizon,
      paa.min_order_qty,
      plc1.meaning AS plan_schedule_type,
      plc2.meaning AS ship_schedule_type,
      paa.price_update_tolerance,
      paa.processing_lead_time,
      CASE
        WHEN paa.replenishment_method = 1
        THEN 'Min-Max Quantities'
        WHEN paa.replenishment_method = 2
        THEN 'Min-Max Days'
        WHEN paa.replenishment_method = 3
        THEN 'Min Qty and Fixed Order Qty'
        WHEN paa.replenishment_method = 4
        THEN 'Min Days and Fixed Order Qty'
      END AS replenishment_method
    /* ppf.full_name                                                               scheduler_name */ /* cbp1.bucket_pattern_name                                                    ship_bucket_pattern */
    FROM (
      SELECT
        *
      FROM BEC_ODS.po_asl_attributes
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS paa, (
      SELECT
        *
      FROM BEC_ODS.po_approved_supplier_list
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS pasl, (
      SELECT
        *
      FROM BEC_ODS.po_asl_documents
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS pad, (
      SELECT
        *
      FROM BEC_ODS.po_headers_all
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS ph, (
      SELECT
        *
      FROM BEC_ODS.po_lines_all
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS pl, (
      SELECT
        *
      FROM BEC_ODS.mtl_system_items_b
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS msi, (
      SELECT
        *
      FROM BEC_ODS.fnd_lookup_values
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS ml1, (
      SELECT
        *
      FROM BEC_ODS.fnd_lookup_values
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS ml2, (
      SELECT
        *
      FROM BEC_ODS.mtl_item_sub_defaults
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS dsub, (
      SELECT
        *
      FROM BEC_ODS.fnd_lookup_values
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS plc, (
      SELECT
        *
      FROM BEC_ODS.fnd_lookup_values
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS plc1, (
      SELECT
        *
      FROM BEC_ODS.fnd_lookup_values
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS plc2
    WHERE
      pasl.item_id = msi.inventory_item_id
      AND pasl.owning_organization_id = msi.organization_id
      AND pasl.asl_id = paa.asl_id
      AND pasl.asl_id = pad.asl_id
      AND pad.document_header_id = ph.PO_HEADER_ID()
      AND ph.po_header_id = pl.PO_HEADER_ID()
      AND (
        pad.document_line_id IS NULL OR pad.document_line_id = pl.po_line_id
      )
      AND ml1.LOOKUP_TYPE() = 'MTL_MATERIAL_PLANNING'
      AND ml1.LANGUAGE() = 'US'
      AND msi.inventory_planning_code = ml1.lookup_code
      AND ml2.LOOKUP_TYPE() = 'MRP_PLANNING_CODE'
      AND ml2.LANGUAGE() = 'US'
      AND msi.mrp_planning_code = ml2.lookup_code
      AND msi.organization_id = dsub.ORGANIZATION_ID()
      AND msi.inventory_item_id = dsub.INVENTORY_ITEM_ID()
      AND plc2.LOOKUP_TYPE() = 'SHIP_SCHEDULE_SUBTYPE'
      AND plc2.LANGUAGE() = 'US'
      AND paa.ship_schedule_type = plc2.LOOKUP_CODE()
      AND plc1.LOOKUP_TYPE() = 'PLAN_SCHEDULE_SUBTYPE'
      AND plc1.LANGUAGE() = 'US'
      AND paa.plan_schedule_type = plc1.LOOKUP_CODE()
      AND paa.release_generation_method = plc.LOOKUP_CODE()
      AND plc.LOOKUP_TYPE() = 'DOC GENERATION METHOD'
      AND plc.LANGUAGE() = 'US'
      AND (
        paa.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_asl_sr_validation' AND batch_name = 'po'
        )
      ) /* AND paa.scheduler_id = ppf.person_id (+) */
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_asl_sr_validation' AND batch_name = 'po';