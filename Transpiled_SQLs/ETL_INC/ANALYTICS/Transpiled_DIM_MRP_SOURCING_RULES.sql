/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_MRP_SOURCING_RULES
WHERE
  (COALESCE(sourcing_rule_id, 0), COALESCE(assignment_id, 0), COALESCE(SR_RECEIPT_ID, 0), COALESCE(sr_source_id, 0)) IN (
    SELECT
      COALESCE(ods.sourcing_rule_id, 0) AS sourcing_rule_id,
      COALESCE(ods.assignment_id, 0) AS assignment_id,
      COALESCE(ods.SR_RECEIPT_ID, 0) AS SR_RECEIPT_ID,
      COALESCE(ods.sr_source_id, 0) AS sr_source_id
    FROM gold_bec_dwh.DIM_MRP_SOURCING_RULES AS dw, (
      SELECT
        msra.sourcing_rule_id,
        msra.assignment_id,
        s.sr_receipt_id,
        src.sr_source_id
      FROM silver_bec_ods.mrp_sourcing_rules AS msr, silver_bec_ods.mrp_sr_assignments AS msra, silver_bec_ods.mrp_sr_receipt_org AS s, silver_bec_ods.mrp_sr_source_org AS src, silver_bec_ods.mtl_parameters AS mp
      WHERE
        msr.sourcing_rule_id = msra.sourcing_rule_id
        AND msra.assignment_set_id = 1
        AND msra.sourcing_rule_id = s.SOURCING_RULE_ID()
        AND s.sr_receipt_id = src.SR_RECEIPT_ID()
        AND src.source_organization_id = mp.ORGANIZATION_ID()
        AND (
          msra.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_mrp_sourcing_rules' AND batch_name = 'po'
          )
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.sourcing_rule_id, 0) || '-' || COALESCE(ods.assignment_id, 0) || '-' || COALESCE(ods.SR_RECEIPT_ID, 0) || '-' || COALESCE(ods.sr_source_id, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_MRP_SOURCING_RULES (
  assignment_id,
  assignment_set_id,
  assignment_type,
  organization_id,
  customer_id,
  ship_to_site_id,
  sourcing_rule_type,
  sourcing_rule_id,
  sourcing_rule_name,
  status,
  planning_active,
  category_id,
  category_set_id,
  inventory_item_id,
  secondary_inventory,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  sr_receipt_id,
  s_sourcing_rule_id,
  receipt_organization_id,
  effective_date,
  disable_date,
  sr_source_id,
  src_sr_receipt_id,
  source_organization_id,
  vendor_id,
  vendor_site_id,
  source_type,
  allocation_percent,
  rank,
  ship_method,
  source_organization_code,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    assignment_id,
    assignment_set_id,
    assignment_type,
    organization_id,
    customer_id,
    ship_to_site_id,
    sourcing_rule_type,
    sourcing_rule_id,
    sourcing_rule_name,
    status,
    planning_active,
    category_id,
    category_set_id,
    inventory_item_id,
    secondary_inventory,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    sr_receipt_id,
    s_sourcing_rule_id,
    receipt_organization_id,
    effective_date,
    disable_date,
    sr_source_id,
    src_sr_receipt_id,
    source_organization_id,
    vendor_id,
    vendor_site_id,
    source_type,
    allocation_percent,
    rank,
    ship_method,
    source_organization_code,
    'N' AS is_deleted_flg, /* Audit COLUMNS */
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
    ) || '-' || COALESCE(sourcing_rule_id, 0) || '-' || COALESCE(assignment_id, 0) || '-' || COALESCE(SR_RECEIPT_ID, 0) || '-' || COALESCE(sr_source_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      msra.assignment_id,
      msra.assignment_set_id,
      msra.assignment_type,
      msra.organization_id,
      msra.customer_id,
      msra.ship_to_site_id,
      msra.sourcing_rule_type,
      msra.sourcing_rule_id,
      msr.sourcing_rule_name,
      msr.status,
      msr.planning_active,
      msra.category_id,
      msra.category_set_id,
      msra.inventory_item_id,
      msra.secondary_inventory,
      msra.last_update_date,
      msra.last_updated_by,
      msra.creation_date,
      msra.created_by,
      msra.last_update_login,
      s.sr_receipt_id,
      s.sourcing_rule_id AS s_sourcing_rule_id,
      s.receipt_organization_id,
      s.effective_date,
      s.disable_date,
      src.sr_source_id,
      src.sr_receipt_id AS src_sr_receipt_id,
      src.source_organization_id,
      src.vendor_id,
      src.vendor_site_id,
      src.source_type,
      src.allocation_percent,
      src.rank,
      src.ship_method,
      mp.organization_code AS source_organization_code
    FROM silver_bec_ods.mrp_sourcing_rules AS msr, silver_bec_ods.mrp_sr_assignments AS msra, silver_bec_ods.mrp_sr_receipt_org AS s, silver_bec_ods.mrp_sr_source_org AS src, silver_bec_ods.mtl_parameters AS mp
    WHERE
      msr.sourcing_rule_id = msra.sourcing_rule_id
      AND msra.assignment_set_id = 1
      AND msra.sourcing_rule_id = s.SOURCING_RULE_ID()
      AND s.sr_receipt_id = src.SR_RECEIPT_ID()
      AND src.source_organization_id = mp.ORGANIZATION_ID()
      AND (
        msra.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_mrp_sourcing_rules' AND batch_name = 'po'
        )
      )
  )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_MRP_SOURCING_RULES SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(sourcing_rule_id, 0), COALESCE(assignment_id, 0), COALESCE(SR_RECEIPT_ID, 0), COALESCE(sr_source_id, 0)) IN (
    SELECT
      COALESCE(ods.sourcing_rule_id, 0) AS sourcing_rule_id,
      COALESCE(ods.assignment_id, 0) AS assignment_id,
      COALESCE(ods.SR_RECEIPT_ID, 0) AS SR_RECEIPT_ID,
      COALESCE(ods.sr_source_id, 0) AS sr_source_id
    FROM gold_bec_dwh.DIM_MRP_SOURCING_RULES AS dw, (
      SELECT
        msra.sourcing_rule_id,
        msra.assignment_id,
        s.sr_receipt_id,
        src.sr_source_id
      FROM silver_bec_ods.mrp_sourcing_rules AS msr, silver_bec_ods.mrp_sr_assignments AS msra, silver_bec_ods.mrp_sr_receipt_org AS s, silver_bec_ods.mrp_sr_source_org AS src, silver_bec_ods.mtl_parameters AS mp
      WHERE
        msr.sourcing_rule_id = msra.sourcing_rule_id
        AND msra.assignment_set_id = 1
        AND msra.sourcing_rule_id = s.SOURCING_RULE_ID()
        AND s.sr_receipt_id = src.SR_RECEIPT_ID()
        AND src.source_organization_id = mp.ORGANIZATION_ID()
        AND (
          msra.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_mrp_sourcing_rules' AND batch_name = 'po'
          )
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.sourcing_rule_id, 0) || '-' || COALESCE(ods.assignment_id, 0) || '-' || COALESCE(ods.SR_RECEIPT_ID, 0) || '-' || COALESCE(ods.sr_source_id, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_mrp_sourcing_rules' AND batch_name = 'po';