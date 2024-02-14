/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_INV_CYCLE_COUNT_ADJ
WHERE
  (COALESCE(inventory_item_id, 0), COALESCE(organization_id, 0), COALESCE(cycle_count_header_id, 0), COALESCE(cycle_count_entry_id, 0)) IN (
    SELECT
      COALESCE(ODS.inventory_item_id, 0) AS inventory_item_id,
      COALESCE(ODS.organization_id, 0) AS organization_id,
      COALESCE(ODS.cycle_count_header_id, 0) AS cycle_count_header_id,
      COALESCE(ODS.cycle_count_entry_id, 0) AS cycle_count_entry_id
    FROM gold_bec_dwh.FACT_INV_CYCLE_COUNT_ADJ AS dw, (
      SELECT
        mcce.inventory_item_id,
        mcce.organization_id,
        mcce.cycle_count_entry_id,
        mcch.cycle_count_header_id
      FROM silver_bec_ods.mtl_cycle_count_headers AS mcch, silver_bec_ods.mtl_cycle_count_entries AS mcce, silver_bec_ods.mtl_system_items_b AS msib, silver_bec_ods.mtl_item_locations AS mil, silver_bec_ods.mtl_transaction_reasons AS REA
      WHERE
        1 = 1
        AND mcce.inventory_item_id = msib.inventory_item_id
        AND mcce.organization_id = msib.organization_id
        AND mcce.cycle_count_header_id = mcch.cycle_count_header_id
        AND mcce.locator_id = mil.inventory_location_id
        AND rea.REASON_ID() = mcce.transaction_reason_id
        AND (
          mcce.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_inv_cycle_count_adj' AND batch_name = 'inv'
          )
          OR mcch.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_inv_cycle_count_adj' AND batch_name = 'inv'
          )
          OR mcce.is_deleted_flg = 'Y'
          OR mcch.is_deleted_flg = 'Y'
          OR msib.is_deleted_flg = 'Y'
          OR mil.is_deleted_flg = 'Y'
          OR REA.is_deleted_flg = 'Y'
        )
    ) AS ODS
    WHERE
      1 = 1
      AND dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.inventory_item_id, 0) || '-' || COALESCE(ods.organization_id, 0) || '-' || COALESCE(ods.cycle_count_header_id, 0) || '-' || COALESCE(ods.cycle_count_entry_id, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_INV_CYCLE_COUNT_ADJ (
  inventory_item_id,
  organization_id,
  cycle_count_header_name,
  item,
  system_quantity,
  count_uom_first,
  adjustment_quantity,
  adjustment_amount,
  adjustment_percentage,
  subinventory,
  `locator`,
  remaining_quantity,
  counter,
  count_date,
  count_list_sequence,
  count_status_code,
  reason,
  description,
  cycle_count_request_date,
  cycle_count_due_date,
  approval_date,
  cycle_count_approved_by,
  cycle_count_entry_id,
  cycle_count_header_id,
  inventory_item_id_key,
  organization_id_key,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    mcce.inventory_item_id,
    mcce.organization_id,
    mcch.cycle_count_header_name,
    msib.segment1 AS Item,
    mcce.system_quantity_current AS system_quantity,
    mcce.count_uom_first,
    mcce.adjustment_quantity,
    mcce.adjustment_amount,
    ROUND(
      (
        mcce.adjustment_quantity / CASE
          WHEN mcce.system_quantity_current = 0
          THEN 1
          ELSE mcce.system_quantity_current
        END
      ) * 100,
      2
    ) AS Adjustment_percentage,
    mcce.SUBINVENTORY,
    mil.segment1 || '.' || mil.segment2 || '.' || mil.segment3 AS locator,
    (
      mcce.system_quantity_current + (
        mcce.adjustment_quantity
      )
    ) AS Remaining_quantity,
    (
      SELECT DISTINCT
        full_name
      FROM (
        SELECT
          *
        FROM silver_bec_ods.per_all_people_f
        WHERE
          is_deleted_flg <> 'Y'
      ) AS per_all_people_f
      WHERE
        person_id = mcce.counted_by_employee_id_current
        AND effective_end_Date > CURRENT_TIMESTAMP()
    ) AS counter,
    mcce.count_date_current AS count_Date,
    mcce.count_list_sequence,
    mcce.entry_status_code AS count_status_code,
    rea.reason_name AS reason,
    rea.description,
    mcce.creation_Date AS Cycle_count_request_date,
    mcce.count_due_Date AS cycle_count_due_date,
    mcce.approval_Date,
    (
      SELECT DISTINCT
        full_name
      FROM (
        SELECT
          *
        FROM silver_bec_ods.per_all_people_f
        WHERE
          is_deleted_flg <> 'Y'
      ) AS per_all_people_f
      WHERE
        person_id = mcce.approver_employee_id AND effective_end_Date > CURRENT_TIMESTAMP()
    ) AS cycle_count_approved_by,
    mcce.cycle_count_entry_id,
    mcch.cycle_count_header_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || mcce.inventory_item_id AS INVENTORY_ITEM_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || mcce.organization_id AS ORGANIZATION_ID_KEY,
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
    ) || '-' || COALESCE(mcce.inventory_item_id, 0) || '-' || COALESCE(mcce.organization_id, 0) || '-' || COALESCE(mcch.cycle_count_header_id, 0) || '-' || COALESCE(mcce.cycle_count_entry_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.mtl_cycle_count_headers
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mcch, (
    SELECT
      *
    FROM silver_bec_ods.mtl_cycle_count_entries
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mcce, (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msib, (
    SELECT
      *
    FROM silver_bec_ods.mtl_item_locations
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mil, (
    SELECT
      *
    FROM silver_bec_ods.mtl_transaction_reasons
    WHERE
      is_deleted_flg <> 'Y'
  ) AS REA
  WHERE
    1 = 1
    AND mcce.inventory_item_id = msib.inventory_item_id
    AND mcce.organization_id = msib.organization_id
    AND mcce.cycle_count_header_id = mcch.cycle_count_header_id
    AND mcce.locator_id = mil.inventory_location_id
    AND rea.REASON_ID() = mcce.transaction_reason_id
    AND (
      mcce.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_inv_cycle_count_adj' AND batch_name = 'inv'
      )
      OR mcch.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_inv_cycle_count_adj' AND batch_name = 'inv'
      )
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_inv_cycle_count_adj' AND batch_name = 'inv';