DROP table IF EXISTS gold_bec_dwh.FACT_CYCLE_COUNT_ENTRY;
CREATE TABLE gold_bec_dwh.FACT_CYCLE_COUNT_ENTRY AS
SELECT
  cce.cycle_count_header_id,
  cycle_count_entry_id,
  cch.cycle_count_header_name,
  CCH.ABC_ASSIGNMENT_GROUP_ID,
  cch.organization_id,
  msi.inventory_item_id,
  cce.creation_date,
  cce.last_update_date,
  count_list_sequence,
  msi.segment1,
  msi.description,
  subinventory,
  cce.locator_id,
  revision,
  lot_number,
  lot_control,
  approval_type,
  serial_detail,
  count_quantity_current,
  system_quantity_current,
  adjustment_quantity,
  adjustment_date,
  adjustment_amount,
  item_unit_cost,
  COALESCE(count_quantity_current, 0) * COALESCE(item_unit_cost, 0) AS extended_cost,
  approval_date,
  DATE_FORMAT(approval_date, 'yyyy') AS Approval_year,
  DATE_FORMAT(approval_date, '%U') AS work_week,
  cce.counted_by_employee_id_current,
  approver_employee_id,
  CASE WHEN adjustment_quantity = 0 THEN 1 ELSE 0 END AS hit_miss_percent,
  cce.transaction_reason_id,
  (
    SELECT
      reason_name
    FROM silver_bec_ods.mtl_transaction_reasons
    WHERE
      is_deleted_flg <> 'Y' AND reason_id = cce.transaction_reason_id
  ) AS transaction_reason,
  cce.reference_current,
  cce.reference_first,
  cce.reference_prior,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || cce.cycle_count_header_id AS cycle_count_header_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || cycle_count_entry_id AS cycle_count_entry_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || CCH.ABC_ASSIGNMENT_GROUP_ID AS ABC_ASSIGNMENT_GROUP_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || cch.organization_id AS organization_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || cce.transaction_reason_id AS transaction_reason_id_KEY,
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
  ) || '-' || COALESCE(cycle_count_entry_id, 0) AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM (
  SELECT
    *
  FROM silver_bec_ods.mtl_cycle_count_entries
  WHERE
    is_deleted_flg <> 'Y'
) AS cce, (
  SELECT
    *
  FROM silver_bec_ods.mtl_system_items_b
  WHERE
    is_deleted_flg <> 'Y'
) AS msi, (
  SELECT
    *
  FROM silver_bec_ods.mtl_cycle_count_headers
  WHERE
    is_deleted_flg <> 'Y'
) AS cch
WHERE
  entry_status_code = 5
  AND cce.inventory_item_id = msi.inventory_item_id
  AND msi.organization_id = 90
  AND cce.cycle_count_header_id = cch.cycle_count_header_id;
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_cycle_count_entry' AND batch_name = 'inv';