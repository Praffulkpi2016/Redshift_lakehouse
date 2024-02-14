create or replace
procedure fact_cst_onhand_qty_snp_proc(v_current_date date)
as $$

begin
execute 'delete from gold_bec_dwh.fact_cst_onhand_qty_snp where snp_date =' || quote_literal(v_current_date);

execute 'INSERT INTO gold_bec_dwh.fact_cst_onhand_qty_snp
(part_number,
    cat_seg1,
    cat_seg2,
    category,
    organization_name,
    operating_unit,
    description,
    unit_of_measure,
    inventory_item_status_code,
    planning_make_buy_code,
    mrp_planning_code,
    quantity,
    material_cost,
    material_overhead_cost,
    resource_cost,
    outside_processing_cost,
    overhead_cost,
    item_cost,
	ext_material_cost,
	ext_material_overhead_cost,
	ext_resource_cost,
	ext_outside_processing_cost,
	ext_overhead_cost,
	extended_cost,
    subinventory,
    locator,
    serial_number,
    lot_number,
    date_received,
    transaction_type_id,
    transaction_source_type_id,
    organization_id,
    source_date_aging,
    material_account,
    subinventory_type,
    subinventory_category,
    subinventory_subcategory,
    uom_code,
    vmi_flag,
    organization_id_key,
    transaction_type_id_key,
    transaction_source_type_id_key,
    source_app_id,
    dw_load_id,
	rownumber,
    snp_date)
(select part_number,
    cat_seg1,
    cat_seg2,
    category,
    organization_name,
    operating_unit,
    description,
    unit_of_measure,
    inventory_item_status_code,
    planning_make_buy_code,
    mrp_planning_code,
    quantity,
    material_cost,
    material_overhead_cost,
    resource_cost,
    outside_processing_cost,
    overhead_cost,
    item_cost,
	ext_material_cost,
	ext_material_overhead_cost,
	ext_resource_cost,
	ext_outside_processing_cost,
	ext_overhead_cost,
	extended_cost,
    subinventory,
    locator,
    serial_number,
    lot_number,
    date_received,
    transaction_type_id,
    transaction_source_type_id,
    organization_id,
    source_date_aging,
    material_account,
    subinventory_type,
    subinventory_category,
    subinventory_subcategory,
    uom_code,
    vmi_flag,
    organization_id_key,
    transaction_type_id_key,
    transaction_source_type_id_key,
    source_app_id,
    dw_load_id,
	rownumber,'
    ||quote_literal(v_current_date)|| ' from gold_bec_dwh.fact_cst_onhand_qty)';
	
-- Purge the Snapshot data which exceeds 90 days and keep only last date of every month.

execute 'delete from gold_bec_dwh.fact_cst_onhand_qty_snp where 
 snp_date in (SELECT
  distinct snp_date
FROM
  gold_bec_dwh.fact_cst_onhand_qty_snp
WHERE
  snp_date not IN (
    SELECT   MAX(snp_date)
    FROM     gold_bec_dwh.fact_cst_onhand_qty_snp
    GROUP BY date_part(month,snp_date), date_part(year,snp_date)
  )) and snp_date <'|| quote_literal(v_current_date)||'-90';

;

$$ language plpgsql;