/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Facts.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh.FACT_RAPA_PO_TMP2;

create table bec_dwh.FACT_RAPA_PO_TMP2 
	diststyle all
	sortkey (promised_date1)
as 
(
select
	a.inventory_item_id,
	a.organization_id,
	a.cost_type,
	a.plan_name,
	a.data_type,
	a.order_group,
	a.order_type_text,
	a.po_number,
	a.promised_date1,
	a.aging_period,
	a.promised_date,
	a.purchase_item,
	a.item_description,
	a.planning_make_buy_code,
	diics.item_category_segment1||'.'||diics.item_category_segment2 as category_name,
 	a.po_open_qty,
	a.primary_quantity::Numeric(38,10),
	a.unit_price,
	a.primary_unit_of_measure,
	a.po_open_amount::Numeric(38,10),
	a.po_line_type,
	a.vendor_name,
	a.mtl_cost,
	a.ext_mtl_cost::Numeric(38,10),
	a.VARIANCE::Numeric(38,10),
	a.source_organization_id,
	a.planner_code,
	a.buyer_name,
 	a.transactional_uom_code,
	a.release_num,
	a.inventory_item_id_KEY,
	a.organization_id_KEY,
	a.source_organization_id_KEY,
	a.is_deleted_flg,
	a.source_app_id,
	a.dw_load_id,
	a.dw_insert_date,
	a.dw_update_date
from bec_dwh.FACT_RAPA_STG3 a
Left Outer Join bec_Dwh.dim_inv_item_category_set diics
ON a.inventory_item_id = diics.inventory_item_id
and a.organization_id = diics.organization_id
AND diics.category_set_id = 1
UNION ALL
select 
    inventory_item_id,
	organization_id,
	cost_type, 
	plan_name, 
	data_type, 
	order_group, 
	order_type_text, 
	order_number, 
	promised_date, 
	aging_period, 
	receipt_date, 
	part_number, 
	item_description, 
	planning_make_buy_code, 
	category_name,
	rcv_quantity_received, 
	primary_quantity::Numeric(38,10), 
	po_unit_price, 
	primary_unit_of_measure, 
	extended_po_rcv_price::Numeric(38,10), 
	po_line_type, 
	vendor_name, 
	mtl_cost,
	ext_mtl_cost::Numeric(38,10),
	variance::Numeric(38,10),
	source_organization_id, 
	planner_code, 
	buyer_name, 
	po_unit_of_measure, 
	po_release_number,
	inventory_item_id_KEY,
	organization_id_KEY,
	source_organization_id_KEY,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
from bec_dwh.FACT_RAPA_STG4
UNION ALL
select
	inventory_item_id,
	organization_id,
	cost_type,
	plan_name,
	data_type,
	order_group,
	order_type_text,
	po_number,
	consumption_date1,
	aging_period,
	consumption_date,
	item,
	description,
	planning_make_buy_code,
	category_name,
	consumption_quantity,
	primary_quantity,
	unit_price,
	uom_code,
	consumption_value,
	po_line_type,
	vendor_name,
	material_cost,
	extended_cost,
	consigned_ppv,
	source_organization_id,
	planner_code,
	buyer_name,
	primary_unit_of_measure,
	release_num,
	inventory_item_id_KEY,
	organization_id_KEY,
	source_organization_id_KEY,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
from bec_dwh.FACT_RAPA_STG5
);

commit;

end;

update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_rapa_po_tmp2' 
  and batch_name = 'ascp';
  
commit;
