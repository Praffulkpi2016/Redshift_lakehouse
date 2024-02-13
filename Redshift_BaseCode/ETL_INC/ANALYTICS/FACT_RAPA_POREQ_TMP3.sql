/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Facts.
# File Version: KPI v1.0
*/
begin;

Truncate table bec_dwh.FACT_RAPA_POREQ_TMP3;

Insert Into bec_dwh.FACT_RAPA_POREQ_TMP3 
(
select
	inventory_item_id,
    organization_id,
	cost_type,
	plan_name,
	data_type,
	order_group,
	order_type_text,
	requisition_number,
	need_by_date,
	aging_period,
	dock_promised_date,
	part_number,
	description,
	planning_make_buy_code,
	category_name,
	so_ship_qty,
	primary_quantity,
	unit_price,
	primary_unit_of_measure,
	extended_cost::Numeric(38,10),
	po_line_type,
	vendor_name,
	std_cost,
	ext_std_cost::Numeric(38,10),
	variance,
	source_organization_id,
	planner_code,
	buyer_name,
	transactional_uom_code,
	release_num,
	inventory_item_id_KEY,
	organization_id_KEY,
	source_organization_id_KEY,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
from bec_dwh.FACT_RAPA_STG7
UNION ALL
select 
    inventory_item_id,
	organization_id,
	cost_type,
	plan_name,
	data_type,
	order_group,
	order_type_text,
	requisition_number,
	need_by_date,
	aging_period,
	dock_promised_date,
	part_number,
	description,
	planning_make_buy_code,
	category_name,
	pr_open_qty,
	Primary_quantity,
	unit_price,
	primary_unit_of_measure,
	extended_cost::Numeric(38,10),
	po_line_type,
	vendor_name,
	std_cost,
	ext_std_cost::Numeric(38,10),
	variance::Numeric(38,10),
	source_organization_id,
	planner_code,
	buyer_name,
	transactional_uom_code,
	release_num, 
	inventory_item_id_KEY,
	organization_id_KEY,
	source_organization_id_KEY,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
from bec_dwh.FACT_RAPA_STG8
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
	promised_date1,
	aging_period,
	promised_date,
	purchase_item,
	item_description,
	planning_make_buy_code,
	category_name,
	po_open_qty,
	primary_quantity,
	unit_price,
	primary_unit_of_measure,
	po_open_amount::Numeric(38,10),
	po_line_type,
	suggested_vendor_name,
	material_cost,
	ext_mtl_cost::Numeric(38,10),
	VARIANCE::Numeric(38,10),
	source_organization_id,
	planner_code,
	buyer_name,
	transactional_uom_code,
	release_num, 
	inventory_item_id_KEY,
	organization_id_KEY,
	source_organization_id_KEY,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
from bec_dwh.FACT_RAPA_STG9
);

commit;

end;

update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_rapa_poreq_tmp3' 
  and batch_name = 'ascp';
  
commit;
