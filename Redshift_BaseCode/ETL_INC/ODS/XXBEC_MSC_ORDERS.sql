/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
 
begin;
-- Delete Records

delete
from
	bec_ods.XXBEC_MSC_ORDERS
where
	(nvl(TRANSACTION_ID, 0),
	nvl(INVENTORY_ITEM_ID, 0)) in 
	(
	select
		NVL(stg.TRANSACTION_ID, 0) as TRANSACTION_ID,
		NVL(stg.INVENTORY_ITEM_ID, 0) as INVENTORY_ITEM_ID
	from
		bec_ods.XXBEC_MSC_ORDERS ods,
		bec_ods_stg.XXBEC_MSC_ORDERS stg
	where
		NVL(ods.TRANSACTION_ID, 0) = NVL(stg.TRANSACTION_ID, 0)
			and 
		NVL(ods.INVENTORY_ITEM_ID, 0) = NVL(stg.INVENTORY_ITEM_ID, 0)
				and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.XXBEC_MSC_ORDERS (
	plan_id,
	compile_designator,
	organization_code,
	organization_id,
	item_segments,
	requested_start_date,
	requested_completion_date,
	order_type_text,
	new_due_date,
	ship_set_name,
	arrival_set_name,
	schedule_arrival_date,
	schedule_ship_date,
	actual_start_date,
	planned_arrival_date,
	latest_acceptable_date,
	quantity_rate,
	qty_by_due_date,
	po_line_id,
	shipment_id,
	reschedule_days,
	old_due_date,
	sr_instance_id,
	disposition_id,
	description,
	original_item_name,
	prev_subst_item,
	prev_subst_org,
	original_item_qty,
	last_unit_completion_date,
	line_code,
	expiration_date,
	order_number,
	"action",
	mrp_planning_code_text,
	old_order_quantity,
	orig_org_code,
	dest_org_code,
	dest_org_id,
	dest_inst_id,
	ship_method,
	orig_ship_method,
	source_vendor_name,
	source_vendor_site_code,
	planning_group,
	schedule_group_name,
	quantity,
	ship_date,
	assembly_demand_comp_date,
	planner_code,
	order_type,
	vendor_id,
	vendor_site_id,
	category_id,
	source_vendor_site_id,
	source_vendor_id,
	supplier_name,
	supplier_site_code,
	subinventory_code,
	mrp_planning_code,
	source_organization_id,
	status_code,
	customer_id,
	customer_name,
	ship_to_site_id,
	ship_to_site_name,
	customer_site_id,
	customer_site_name,
	promise_date,
	promise_ship_date,
	original_need_by_date,
	request_date,
	request_ship_date,
	category_set_id,
	sales_order_line_id,
	ship_set_id,
	customer_po_number,
	customer_po_line_number,
	buyer_name,
	source_table,
	transaction_id,
	INVENTORY_ITEM_ID,
	new_order_date,
	new_dock_date,
	new_start_date 
	,kca_operation,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date)
(
	select
	plan_id,
	compile_designator,
	organization_code,
	organization_id,
	item_segments,
	requested_start_date,
	requested_completion_date,
	order_type_text,
	new_due_date,
	ship_set_name,
	arrival_set_name,
	schedule_arrival_date,
	schedule_ship_date,
	actual_start_date,
	planned_arrival_date,
	latest_acceptable_date,
	quantity_rate,
	qty_by_due_date,
	po_line_id,
	shipment_id,
	reschedule_days,
	old_due_date,
	sr_instance_id,
	disposition_id,
	description,
	original_item_name,
	prev_subst_item,
	prev_subst_org,
	original_item_qty,
	last_unit_completion_date,
	line_code,
	expiration_date,
	order_number,
	"action",
	mrp_planning_code_text,
	old_order_quantity,
	orig_org_code,
	dest_org_code,
	dest_org_id,
	dest_inst_id,
	ship_method,
	orig_ship_method,
	source_vendor_name,
	source_vendor_site_code,
	planning_group,
	schedule_group_name,
	quantity,
	ship_date,
	assembly_demand_comp_date,
	planner_code,
	order_type,
	vendor_id,
	vendor_site_id,
	category_id,
	source_vendor_site_id,
	source_vendor_id,
	supplier_name,
	supplier_site_code,
	subinventory_code,
	mrp_planning_code,
	source_organization_id,
	status_code,
	customer_id,
	customer_name,
	ship_to_site_id,
	ship_to_site_name,
	customer_site_id,
	customer_site_name,
	promise_date,
	promise_ship_date,
	original_need_by_date,
	request_date,
	request_ship_date,
	category_set_id,
	sales_order_line_id,
	ship_set_id,
	customer_po_number,
	customer_po_line_number,
	buyer_name,
	source_table,
	transaction_id,
	INVENTORY_ITEM_ID,
	new_order_date,
	new_dock_date,
	new_start_date
		,kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
		,kca_seq_date
	from
		bec_ods_stg.XXBEC_MSC_ORDERS
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		NVL(TRANSACTION_ID, 0),
		NVL(INVENTORY_ITEM_ID, 0),
		KCA_SEQ_ID
		) in 
	(
		select
			NVL(TRANSACTION_ID, 0) as TRANSACTION_ID,
			NVL(INVENTORY_ITEM_ID, 0) as INVENTORY_ITEM_ID,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.XXBEC_MSC_ORDERS
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			NVL(TRANSACTION_ID, 0) ,
			NVL(INVENTORY_ITEM_ID, 0)
			)	
	);

commit;

-- Soft delete
update bec_ods.XXBEC_MSC_ORDERS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.XXBEC_MSC_ORDERS set IS_DELETED_FLG = 'Y'
where (NVL(TRANSACTION_ID, 0),NVL(INVENTORY_ITEM_ID, 0))  in
(
select NVL(TRANSACTION_ID, 0),NVL(INVENTORY_ITEM_ID, 0) from bec_raw_dl_ext.XXBEC_MSC_ORDERS
where (NVL(TRANSACTION_ID, 0),NVL(INVENTORY_ITEM_ID, 0),KCA_SEQ_ID)
in 
(
select NVL(TRANSACTION_ID, 0),NVL(INVENTORY_ITEM_ID, 0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.XXBEC_MSC_ORDERS
group by NVL(TRANSACTION_ID, 0),NVL(INVENTORY_ITEM_ID, 0)
) 
and kca_operation= 'DELETE'
);
commit;
end;

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'xxbec_msc_orders';

commit;