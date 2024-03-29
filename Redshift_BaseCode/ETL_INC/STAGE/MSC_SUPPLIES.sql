/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
begin;

truncate table bec_ods_stg.MSC_SUPPLIES;

insert into	bec_ods_stg.MSC_SUPPLIES
   (	plan_id,
	transaction_id,
	organization_id,
	sr_instance_id,
	inventory_item_id,
	schedule_designator_id,
	revision,
	unit_number,
	new_schedule_date,
	old_schedule_date,
	new_wip_start_date,
	old_wip_start_date,
	first_unit_completion_date,
	last_unit_completion_date,
	first_unit_start_date,
	last_unit_start_date,
	disposition_id,
	disposition_status_type,
	order_type,
	supplier_id,
	supplier_site_id,
	new_order_quantity,
	old_order_quantity,
	new_order_placement_date,
	old_order_placement_date,
	reschedule_days,
	reschedule_flag,
	schedule_compress_days,
	new_processing_days,
	purch_line_num,
	quantity_in_process,
	implemented_quantity,
	firm_planned_type,
	firm_quantity,
	firm_date,
	implement_demand_class,
	implement_date,
	implement_quantity,
	implement_firm,
	implement_wip_class_code,
	implement_job_name,
	implement_dock_date,
	implement_status_code,
	implement_employee_id,
	implement_uom_code,
	implement_location_id,
	implement_source_org_id,
	implement_sr_instance_id,
	implement_supplier_id,
	implement_supplier_site_id,
	implement_as,
	release_status,
	load_type,
	process_seq_id,
	sco_supply_flag,
	alternate_bom_designator,
	alternate_routing_designator,
	operation_seq_num,
	by_product_using_assy_id,
	source_organization_id,
	source_sr_instance_id,
	source_supplier_site_id,
	source_supplier_id,
	ship_method,
	weight_capacity_used,
	volume_capacity_used,
	new_ship_date,
	new_dock_date,
	old_dock_date,
	line_id,
	project_id,
	task_id,
	planning_group,
	implement_project_id,
	implement_task_id,
	implement_schedule_group_id,
	implement_build_sequence,
	implement_alternate_bom,
	implement_alternate_routing,
	implement_unit_number,
	implement_line_id,
	release_errors,
	number1,
	source_item_id,
	order_number,
	schedule_group_id,
	build_sequence,
	wip_entity_name,
	implement_processing_days,
	delivery_price,
	late_supply_date,
	late_supply_qty,
	lot_number,
	subinventory_code,
	qty_scrapped,
	expected_scrap_qty,
	qty_completed,
	daily_rate,
	schedule_group_name,
	updated,
	subst_item_flag,
	status,
	applied,
	expiration_quantity,
	expiration_date,
	non_nettable_qty,
	implement_wip_start_date,
	refresh_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	implement_daily_rate,
	need_by_date,
	source_supply_id,
	sr_mtl_supply_id,
	wip_status_code,
	demand_class,
	from_organization_id,
	wip_supply_type,
	po_line_id,
	load_factor_rate,
	routing_sequence_id,
	bill_sequence_id,
	coproducts_supply,
	cfm_routing_flag,
	customer_id,
	ship_to_site_id,
	old_need_by_date,
	old_daily_rate,
	old_first_unit_start_date,
	old_last_unit_completion_date,
	old_new_schedule_date,
	old_qty_completed,
	old_new_order_quantity,
	old_firm_quantity,
	old_firm_date,
	planning_partner_site_id,
	planning_tp_type,
	owning_partner_site_id,
	owning_tp_type,
	vmi_flag,
	earliest_start_date,
	earliest_completion_date,
	min_start_date,
	scheduled_demand_id,
	explosion_date,
	sco_supply_date,
	record_source,
	supply_is_shared,
	ulpsd,
	ulpcd,
	uepsd,
	uepcd,
	eacd,
	original_need_by_date,
	original_quantity,
	acceptance_required_flag,
	promised_date,
	wip_start_quantity,
	end_order_number,
	end_order_line_number,
	order_line_number,
	quantity_per_assembly,
	quantity_issued,
	unbucketed_demand_date,
	shipment_id,
	job_op_seq_num,
	jump_op_seq_num,
	ship_calendar,
	receiving_calendar,
	intransit_calendar,
	intransit_lead_time,
	old_ship_date,
	implement_ship_date,
	orig_ship_method,
	orig_intransit_lead_time,
	parent_id,
	days_late,
	schedule_priority,
	po_line_location_id,
	po_distribution_id,
	wsm_faulty_network,
	implement_dest_org_id,
	implement_dest_inst_id,
	requested_start_date,
	requested_completion_date,
	asset_serial_number,
	asset_item_id,
	top_transaction_id,
	unbucketed_new_sched_date,
	implement_ship_method,
	actual_start_date,
	firm_ship_date,
	schedule_origination_type,
	sr_customer_acct_id,
	item_type_id,
	customer_product_id,
	ro_status_code,
	sr_repair_group_id,
	sr_repair_type_id,
	item_type_value,
	zone_id,
	ro_creation_date,
	repair_lead_time,
	firm_start_date,
	req_line_id,
	intransit_owning_org_id,
	releasable,
	batch_id,
	otm_arrival_date,
	unbucketed_start_date,
	ps_supply_flag,
	ctb_flag,
	ctb_comp_avail_percent,
	rtb_order_qty_percent,
	ctb_expected_date,
	potential_rtb_percent,
	ctb_priority,
	description,
	maintenance_object_source,
	production_schedule_id,
	visit_id,
	produces_to_stock,
	product_classification,
	maintenance_reqt,
	activity_type,
	activity_name,
	class_code,
	shutdown_type,
	to_be_exploded,
	object_type,
	maintenance_type_code,
	activity_item_id,
	orig_firm_date,
	orig_firm_quantity,
	use_wo_substitute,
	asset_number,
	maintenance_object_id,
	maintenance_object_type,
	operating_fleet,
	maintenance_requirement,
	coll_order_type,
	ctb_expected_date_buy_comps,
	substitute_components_used, 
	reserved_qty,
	po_or_req_header_id,
	actual_lead_time,
	min_lead_time,
	pip_item_id,
	supp_capacity_overload,
	volume_capacity_overload,
	weight_capacity_overload,
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		plan_id,
	transaction_id,
	organization_id,
	sr_instance_id,
	inventory_item_id,
	schedule_designator_id,
	revision,
	unit_number,
	new_schedule_date,
	old_schedule_date,
	new_wip_start_date,
	old_wip_start_date,
	first_unit_completion_date,
	last_unit_completion_date,
	first_unit_start_date,
	last_unit_start_date,
	disposition_id,
	disposition_status_type,
	order_type,
	supplier_id,
	supplier_site_id,
	new_order_quantity,
	old_order_quantity,
	new_order_placement_date,
	old_order_placement_date,
	reschedule_days,
	reschedule_flag,
	schedule_compress_days,
	new_processing_days,
	purch_line_num,
	quantity_in_process,
	implemented_quantity,
	firm_planned_type,
	firm_quantity,
	firm_date,
	implement_demand_class,
	implement_date,
	implement_quantity,
	implement_firm,
	implement_wip_class_code,
	implement_job_name,
	implement_dock_date,
	implement_status_code,
	implement_employee_id,
	implement_uom_code,
	implement_location_id,
	implement_source_org_id,
	implement_sr_instance_id,
	implement_supplier_id,
	implement_supplier_site_id,
	implement_as,
	release_status,
	load_type,
	process_seq_id,
	sco_supply_flag,
	alternate_bom_designator,
	alternate_routing_designator,
	operation_seq_num,
	by_product_using_assy_id,
	source_organization_id,
	source_sr_instance_id,
	source_supplier_site_id,
	source_supplier_id,
	ship_method,
	weight_capacity_used,
	volume_capacity_used,
	new_ship_date,
	new_dock_date,
	old_dock_date,
	line_id,
	project_id,
	task_id,
	planning_group,
	implement_project_id,
	implement_task_id,
	implement_schedule_group_id,
	implement_build_sequence,
	implement_alternate_bom,
	implement_alternate_routing,
	implement_unit_number,
	implement_line_id,
	release_errors,
	number1,
	source_item_id,
	order_number,
	schedule_group_id,
	build_sequence,
	wip_entity_name,
	implement_processing_days,
	delivery_price,
	late_supply_date,
	late_supply_qty,
	lot_number,
	subinventory_code,
	qty_scrapped,
	expected_scrap_qty,
	qty_completed,
	daily_rate,
	schedule_group_name,
	updated,
	subst_item_flag,
	status,
	applied,
	expiration_quantity,
	expiration_date,
	non_nettable_qty,
	implement_wip_start_date,
	refresh_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	implement_daily_rate,
	need_by_date,
	source_supply_id,
	sr_mtl_supply_id,
	wip_status_code,
	demand_class,
	from_organization_id,
	wip_supply_type,
	po_line_id,
	load_factor_rate,
	routing_sequence_id,
	bill_sequence_id,
	coproducts_supply,
	cfm_routing_flag,
	customer_id,
	ship_to_site_id,
	old_need_by_date,
	old_daily_rate,
	old_first_unit_start_date,
	old_last_unit_completion_date,
	old_new_schedule_date,
	old_qty_completed,
	old_new_order_quantity,
	old_firm_quantity,
	old_firm_date,
	planning_partner_site_id,
	planning_tp_type,
	owning_partner_site_id,
	owning_tp_type,
	vmi_flag,
	earliest_start_date,
	earliest_completion_date,
	min_start_date,
	scheduled_demand_id,
	explosion_date,
	sco_supply_date,
	record_source,
	supply_is_shared,
	ulpsd,
	ulpcd,
	uepsd,
	uepcd,
	eacd,
	original_need_by_date,
	original_quantity,
	acceptance_required_flag,
	promised_date,
	wip_start_quantity,
	end_order_number,
	end_order_line_number,
	order_line_number,
	quantity_per_assembly,
	quantity_issued,
	unbucketed_demand_date,
	shipment_id,
	job_op_seq_num,
	jump_op_seq_num,
	ship_calendar,
	receiving_calendar,
	intransit_calendar,
	intransit_lead_time,
	old_ship_date,
	implement_ship_date,
	orig_ship_method,
	orig_intransit_lead_time,
	parent_id,
	days_late,
	schedule_priority,
	po_line_location_id,
	po_distribution_id,
	wsm_faulty_network,
	implement_dest_org_id,
	implement_dest_inst_id,
	requested_start_date,
	requested_completion_date,
	asset_serial_number,
	asset_item_id,
	top_transaction_id,
	unbucketed_new_sched_date,
	implement_ship_method,
	actual_start_date,
	firm_ship_date,
	schedule_origination_type,
	sr_customer_acct_id,
	item_type_id,
	customer_product_id,
	ro_status_code,
	sr_repair_group_id,
	sr_repair_type_id,
	item_type_value,
	zone_id,
	ro_creation_date,
	repair_lead_time,
	firm_start_date,
	req_line_id,
	intransit_owning_org_id,
	releasable,
	batch_id,
	otm_arrival_date,
	unbucketed_start_date,
	ps_supply_flag,
	ctb_flag,
	ctb_comp_avail_percent,
	rtb_order_qty_percent,
	ctb_expected_date,
	potential_rtb_percent,
	ctb_priority,
	description,
	maintenance_object_source,
	production_schedule_id,
	visit_id,
	produces_to_stock,
	product_classification,
	maintenance_reqt,
	activity_type,
	activity_name,
	class_code,
	shutdown_type,
	to_be_exploded,
	object_type,
	maintenance_type_code,
	activity_item_id,
	orig_firm_date,
	orig_firm_quantity,
	use_wo_substitute,
	asset_number,
	maintenance_object_id,
	maintenance_object_type,
	operating_fleet,
	maintenance_requirement,
	coll_order_type,
	ctb_expected_date_buy_comps,
	substitute_components_used, 
	reserved_qty,
	po_or_req_header_id,
	actual_lead_time,
	min_lead_time,
	pip_item_id,
	supp_capacity_overload,
	volume_capacity_overload,
	weight_capacity_overload,
        KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from bec_raw_dl_ext.MSC_SUPPLIES
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (nvl(PLAN_ID,0),nvl(SR_INSTANCE_ID, 0 ),nvl(TRANSACTION_ID, 0 ),kca_seq_id) in 
	(select nvl(PLAN_ID,0) as PLAN_ID,nvl(SR_INSTANCE_ID, 0 ) as SR_INSTANCE_ID,nvl(TRANSACTION_ID, 0 ) as TRANSACTION_ID,max(kca_seq_id) from bec_raw_dl_ext.MSC_SUPPLIES 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by nvl(PLAN_ID,0),nvl(SR_INSTANCE_ID, 0 ),nvl(TRANSACTION_ID, 0 ))
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'msc_supplies')
);
end;
