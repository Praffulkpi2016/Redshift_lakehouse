/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS.
# File Version: KPI v1.0
*/

begin;

DROP TABLE if exists bec_ods.MSC_SUPPLIES;

CREATE TABLE IF NOT EXISTS bec_ods.MSC_SUPPLIES
(
	plan_id NUMERIC(15,0)   ENCODE az64
	,transaction_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,sr_instance_id NUMERIC(15,0)   ENCODE az64
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,schedule_designator_id NUMERIC(15,0)   ENCODE az64
	,revision VARCHAR(10)   ENCODE lzo
	,unit_number VARCHAR(30)   ENCODE lzo
	,new_schedule_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,old_schedule_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,new_wip_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,old_wip_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,first_unit_completion_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_unit_completion_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,first_unit_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_unit_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,disposition_id NUMERIC(15,0)   ENCODE az64
	,disposition_status_type NUMERIC(15,0)   ENCODE az64
	,order_type NUMERIC(15,0)   ENCODE az64
	,supplier_id NUMERIC(15,0)   ENCODE az64
	,supplier_site_id NUMERIC(15,0)   ENCODE az64
	,new_order_quantity NUMERIC(28,10)   ENCODE az64
	,old_order_quantity NUMERIC(28,10)   ENCODE az64
	,new_order_placement_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,old_order_placement_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,reschedule_days NUMERIC(15,0)   ENCODE az64
	,reschedule_flag NUMERIC(15,0)   ENCODE az64
	,schedule_compress_days NUMERIC(15,0)   ENCODE az64
	,new_processing_days NUMERIC(15,0)   ENCODE az64
	,purch_line_num NUMERIC(15,0)   ENCODE az64
	,quantity_in_process NUMERIC(28,10)   ENCODE az64
	,implemented_quantity NUMERIC(28,10)   ENCODE az64
	,firm_planned_type NUMERIC(15,0)   ENCODE az64
	,firm_quantity NUMERIC(28,10)   ENCODE az64
	,firm_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,implement_demand_class VARCHAR(30)   ENCODE lzo
	,implement_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,implement_quantity NUMERIC(28,10)   ENCODE az64
	,implement_firm NUMERIC(15,0)   ENCODE az64
	,implement_wip_class_code VARCHAR(10)   ENCODE lzo
	,implement_job_name VARCHAR(240)   ENCODE lzo
	,implement_dock_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,implement_status_code NUMERIC(15,0)   ENCODE az64
	,implement_employee_id NUMERIC(15,0)   ENCODE az64
	,implement_uom_code VARCHAR(3)   ENCODE lzo
	,implement_location_id NUMERIC(15,0)   ENCODE az64
	,implement_source_org_id NUMERIC(15,0)   ENCODE az64
	,implement_sr_instance_id NUMERIC(15,0)   ENCODE az64
	,implement_supplier_id NUMERIC(15,0)   ENCODE az64
	,implement_supplier_site_id NUMERIC(15,0)   ENCODE az64
	,implement_as NUMERIC(15,0)   ENCODE az64
	,release_status NUMERIC(15,0)   ENCODE az64
	,load_type NUMERIC(15,0)   ENCODE az64
	,process_seq_id NUMERIC(15,0)   ENCODE az64
	,sco_supply_flag NUMERIC(15,0)   ENCODE az64
	,alternate_bom_designator VARCHAR(10)   ENCODE lzo
	,alternate_routing_designator VARCHAR(10)   ENCODE lzo
	,operation_seq_num NUMERIC(15,0)   ENCODE az64
	,by_product_using_assy_id NUMERIC(15,0)   ENCODE az64
	,source_organization_id NUMERIC(15,0)   ENCODE az64
	,source_sr_instance_id NUMERIC(15,0)   ENCODE az64
	,source_supplier_site_id NUMERIC(15,0)   ENCODE az64
	,source_supplier_id NUMERIC(15,0)   ENCODE az64
	,ship_method VARCHAR(30)   ENCODE lzo
	,weight_capacity_used NUMERIC(28,10)   ENCODE az64
	,volume_capacity_used NUMERIC(28,10)   ENCODE az64
	,new_ship_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,new_dock_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,old_dock_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,line_id NUMERIC(15,0)   ENCODE az64
	,project_id NUMERIC(15,0)   ENCODE az64
	,task_id NUMERIC(15,0)   ENCODE az64
	,planning_group VARCHAR(30)   ENCODE lzo
	,implement_project_id NUMERIC(15,0)   ENCODE az64
	,implement_task_id NUMERIC(15,0)   ENCODE az64
	,implement_schedule_group_id NUMERIC(15,0)   ENCODE az64
	,implement_build_sequence NUMERIC(15,00)   ENCODE az64
	,implement_alternate_bom VARCHAR(10)   ENCODE lzo
	,implement_alternate_routing VARCHAR(10)   ENCODE lzo
	,implement_unit_number VARCHAR(30)   ENCODE lzo
	,implement_line_id NUMERIC(15,0)   ENCODE az64
	,release_errors VARCHAR(250)   ENCODE lzo
	,number1 NUMERIC(15,0)   ENCODE az64
	,source_item_id NUMERIC(15,0)   ENCODE az64
	,order_number VARCHAR(240)   ENCODE lzo
	,schedule_group_id NUMERIC(15,0)   ENCODE az64
	,build_sequence NUMERIC(15,0)   ENCODE az64
	,wip_entity_name VARCHAR(240)   ENCODE lzo
	,implement_processing_days NUMERIC(15,0)   ENCODE az64
	,delivery_price NUMERIC(28,10)   ENCODE az64
	,late_supply_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,late_supply_qty NUMERIC(28,10)   ENCODE az64
	,lot_number VARCHAR(80)   ENCODE lzo
	,subinventory_code VARCHAR(10)   ENCODE lzo
	,qty_scrapped NUMERIC(28,10)   ENCODE az64
	,expected_scrap_qty NUMERIC(28,10)   ENCODE az64
	,qty_completed NUMERIC(28,10)   ENCODE az64
	,daily_rate NUMERIC(28,10)   ENCODE az64
	,schedule_group_name VARCHAR(30)   ENCODE lzo
	,updated NUMERIC(15,0)   ENCODE az64
	,subst_item_flag NUMERIC(15,0)   ENCODE az64
	,status NUMERIC(15,0)   ENCODE az64
	,applied NUMERIC(15,0)   ENCODE az64
	,expiration_quantity NUMERIC(28,10)   ENCODE az64
	,expiration_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,non_nettable_qty NUMERIC(28,10)   ENCODE az64
	,implement_wip_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,refresh_number NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,implement_daily_rate NUMERIC(28,10)   ENCODE az64
	,need_by_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,source_supply_id NUMERIC(15,0)   ENCODE az64
	,sr_mtl_supply_id NUMERIC(15,0)   ENCODE az64
	,wip_status_code NUMERIC(15,0)   ENCODE az64
	,demand_class VARCHAR(34)   ENCODE lzo
	,from_organization_id NUMERIC(15,0)   ENCODE az64
	,wip_supply_type NUMERIC(15,0)   ENCODE az64
	,po_line_id NUMERIC(15,0)   ENCODE az64
	,load_factor_rate NUMERIC(28,10)   ENCODE az64
	,routing_sequence_id NUMERIC(15,0)   ENCODE az64
	,bill_sequence_id NUMERIC(15,0)   ENCODE az64
	,coproducts_supply NUMERIC(15,0)   ENCODE az64
	,cfm_routing_flag NUMERIC(15,0)   ENCODE az64
	,customer_id NUMERIC(15,0)   ENCODE az64
	,ship_to_site_id NUMERIC(15,0)   ENCODE az64
	,old_need_by_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,old_daily_rate NUMERIC(28,10)   ENCODE az64
	,old_first_unit_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,old_last_unit_completion_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,old_new_schedule_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,old_qty_completed NUMERIC(28,10)   ENCODE az64
	,old_new_order_quantity NUMERIC(28,10)   ENCODE az64
	,old_firm_quantity NUMERIC(28,10)   ENCODE az64
	,old_firm_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,planning_partner_site_id NUMERIC(15,0)   ENCODE az64
	,planning_tp_type NUMERIC(15,0)   ENCODE az64
	,owning_partner_site_id NUMERIC(15,0)   ENCODE az64
	,owning_tp_type NUMERIC(15,0)   ENCODE az64
	,vmi_flag NUMERIC(15,0)   ENCODE az64
	,earliest_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,earliest_completion_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,min_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,scheduled_demand_id NUMERIC(15,0)   ENCODE az64
	,explosion_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,sco_supply_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,record_source NUMERIC(28,10)   ENCODE az64
	,supply_is_shared NUMERIC(15,0)   ENCODE az64
	,ulpsd TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,ulpcd TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,uepsd TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,uepcd TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eacd TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,original_need_by_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,original_quantity NUMERIC(28,10)   ENCODE az64
	,acceptance_required_flag VARCHAR(1)   ENCODE lzo
	,promised_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,wip_start_quantity NUMERIC(28,10)   ENCODE az64
	,end_order_number VARCHAR(240)   ENCODE lzo
	,end_order_line_number VARCHAR(25)   ENCODE lzo
	,order_line_number VARCHAR(25)   ENCODE lzo
	,quantity_per_assembly NUMERIC(28,10)   ENCODE az64
	,quantity_issued NUMERIC(28,10)   ENCODE az64
	,unbucketed_demand_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,shipment_id NUMERIC(15,0)   ENCODE az64
	,job_op_seq_num NUMERIC(15,0)   ENCODE az64
	,jump_op_seq_num NUMERIC(15,0)   ENCODE az64
	,ship_calendar VARCHAR(15)   ENCODE lzo
	,receiving_calendar VARCHAR(15)   ENCODE lzo
	,intransit_calendar VARCHAR(15)   ENCODE lzo
	,intransit_lead_time NUMERIC(28,10)   ENCODE az64
	,old_ship_date VARCHAR(240)   ENCODE lzo
	,implement_ship_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,orig_ship_method VARCHAR(30)   ENCODE lzo
	,orig_intransit_lead_time NUMERIC(28,10)   ENCODE az64
	,parent_id NUMERIC(15,0)   ENCODE az64
	,days_late NUMERIC(15,0)   ENCODE az64
	,schedule_priority NUMERIC(15,0)   ENCODE az64
	,po_line_location_id NUMERIC(15,0)   ENCODE az64
	,po_distribution_id NUMERIC(15,0)   ENCODE az64
	,wsm_faulty_network NUMERIC(28,10)   ENCODE az64
	,implement_dest_org_id NUMERIC(15,0)   ENCODE az64
	,implement_dest_inst_id NUMERIC(15,0)   ENCODE az64
	,requested_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,requested_completion_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,asset_serial_number VARCHAR(30)   ENCODE lzo
	,asset_item_id NUMERIC(15,0)   ENCODE az64
	,top_transaction_id NUMERIC(15,0)   ENCODE az64
	,unbucketed_new_sched_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,implement_ship_method VARCHAR(30)   ENCODE lzo
	,actual_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,firm_ship_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,schedule_origination_type NUMERIC(15,0)   ENCODE az64
	,sr_customer_acct_id NUMERIC(15,0)   ENCODE az64
	,item_type_id NUMERIC(15,0)   ENCODE az64
	,customer_product_id NUMERIC(15,0)   ENCODE az64
	,ro_status_code VARCHAR(30)   ENCODE lzo
	,sr_repair_group_id NUMERIC(15,0)   ENCODE az64
	,sr_repair_type_id NUMERIC(15,0)   ENCODE az64
	,item_type_value NUMERIC(28,10)   ENCODE az64
	,zone_id NUMERIC(15,0)   ENCODE az64
	,ro_creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,repair_lead_time NUMERIC(28,10)   ENCODE az64
	,firm_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,req_line_id NUMERIC(15,0)   ENCODE az64
	,intransit_owning_org_id NUMERIC(15,0)   ENCODE az64
	,releasable NUMERIC(28,10)   ENCODE az64
	,batch_id NUMERIC(15,0)   ENCODE az64
	,otm_arrival_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,unbucketed_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,ps_supply_flag NUMERIC(15,0)   ENCODE az64
	,ctb_flag VARCHAR(1)   ENCODE lzo
	,ctb_comp_avail_percent NUMERIC(28,10)   ENCODE az64
	,rtb_order_qty_percent NUMERIC(28,10)   ENCODE az64
	,ctb_expected_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,potential_rtb_percent NUMERIC(28,10)   ENCODE az64
	,ctb_priority NUMERIC(15,0)   ENCODE az64
	,description VARCHAR(240)   ENCODE lzo
	,maintenance_object_source NUMERIC(28,10)   ENCODE az64
	,production_schedule_id NUMERIC(15,0)   ENCODE az64
	,visit_id NUMERIC(15,0)   ENCODE az64
	,produces_to_stock NUMERIC(28,10)   ENCODE az64
	,product_classification VARCHAR(150)   ENCODE lzo
	,maintenance_reqt VARCHAR(80)   ENCODE lzo
	,activity_type NUMERIC(15,0)   ENCODE az64
	,activity_name VARCHAR(250)   ENCODE lzo
	,class_code VARCHAR(10)   ENCODE lzo
	,shutdown_type NUMERIC(15,0)   ENCODE az64
	,to_be_exploded NUMERIC(15,0)   ENCODE az64
	,object_type VARCHAR(3)   ENCODE lzo
	,maintenance_type_code VARCHAR(30)   ENCODE lzo
	,activity_item_id NUMERIC(15,0)   ENCODE az64
	,orig_firm_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,orig_firm_quantity NUMERIC(28,10)   ENCODE az64
	,use_wo_substitute NUMERIC(28,10)   ENCODE az64
	,asset_number VARCHAR(30)   ENCODE lzo
	,maintenance_object_id NUMERIC(15,0)   ENCODE az64
	,maintenance_object_type NUMERIC(15,0)   ENCODE az64
	,operating_fleet NUMERIC(28,10)   ENCODE az64
	,maintenance_requirement VARCHAR(22)   ENCODE lzo
	,coll_order_type NUMERIC(15,0)   ENCODE az64
	,ctb_expected_date_buy_comps TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,substitute_components_used VARCHAR(250)   ENCODE lzo
  	,reserved_qty NUMERIC(28,10)   ENCODE az64
	,po_or_req_header_id NUMERIC(15,0)   ENCODE az64
	,actual_lead_time NUMERIC(28,10)   ENCODE az64
	,min_lead_time NUMERIC(28,10)   ENCODE az64
	,pip_item_id NUMERIC(15,0)   ENCODE az64
	,supp_capacity_overload NUMERIC(28,10)   ENCODE az64
	,volume_capacity_overload NUMERIC(28,10)   ENCODE az64
	,weight_capacity_overload NUMERIC(28,10)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MSC_SUPPLIES (
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
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
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
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.MSC_SUPPLIES;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'msc_supplies';
	
Commit;