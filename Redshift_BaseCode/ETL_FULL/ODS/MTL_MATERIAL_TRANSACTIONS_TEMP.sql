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

DROP TABLE if exists bec_ods.MTL_MATERIAL_TRANSACTIONS_TEMP;

CREATE TABLE IF NOT EXISTS bec_ods.MTL_MATERIAL_TRANSACTIONS_TEMP
(

    transaction_header_id NUMERIC(15,0)   ENCODE az64
	,transaction_temp_id NUMERIC(15,0)   ENCODE az64
	,source_code VARCHAR(30)   ENCODE lzo
	,source_line_id NUMERIC(15,0)   ENCODE az64
	,transaction_mode NUMERIC(15,0)   ENCODE az64
	,lock_flag VARCHAR(1)   ENCODE lzo
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,revision VARCHAR(3)   ENCODE lzo
	,organization_id NUMERIC(15,0)   ENCODE az64
	,subinventory_code VARCHAR(10)   ENCODE lzo
	,locator_id NUMERIC(15,0)   ENCODE az64
	,transaction_quantity NUMERIC(28,10)   ENCODE az64
	,primary_quantity NUMERIC(28,10)   ENCODE az64
	,transaction_uom VARCHAR(3)   ENCODE lzo
	,transaction_cost NUMERIC(28,10)   ENCODE az64
	,transaction_type_id NUMERIC(15,0)   ENCODE az64
	,transaction_action_id NUMERIC(15,0)   ENCODE az64
	,transaction_source_type_id NUMERIC(15,0)   ENCODE az64
	,transaction_source_id NUMERIC(15,0)   ENCODE az64
	,transaction_source_name VARCHAR(80)   ENCODE lzo
	,transaction_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,acct_period_id NUMERIC(15,0)   ENCODE az64
	,distribution_account_id NUMERIC(15,0)   ENCODE az64
	,transaction_reference VARCHAR(240)   ENCODE lzo
	,requisition_line_id NUMERIC(15,0)   ENCODE az64
	,requisition_distribution_id NUMERIC(15,0)   ENCODE az64
	,reason_id NUMERIC(15,0)   ENCODE az64
	,lot_number VARCHAR(80)   ENCODE lzo
	,lot_expiration_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,serial_number VARCHAR(30)   ENCODE lzo
	,receiving_document VARCHAR(10)   ENCODE lzo
	,demand_id NUMERIC(15,0)   ENCODE az64
	,rcv_transaction_id NUMERIC(15,0)   ENCODE az64
	,move_transaction_id NUMERIC(15,0)   ENCODE az64
	,completion_transaction_id NUMERIC(15,0)   ENCODE az64
	,wip_entity_type NUMERIC(15,0)   ENCODE az64
	,schedule_id NUMERIC(15,0)   ENCODE az64
	,repetitive_line_id NUMERIC(15,0)   ENCODE az64
	,employee_code VARCHAR(10)   ENCODE lzo
	,primary_switch NUMERIC(15,0)   ENCODE az64
	,schedule_update_code NUMERIC(15,0)   ENCODE az64
	,setup_teardown_code NUMERIC(15,0)   ENCODE az64
	,item_ordering NUMERIC(28,10)   ENCODE az64
	,negative_req_flag NUMERIC(28,10)   ENCODE az64
	,operation_seq_num NUMERIC(15,0)   ENCODE az64
	,picking_line_id NUMERIC(15,0)   ENCODE az64
	,trx_source_line_id NUMERIC(15,0)   ENCODE az64
	,trx_source_delivery_id NUMERIC(15,0)   ENCODE az64
	,physical_adjustment_id NUMERIC(15,0)   ENCODE az64
	,cycle_count_id NUMERIC(15,0)   ENCODE az64
	,rma_line_id NUMERIC(15,0)   ENCODE az64
	,customer_ship_id NUMERIC(15,0)   ENCODE az64
	,currency_code VARCHAR(10)   ENCODE lzo
	,currency_conversion_rate NUMERIC(28,10)   ENCODE az64
	,currency_conversion_type VARCHAR(30)   ENCODE lzo
	,currency_conversion_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,ussgl_transaction_code VARCHAR(30)   ENCODE lzo
	,vendor_lot_number VARCHAR(30)   ENCODE lzo
	,encumbrance_account NUMERIC(28,10)   ENCODE az64
	,encumbrance_amount NUMERIC(28,10)   ENCODE az64
	,ship_to_location NUMERIC(15,0)   ENCODE az64
	,shipment_number VARCHAR(30)   ENCODE lzo
	,transfer_cost NUMERIC(28,10)   ENCODE az64
	,transportation_cost NUMERIC(28,10)   ENCODE az64
	,transportation_account NUMERIC(15,0)   ENCODE az64
	,freight_code VARCHAR(30)   ENCODE lzo
	,containers NUMERIC(15,0)   ENCODE az64
	,waybill_airbill VARCHAR(30)   ENCODE lzo
	,expected_arrival_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,transfer_subinventory VARCHAR(10)   ENCODE lzo
	,transfer_organization NUMERIC(15,0)   ENCODE az64
	,transfer_to_location NUMERIC(15,0)   ENCODE az64
	,new_average_cost NUMERIC(28,10)   ENCODE az64
	,value_change NUMERIC(28,10)   ENCODE az64
	,percentage_change NUMERIC(28,10)   ENCODE az64
	,material_allocation_temp_id NUMERIC(15,0)   ENCODE az64
	,demand_source_header_id NUMERIC(15,0)   ENCODE az64
	,demand_source_line VARCHAR(30)   ENCODE lzo
	,demand_source_delivery VARCHAR(30)   ENCODE lzo
	,item_segments VARCHAR(240)   ENCODE lzo
	,item_description VARCHAR(240)   ENCODE lzo
	,item_trx_enabled_flag VARCHAR(1)   ENCODE lzo
	,item_location_control_code NUMERIC(15,0)   ENCODE az64
	,item_restrict_subinv_code NUMERIC(15,0)   ENCODE az64
	,item_restrict_locators_code NUMERIC(15,0)   ENCODE az64
	,item_revision_qty_control_code NUMERIC(15,0)   ENCODE az64
	,item_primary_uom_code VARCHAR(3)   ENCODE lzo
	,item_uom_class VARCHAR(10)   ENCODE lzo
	,item_shelf_life_code NUMERIC(15,0)   ENCODE az64
	,item_shelf_life_days NUMERIC(15,0)   ENCODE az64
	,item_lot_control_code NUMERIC(15,0)   ENCODE az64
	,item_serial_control_code NUMERIC(15,0)   ENCODE az64
	,item_inventory_asset_flag VARCHAR(1)   ENCODE lzo
	,allowed_units_lookup_code NUMERIC(15,0)   ENCODE az64
	,department_id NUMERIC(15,0)   ENCODE az64
	,department_code VARCHAR(10)   ENCODE lzo
	,wip_supply_type NUMERIC(15,0)   ENCODE az64
	,supply_subinventory VARCHAR(10)   ENCODE lzo
	,supply_locator_id NUMERIC(15,0)   ENCODE az64
	,valid_subinventory_flag VARCHAR(1)   ENCODE lzo
	,valid_locator_flag VARCHAR(1)   ENCODE lzo
	,locator_segments VARCHAR(240)   ENCODE lzo
	,current_locator_control_code NUMERIC(15,0)   ENCODE az64
	,number_of_lots_entered NUMERIC(15,0)   ENCODE az64
	,wip_commit_flag VARCHAR(1)   ENCODE lzo
	,next_lot_number VARCHAR(30)   ENCODE lzo
	,lot_alpha_prefix VARCHAR(30)   ENCODE lzo
	,next_serial_number VARCHAR(30)   ENCODE lzo
	,serial_alpha_prefix VARCHAR(30)   ENCODE lzo
	,shippable_flag VARCHAR(1)   ENCODE lzo
	,posting_flag VARCHAR(1)   ENCODE lzo
	,required_flag VARCHAR(1)   ENCODE lzo
	,process_flag VARCHAR(1)   ENCODE lzo
	,error_code VARCHAR(240)   ENCODE lzo
	,error_explanation VARCHAR(240)   ENCODE lzo
	,attribute_category VARCHAR(30)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,attribute6 VARCHAR(150)   ENCODE lzo
	,attribute7 VARCHAR(150)   ENCODE lzo
	,attribute8 VARCHAR(150)   ENCODE lzo
	,attribute9 VARCHAR(150)   ENCODE lzo
	,attribute10 VARCHAR(150)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,movement_id NUMERIC(15,0)   ENCODE az64
	,reservation_quantity NUMERIC(28,10)   ENCODE az64
	,shipped_quantity NUMERIC(28,10)   ENCODE az64
	,transaction_line_number NUMERIC(15,0)   ENCODE az64
	,task_id NUMERIC(15,0)   ENCODE az64
	,to_task_id NUMERIC(15,0)   ENCODE az64
	,source_task_id NUMERIC(15,0)    ENCODE az64
	,project_id NUMERIC(15,0)   ENCODE az64
	,source_project_id NUMERIC(15,0)    ENCODE az64
	,pa_expenditure_org_id NUMERIC(15,0)    ENCODE az64
	,to_project_id NUMERIC(15,0)   ENCODE az64
	,expenditure_type VARCHAR(30)   ENCODE lzo
	,final_completion_flag VARCHAR(1)   ENCODE lzo
	,transfer_percentage NUMERIC(28,10)   ENCODE az64
	,transaction_sequence_id NUMERIC(15,0)    ENCODE az64
	,material_account NUMERIC(28,10)   ENCODE az64
	,material_overhead_account NUMERIC(28,10)   ENCODE az64
	,resource_account NUMERIC(28,10)   ENCODE az64
	,outside_processing_account NUMERIC(28,10)   ENCODE az64
	,overhead_account NUMERIC(28,10)   ENCODE az64
	,flow_schedule VARCHAR(1)   ENCODE lzo
	,cost_group_id NUMERIC(15,0)   ENCODE az64
	,demand_class VARCHAR(30)   ENCODE lzo
	,qa_collection_id NUMERIC(15,0)   ENCODE az64
	,kanban_card_id NUMERIC(15,0)   ENCODE az64
	,overcompletion_transaction_qty NUMERIC(28,10)   ENCODE az64
	,overcompletion_primary_qty NUMERIC(28,10)   ENCODE az64
	,overcompletion_transaction_id NUMERIC(15,0)   ENCODE az64
	,end_item_unit_number VARCHAR(60)   ENCODE lzo
	,scheduled_payback_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,line_type_code NUMERIC(15,0)   ENCODE az64
	,parent_transaction_temp_id NUMERIC(15,0)   ENCODE az64
	,put_away_strategy_id NUMERIC(15,0)   ENCODE az64
	,put_away_rule_id NUMERIC(15,0)   ENCODE az64
	,pick_strategy_id NUMERIC(15,0)   ENCODE az64
	,pick_rule_id NUMERIC(15,0)   ENCODE az64
	,move_order_line_id NUMERIC(15,0)  ENCODE az64
	,task_group_id NUMERIC(15,0)   ENCODE az64
	,pick_slip_number NUMERIC(15,0)   ENCODE az64
	,reservation_id NUMERIC(15,0)   ENCODE az64
	,common_bom_seq_id NUMERIC(15,0)   ENCODE az64
	,common_routing_seq_id NUMERIC(15,0)   ENCODE az64
	,org_cost_group_id NUMERIC(15,0)   ENCODE az64
	,cost_type_id NUMERIC(15,0)   ENCODE az64
	,transaction_status NUMERIC(15,0)    ENCODE az64
	,standard_operation_id NUMERIC(15,0)    ENCODE az64
	,task_priority NUMERIC(15,0)   ENCODE az64
	,wms_task_type NUMERIC(15,0)   ENCODE az64
	,parent_line_id NUMERIC(15,0)   ENCODE az64
	,source_lot_number VARCHAR(80)   ENCODE lzo
	,transfer_cost_group_id NUMERIC(15,0)   ENCODE az64
	,lpn_id NUMERIC(15,0)   ENCODE az64
	,transfer_lpn_id NUMERIC(15,0)   ENCODE az64
	,wms_task_status NUMERIC(15,0)   ENCODE az64
	,content_lpn_id NUMERIC(15,0)   ENCODE az64
	,container_item_id NUMERIC(15,0)   ENCODE az64
	,cartonization_id NUMERIC(15,0)   ENCODE az64
	,pick_slip_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,rebuild_item_id NUMERIC(15,0)   ENCODE az64
	,rebuild_serial_number VARCHAR(30)   ENCODE lzo
	,rebuild_activity_id NUMERIC(15,0)   ENCODE az64
	,rebuild_job_name VARCHAR(240)   ENCODE lzo
	,organization_type NUMERIC(15,0)   ENCODE az64
	,transfer_organization_type NUMERIC(15,0)   ENCODE az64
	,owning_organization_id NUMERIC(15,0)   ENCODE az64
	,owning_tp_type NUMERIC(15,0)   ENCODE az64
	,xfr_owning_organization_id NUMERIC(15,0)   ENCODE az64
	,transfer_owning_tp_type NUMERIC(15,0)   ENCODE az64
	,planning_organization_id NUMERIC(15,0)   ENCODE az64
	,planning_tp_type NUMERIC(15,0)   ENCODE az64
	,xfr_planning_organization_id NUMERIC(15,0)   ENCODE az64
	,transfer_planning_tp_type NUMERIC(15,0)   ENCODE az64
	,secondary_uom_code VARCHAR(240)   ENCODE lzo
	,secondary_transaction_quantity NUMERIC(28,10)   ENCODE az64
	,allocated_lpn_id NUMERIC(15,0)   ENCODE az64
	,schedule_number VARCHAR(60)   ENCODE lzo
	,scheduled_flag NUMERIC(15,0)   ENCODE az64
	,class_code VARCHAR(10)   ENCODE lzo
	,schedule_group NUMERIC(15,0)   ENCODE az64
	,build_sequence NUMERIC(15,0)   ENCODE az64
	,bom_revision VARCHAR(3)   ENCODE lzo
	,routing_revision VARCHAR(3)   ENCODE lzo
	,bom_revision_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,routing_revision_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,alternate_bom_designator VARCHAR(10)   ENCODE lzo
	,alternate_routing_designator VARCHAR(10)   ENCODE lzo
	,transaction_batch_id NUMERIC(15,0)   ENCODE az64
	,transaction_batch_seq NUMERIC(15,0)   ENCODE az64
	,operation_plan_id NUMERIC(15,0)   ENCODE az64
	,intransit_account NUMERIC(15,0)   ENCODE az64
	,fob_point NUMERIC(28,10)   ENCODE az64
	,move_order_header_id NUMERIC(15,0)   ENCODE az64
	,serial_allocated_flag VARCHAR(1)   ENCODE lzo
	,trx_flow_header_id NUMERIC(15,0)   ENCODE az64
	,logical_trx_type_code NUMERIC(15,0)   ENCODE az64
	,original_transaction_temp_id NUMERIC(15,0)   ENCODE az64
	,transfer_secondary_quantity NUMERIC(28,10)   ENCODE az64
	,transfer_secondary_uom VARCHAR(3)   ENCODE lzo
	,relieve_reservations_flag VARCHAR(1)   ENCODE lzo
	,relieve_high_level_rsv_flag VARCHAR(1)   ENCODE lzo
	,transfer_price NUMERIC(28,10)   ENCODE az64
	,material_expense_account NUMERIC(28,10)   ENCODE az64
	,fulfillment_base VARCHAR(1)   ENCODE lzo
	,nested_parent_line_id NUMERIC(15,10)   ENCODE az64 
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MTL_MATERIAL_TRANSACTIONS_TEMP (
  transaction_header_id,
	transaction_temp_id,
	source_code,
	source_line_id,
	transaction_mode,
	lock_flag,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	inventory_item_id,
	revision,
	organization_id,
	subinventory_code,
	locator_id,
	transaction_quantity,
	primary_quantity,
	transaction_uom,
	transaction_cost,
	transaction_type_id,
	transaction_action_id,
	transaction_source_type_id,
	transaction_source_id,
	transaction_source_name,
	transaction_date,
	acct_period_id,
	distribution_account_id,
	transaction_reference,
	requisition_line_id,
	requisition_distribution_id,
	reason_id,
	lot_number,
	lot_expiration_date,
	serial_number,
	receiving_document,
	demand_id,
	rcv_transaction_id,
	move_transaction_id,
	completion_transaction_id,
	wip_entity_type,
	schedule_id,
	repetitive_line_id,
	employee_code,
	primary_switch,
	schedule_update_code,
	setup_teardown_code,
	item_ordering,
	negative_req_flag,
	operation_seq_num,
	picking_line_id,
	trx_source_line_id,
	trx_source_delivery_id,
	physical_adjustment_id,
	cycle_count_id,
	rma_line_id,
	customer_ship_id,
	currency_code,
	currency_conversion_rate,
	currency_conversion_type,
	currency_conversion_date,
	ussgl_transaction_code,
	vendor_lot_number,
	encumbrance_account,
	encumbrance_amount,
	ship_to_location,
	shipment_number,
	transfer_cost,
	transportation_cost,
	transportation_account,
	freight_code,
	containers,
	waybill_airbill,
	expected_arrival_date,
	transfer_subinventory,
	transfer_organization,
	transfer_to_location,
	new_average_cost,
	value_change,
	percentage_change,
	material_allocation_temp_id,
	demand_source_header_id,
	demand_source_line,
	demand_source_delivery,
	item_segments,
	item_description,
	item_trx_enabled_flag,
	item_location_control_code,
	item_restrict_subinv_code,
	item_restrict_locators_code,
	item_revision_qty_control_code,
	item_primary_uom_code,
	item_uom_class,
	item_shelf_life_code,
	item_shelf_life_days,
	item_lot_control_code,
	item_serial_control_code,
	item_inventory_asset_flag,
	allowed_units_lookup_code,
	department_id,
	department_code,
	wip_supply_type,
	supply_subinventory,
	supply_locator_id,
	valid_subinventory_flag,
	valid_locator_flag,
	locator_segments,
	current_locator_control_code,
	number_of_lots_entered,
	wip_commit_flag,
	next_lot_number,
	lot_alpha_prefix,
	next_serial_number,
	serial_alpha_prefix,
	shippable_flag,
	posting_flag,
	required_flag,
	process_flag,
	error_code,
	error_explanation,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	movement_id,
	reservation_quantity,
	shipped_quantity,
	transaction_line_number,
	task_id,
	to_task_id,
	source_task_id,
	project_id,
	source_project_id,
	pa_expenditure_org_id,
	to_project_id,
	expenditure_type,
	final_completion_flag,
	transfer_percentage,
	transaction_sequence_id,
	material_account,
	material_overhead_account,
	resource_account,
	outside_processing_account,
	overhead_account,
	flow_schedule,
	cost_group_id,
	demand_class,
	qa_collection_id,
	kanban_card_id,
	overcompletion_transaction_qty,
	overcompletion_primary_qty,
	overcompletion_transaction_id,
	end_item_unit_number,
	scheduled_payback_date,
	line_type_code,
	parent_transaction_temp_id,
	put_away_strategy_id,
	put_away_rule_id,
	pick_strategy_id,
	pick_rule_id,
	move_order_line_id,
	task_group_id,
	pick_slip_number,
	reservation_id,
	common_bom_seq_id,
	common_routing_seq_id,
	org_cost_group_id,
	cost_type_id,
	transaction_status,
	standard_operation_id,
	task_priority,
	wms_task_type,
	parent_line_id,
	source_lot_number,
	transfer_cost_group_id,
	lpn_id,
	transfer_lpn_id,
	wms_task_status,
	content_lpn_id,
	container_item_id,
	cartonization_id,
	pick_slip_date,
	rebuild_item_id,
	rebuild_serial_number,
	rebuild_activity_id,
	rebuild_job_name,
	organization_type,
	transfer_organization_type,
	owning_organization_id,
	owning_tp_type,
	xfr_owning_organization_id,
	transfer_owning_tp_type,
	planning_organization_id,
	planning_tp_type,
	xfr_planning_organization_id,
	transfer_planning_tp_type,
	secondary_uom_code,
	secondary_transaction_quantity,
	allocated_lpn_id,
	schedule_number,
	scheduled_flag,
	class_code,
	schedule_group,
	build_sequence,
	bom_revision,
	routing_revision,
	bom_revision_date,
	routing_revision_date,
	alternate_bom_designator,
	alternate_routing_designator,
	transaction_batch_id,
	transaction_batch_seq,
	operation_plan_id,
	intransit_account,
	fob_point,
	move_order_header_id,
	serial_allocated_flag,
	trx_flow_header_id,
	logical_trx_type_code,
	original_transaction_temp_id,
	transfer_secondary_quantity,
	transfer_secondary_uom,
	relieve_reservations_flag,
	relieve_high_level_rsv_flag,
	transfer_price,
	material_expense_account,
	fulfillment_base,
	nested_parent_line_id,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
    SELECT
   transaction_header_id,
	transaction_temp_id,
	source_code,
	source_line_id,
	transaction_mode,
	lock_flag,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	inventory_item_id,
	revision,
	organization_id,
	subinventory_code,
	locator_id,
	transaction_quantity,
	primary_quantity,
	transaction_uom,
	transaction_cost,
	transaction_type_id,
	transaction_action_id,
	transaction_source_type_id,
	transaction_source_id,
	transaction_source_name,
	transaction_date,
	acct_period_id,
	distribution_account_id,
	transaction_reference,
	requisition_line_id,
	requisition_distribution_id,
	reason_id,
	lot_number,
	lot_expiration_date,
	serial_number,
	receiving_document,
	demand_id,
	rcv_transaction_id,
	move_transaction_id,
	completion_transaction_id,
	wip_entity_type,
	schedule_id,
	repetitive_line_id,
	employee_code,
	primary_switch,
	schedule_update_code,
	setup_teardown_code,
	item_ordering,
	negative_req_flag,
	operation_seq_num,
	picking_line_id,
	trx_source_line_id,
	trx_source_delivery_id,
	physical_adjustment_id,
	cycle_count_id,
	rma_line_id,
	customer_ship_id,
	currency_code,
	currency_conversion_rate,
	currency_conversion_type,
	currency_conversion_date,
	ussgl_transaction_code,
	vendor_lot_number,
	encumbrance_account,
	encumbrance_amount,
	ship_to_location,
	shipment_number,
	transfer_cost,
	transportation_cost,
	transportation_account,
	freight_code,
	containers,
	waybill_airbill,
	expected_arrival_date,
	transfer_subinventory,
	transfer_organization,
	transfer_to_location,
	new_average_cost,
	value_change,
	percentage_change,
	material_allocation_temp_id,
	demand_source_header_id,
	demand_source_line,
	demand_source_delivery,
	item_segments,
	item_description,
	item_trx_enabled_flag,
	item_location_control_code,
	item_restrict_subinv_code,
	item_restrict_locators_code,
	item_revision_qty_control_code,
	item_primary_uom_code,
	item_uom_class,
	item_shelf_life_code,
	item_shelf_life_days,
	item_lot_control_code,
	item_serial_control_code,
	item_inventory_asset_flag,
	allowed_units_lookup_code,
	department_id,
	department_code,
	wip_supply_type,
	supply_subinventory,
	supply_locator_id,
	valid_subinventory_flag,
	valid_locator_flag,
	locator_segments,
	current_locator_control_code,
	number_of_lots_entered,
	wip_commit_flag,
	next_lot_number,
	lot_alpha_prefix,
	next_serial_number,
	serial_alpha_prefix,
	shippable_flag,
	posting_flag,
	required_flag,
	process_flag,
	error_code,
	error_explanation,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	movement_id,
	reservation_quantity,
	shipped_quantity,
	transaction_line_number,
	task_id,
	to_task_id,
	source_task_id,
	project_id,
	source_project_id,
	pa_expenditure_org_id,
	to_project_id,
	expenditure_type,
	final_completion_flag,
	transfer_percentage,
	transaction_sequence_id,
	material_account,
	material_overhead_account,
	resource_account,
	outside_processing_account,
	overhead_account,
	flow_schedule,
	cost_group_id,
	demand_class,
	qa_collection_id,
	kanban_card_id,
	overcompletion_transaction_qty,
	overcompletion_primary_qty,
	overcompletion_transaction_id,
	end_item_unit_number,
	scheduled_payback_date,
	line_type_code,
	parent_transaction_temp_id,
	put_away_strategy_id,
	put_away_rule_id,
	pick_strategy_id,
	pick_rule_id,
	move_order_line_id,
	task_group_id,
	pick_slip_number,
	reservation_id,
	common_bom_seq_id,
	common_routing_seq_id,
	org_cost_group_id,
	cost_type_id,
	transaction_status,
	standard_operation_id,
	task_priority,
	wms_task_type,
	parent_line_id,
	source_lot_number,
	transfer_cost_group_id,
	lpn_id,
	transfer_lpn_id,
	wms_task_status,
	content_lpn_id,
	container_item_id,
	cartonization_id,
	pick_slip_date,
	rebuild_item_id,
	rebuild_serial_number,
	rebuild_activity_id,
	rebuild_job_name,
	organization_type,
	transfer_organization_type,
	owning_organization_id,
	owning_tp_type,
	xfr_owning_organization_id,
	transfer_owning_tp_type,
	planning_organization_id,
	planning_tp_type,
	xfr_planning_organization_id,
	transfer_planning_tp_type,
	secondary_uom_code,
	secondary_transaction_quantity,
	allocated_lpn_id,
	schedule_number,
	scheduled_flag,
	class_code,
	schedule_group,
	build_sequence,
	bom_revision,
	routing_revision,
	bom_revision_date,
	routing_revision_date,
	alternate_bom_designator,
	alternate_routing_designator,
	transaction_batch_id,
	transaction_batch_seq,
	operation_plan_id,
	intransit_account,
	fob_point,
	move_order_header_id,
	serial_allocated_flag,
	trx_flow_header_id,
	logical_trx_type_code,
	original_transaction_temp_id,
	transfer_secondary_quantity,
	transfer_secondary_uom,
	relieve_reservations_flag,
	relieve_high_level_rsv_flag,
	transfer_price,
	material_expense_account,
	fulfillment_base,
	nested_parent_line_id,
    KCA_OPERATION,
    'N' as IS_DELETED_FLG,
    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.MTL_MATERIAL_TRANSACTIONS_TEMP;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'mtl_material_transactions_temp';
	
COMMIT;