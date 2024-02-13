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

truncate
	table bec_ods_stg.MTL_MATERIAL_TRANSACTIONS;

insert
	into
	bec_ods_stg.MTL_MATERIAL_TRANSACTIONS
(transaction_id,
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
	transaction_type_id,
	transaction_action_id,
	transaction_source_type_id,
	transaction_source_id,
	transaction_source_name,
	transaction_quantity,
	transaction_uom,
	primary_quantity,
	transaction_date,
	variance_amount,
	acct_period_id,
	transaction_reference,
	reason_id,
	distribution_account_id,
	encumbrance_account,
	encumbrance_amount,
	cost_update_id,
	costed_flag,
	invoiced_flag,
	actual_cost,
	transaction_cost,
	prior_cost,
	new_cost,
	currency_code,
	currency_conversion_rate,
	currency_conversion_type,
	currency_conversion_date,
	ussgl_transaction_code,
	quantity_adjusted,
	employee_code,
	department_id,
	operation_seq_num,
	master_schedule_update_code,
	receiving_document,
	picking_line_id,
	trx_source_line_id,
	trx_source_delivery_id,
	repetitive_line_id,
	physical_adjustment_id,
	cycle_count_id,
	rma_line_id,
	transfer_transaction_id,
	transaction_set_id,
	rcv_transaction_id,
	move_transaction_id,
	completion_transaction_id,
	source_code,
	source_line_id,
	vendor_lot_number,
	transfer_organization_id,
	transfer_subinventory,
	transfer_locator_id,
	shipment_number,
	transfer_cost,
	transportation_dist_account,
	transportation_cost,
	transfer_cost_dist_account,
	waybill_airbill,
	freight_code,
	number_of_containers,
	value_change,
	percentage_change,
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
	transaction_group_id,
	task_id,
	to_task_id,
	project_id,
	to_project_id,
	source_project_id,
	pa_expenditure_org_id,
	source_task_id,
	expenditure_type,
	error_code,
	error_explanation,
	prior_costed_quantity,
	final_completion_flag,
	pm_cost_collected,
	pm_cost_collector_group_id,
	shipment_costed,
	transfer_percentage,
	material_account,
	material_overhead_account,
	resource_account,
	outside_processing_account,
	overhead_account,
	cost_group_id,
	transfer_cost_group_id,
	flow_schedule,
	transfer_prior_costed_quantity,
	shortage_process_code,
	qa_collection_id,
	overcompletion_transaction_qty,
	overcompletion_primary_qty,
	overcompletion_transaction_id,
	mvt_stat_status,
	common_bom_seq_id,
	common_routing_seq_id,
	org_cost_group_id,
	cost_type_id,
	periodic_primary_quantity,
	move_order_line_id,
	task_group_id,
	pick_slip_number,
	lpn_id,
	transfer_lpn_id,
	pick_strategy_id,
	pick_rule_id,
	put_away_strategy_id,
	put_away_rule_id,
	content_lpn_id,
	pick_slip_date,
	cost_category_id,
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
	transaction_group_seq,
	ship_to_location_id,
	reservation_id,
	transaction_mode,
	transaction_batch_id,
	transaction_batch_seq,
	intransit_account,
	fob_point,
	parent_transaction_id,
	logical_trx_type_code,
	trx_flow_header_id,
	logical_transactions_created,
	logical_transaction,
	intercompany_cost,
	intercompany_pricing_option,
	intercompany_currency_code,
	original_transaction_temp_id,
	transfer_price,
	expense_account_id,
	cogs_recognition_percent,
	so_issue_account_type,
	opm_costed_flag,
	material_expense_account,
	MCC_CODE,
	TRANSACTION_EXTRACTED,
	XML_DOCUMENT_ID,
	kca_operation,
	kca_seq_id,
	kca_seq_date
)
(
	select
	transaction_id,
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
	transaction_type_id,
	transaction_action_id,
	transaction_source_type_id,
	transaction_source_id,
	transaction_source_name,
	transaction_quantity,
	transaction_uom,
	primary_quantity,
	transaction_date,
	variance_amount,
	acct_period_id,
	transaction_reference,
	reason_id,
	distribution_account_id,
	encumbrance_account,
	encumbrance_amount,
	cost_update_id,
	costed_flag,
	invoiced_flag,
	actual_cost,
	transaction_cost,
	prior_cost,
	new_cost,
	currency_code,
	currency_conversion_rate,
	currency_conversion_type,
	currency_conversion_date,
	ussgl_transaction_code,
	quantity_adjusted,
	employee_code,
	department_id,
	operation_seq_num,
	master_schedule_update_code,
	receiving_document,
	picking_line_id,
	trx_source_line_id,
	trx_source_delivery_id,
	repetitive_line_id,
	physical_adjustment_id,
	cycle_count_id,
	rma_line_id,
	transfer_transaction_id,
	transaction_set_id,
	rcv_transaction_id,
	move_transaction_id,
	completion_transaction_id,
	source_code,
	source_line_id,
	vendor_lot_number,
	transfer_organization_id,
	transfer_subinventory,
	transfer_locator_id,
	shipment_number,
	transfer_cost,
	transportation_dist_account,
	transportation_cost,
	transfer_cost_dist_account,
	waybill_airbill,
	freight_code,
	number_of_containers,
	value_change,
	percentage_change,
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
	transaction_group_id,
	task_id,
	to_task_id,
	project_id,
	to_project_id,
	source_project_id,
	pa_expenditure_org_id,
	source_task_id,
	expenditure_type,
	error_code,
	error_explanation,
	prior_costed_quantity,
	final_completion_flag,
	pm_cost_collected,
	pm_cost_collector_group_id,
	shipment_costed,
	transfer_percentage,
	material_account,
	material_overhead_account,
	resource_account,
	outside_processing_account,
	overhead_account,
	cost_group_id,
	transfer_cost_group_id,
	flow_schedule,
	transfer_prior_costed_quantity,
	shortage_process_code,
	qa_collection_id,
	overcompletion_transaction_qty,
	overcompletion_primary_qty,
	overcompletion_transaction_id,
	mvt_stat_status,
	common_bom_seq_id,
	common_routing_seq_id,
	org_cost_group_id,
	cost_type_id,
	periodic_primary_quantity,
	move_order_line_id,
	task_group_id,
	pick_slip_number,
	lpn_id,
	transfer_lpn_id,
	pick_strategy_id,
	pick_rule_id,
	put_away_strategy_id,
	put_away_rule_id,
	content_lpn_id,
	pick_slip_date,
	cost_category_id,
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
	transaction_group_seq,
	ship_to_location_id,
	reservation_id,
	transaction_mode,
	transaction_batch_id,
	transaction_batch_seq,
	intransit_account,
	fob_point,
	parent_transaction_id,
	logical_trx_type_code,
	trx_flow_header_id,
	logical_transactions_created,
	logical_transaction,
	intercompany_cost,
	intercompany_pricing_option,
	intercompany_currency_code,
	original_transaction_temp_id,
	transfer_price,
	expense_account_id,
	cogs_recognition_percent,
	so_issue_account_type,
	opm_costed_flag,
	material_expense_account,
	MCC_CODE,
	TRANSACTION_EXTRACTED,
	XML_DOCUMENT_ID,
	kca_operation,
	kca_seq_id,
	kca_seq_date
	from
		bec_raw_dl_ext.MTL_MATERIAL_TRANSACTIONS
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
and (TRANSACTION_ID,kca_seq_id) in 
(select TRANSACTION_ID,max(kca_seq_id) from bec_raw_dl_ext.MTL_MATERIAL_TRANSACTIONS 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
group by TRANSACTION_ID)
and 
(kca_seq_date > (
select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name = 'mtl_material_transactions')
)
);		
		
end;