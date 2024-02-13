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
drop table if exists bec_ods.MTL_MATERIAL_TRANSACTIONS;

CREATE TABLE IF NOT EXISTS bec_ods.MTL_MATERIAL_TRANSACTIONS
(
	transaction_id NUMERIC(15,0)   ENCODE az64
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
	,transaction_type_id NUMERIC(15,0)   ENCODE az64
	,transaction_action_id NUMERIC(15,0)   ENCODE az64
	,transaction_source_type_id NUMERIC(15,0)   ENCODE az64
	,transaction_source_id NUMERIC(15,0)   ENCODE az64
	,transaction_source_name VARCHAR(80)   ENCODE lzo
	,transaction_quantity NUMERIC(28,10)   ENCODE az64
	,transaction_uom VARCHAR(3)   ENCODE lzo
	,primary_quantity NUMERIC(28,10)   ENCODE az64
	,transaction_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,variance_amount NUMERIC(28,10)   ENCODE az64
	,acct_period_id NUMERIC(15,0)   ENCODE az64
	,transaction_reference VARCHAR(240)   ENCODE lzo
	,reason_id NUMERIC(15,0)   ENCODE az64
	,distribution_account_id NUMERIC(15,0)   ENCODE az64
	,encumbrance_account NUMERIC(15,0)   ENCODE az64
	,encumbrance_amount NUMERIC(28,10)   ENCODE az64
	,cost_update_id NUMERIC(15,0)   ENCODE az64
	,costed_flag VARCHAR(1)   ENCODE lzo
	,invoiced_flag VARCHAR(1)   ENCODE lzo
	,actual_cost NUMERIC(28,10)   ENCODE az64
	,transaction_cost NUMERIC(38,10)   ENCODE az64
	,prior_cost NUMERIC(28,10)   ENCODE az64
	,new_cost NUMERIC(28,10)   ENCODE az64
	,currency_code VARCHAR(10)   ENCODE lzo
	,currency_conversion_rate NUMERIC(28,10)   ENCODE az64
	,currency_conversion_type VARCHAR(30)   ENCODE lzo
	,currency_conversion_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,ussgl_transaction_code VARCHAR(30)   ENCODE lzo
	,quantity_adjusted NUMERIC(28,10)   ENCODE az64
	,employee_code VARCHAR(10)   ENCODE lzo
	,department_id NUMERIC(15,0)   ENCODE az64
	,operation_seq_num NUMERIC(15,0)   ENCODE az64
	,master_schedule_update_code VARCHAR(10)   ENCODE lzo
	,receiving_document VARCHAR(10)   ENCODE lzo
	,picking_line_id NUMERIC(15,0)   ENCODE az64
	,trx_source_line_id NUMERIC(15,0)   ENCODE az64
	,trx_source_delivery_id NUMERIC(15,0)   ENCODE az64
	,repetitive_line_id NUMERIC(15,0)   ENCODE az64
	,physical_adjustment_id NUMERIC(15,0)   ENCODE az64
	,cycle_count_id NUMERIC(15,0)   ENCODE az64
	,rma_line_id NUMERIC(15,0)   ENCODE az64
	,transfer_transaction_id NUMERIC(15,0)   ENCODE az64
	,transaction_set_id NUMERIC(15,0)   ENCODE az64
	,rcv_transaction_id NUMERIC(15,0)   ENCODE az64
	,move_transaction_id NUMERIC(15,0)   ENCODE az64
	,completion_transaction_id NUMERIC(15,0)   ENCODE az64
	,source_code VARCHAR(30)   ENCODE lzo
	,source_line_id NUMERIC(15,0)   ENCODE az64
	,vendor_lot_number VARCHAR(30)   ENCODE lzo
	,transfer_organization_id NUMERIC(15,0)   ENCODE az64
	,transfer_subinventory VARCHAR(10)   ENCODE lzo
	,transfer_locator_id NUMERIC(15,0)   ENCODE az64
	,shipment_number VARCHAR(30)   ENCODE lzo
	,transfer_cost NUMERIC(28,10)   ENCODE az64
	,transportation_dist_account NUMERIC(28,10)   ENCODE az64
	,transportation_cost NUMERIC(28,10)   ENCODE az64
	,transfer_cost_dist_account NUMERIC(28,10)   ENCODE az64
	,waybill_airbill VARCHAR(30)   ENCODE lzo
	,freight_code VARCHAR(30)   ENCODE lzo
	,number_of_containers NUMERIC(15,0)   ENCODE az64
	,value_change NUMERIC(15,0)   ENCODE az64
	,percentage_change NUMERIC(15,0)   ENCODE az64
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
	,transaction_group_id NUMERIC(15,0)   ENCODE az64
	,task_id NUMERIC(15,0)   ENCODE az64
	,to_task_id NUMERIC(15,0)   ENCODE az64
	,project_id NUMERIC(15,0)   ENCODE az64
	,to_project_id NUMERIC(15,0)   ENCODE az64
	,source_project_id NUMERIC(15,0)   ENCODE az64
	,pa_expenditure_org_id NUMERIC(15,0)   ENCODE az64
	,source_task_id NUMERIC(15,0)   ENCODE az64
	,expenditure_type VARCHAR(30)   ENCODE lzo
	,error_code VARCHAR(240)   ENCODE lzo
	,error_explanation VARCHAR(240)   ENCODE lzo
	,prior_costed_quantity NUMERIC(28,10)   ENCODE az64
	,final_completion_flag VARCHAR(1)   ENCODE lzo
	,pm_cost_collected VARCHAR(1)   ENCODE lzo
	,pm_cost_collector_group_id NUMERIC(15,0)   ENCODE az64
	,shipment_costed VARCHAR(1)   ENCODE lzo
	,transfer_percentage NUMERIC(28,10)   ENCODE az64
	,material_account NUMERIC(28,10)   ENCODE az64
	,material_overhead_account NUMERIC(28,10)   ENCODE az64
	,resource_account NUMERIC(28,10)   ENCODE az64
	,outside_processing_account NUMERIC(28,10)   ENCODE az64
	,overhead_account NUMERIC(28,10)   ENCODE az64
	,cost_group_id NUMERIC(15,0)   ENCODE az64
	,transfer_cost_group_id NUMERIC(15,0)   ENCODE az64
	,flow_schedule VARCHAR(1)   ENCODE lzo
	,transfer_prior_costed_quantity NUMERIC(28,10)   ENCODE az64
	,shortage_process_code NUMERIC(15,0)   ENCODE az64
	,qa_collection_id NUMERIC(15,0)   ENCODE az64
	,overcompletion_transaction_qty NUMERIC(28,10)   ENCODE az64
	,overcompletion_primary_qty NUMERIC(28,10)   ENCODE az64
	,overcompletion_transaction_id NUMERIC(15,0)   ENCODE az64
	,mvt_stat_status VARCHAR(240)   ENCODE lzo
	,common_bom_seq_id NUMERIC(15,0)   ENCODE az64
	,common_routing_seq_id NUMERIC(15,0)   ENCODE az64
	,org_cost_group_id NUMERIC(15,0)   ENCODE az64
	,cost_type_id NUMERIC(15,0)   ENCODE az64
	,periodic_primary_quantity NUMERIC(28,10)   ENCODE az64
	,move_order_line_id NUMERIC(15,0)   ENCODE az64
	,task_group_id NUMERIC(15,0)   ENCODE az64
	,pick_slip_number NUMERIC(15,0)   ENCODE az64
	,lpn_id NUMERIC(15,0)   ENCODE az64
	,transfer_lpn_id NUMERIC(15,0)   ENCODE az64
	,pick_strategy_id NUMERIC(15,0)   ENCODE az64
	,pick_rule_id NUMERIC(15,0)   ENCODE az64
	,put_away_strategy_id NUMERIC(15,0)   ENCODE az64
	,put_away_rule_id NUMERIC(15,0)   ENCODE az64
	,content_lpn_id NUMERIC(15,0)   ENCODE az64
	,pick_slip_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,cost_category_id NUMERIC(15,0)   ENCODE az64
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
	,secondary_uom_code VARCHAR(3)   ENCODE lzo
	,secondary_transaction_quantity NUMERIC(28,10)   ENCODE az64
	,transaction_group_seq NUMERIC(15,0)   ENCODE az64
	,ship_to_location_id NUMERIC(15,0)   ENCODE az64
	,reservation_id NUMERIC(15,0)   ENCODE az64
	,transaction_mode NUMERIC(15,0)   ENCODE az64
	,transaction_batch_id NUMERIC(15,0)   ENCODE az64
	,transaction_batch_seq NUMERIC(15,0)   ENCODE az64
	,intransit_account NUMERIC(15,0)   ENCODE az64
	,fob_point NUMERIC(15,0)   ENCODE az64
	,parent_transaction_id NUMERIC(15,0)   ENCODE az64
	,logical_trx_type_code NUMERIC(15,0)   ENCODE az64
	,trx_flow_header_id NUMERIC(15,0)   ENCODE az64
	,logical_transactions_created NUMERIC(15,0)   ENCODE az64
	,logical_transaction NUMERIC(15,0)   ENCODE az64
	,intercompany_cost NUMERIC(15,0)   ENCODE az64
	,intercompany_pricing_option NUMERIC(15,0)   ENCODE az64
	,intercompany_currency_code VARCHAR(15)   ENCODE lzo
	,original_transaction_temp_id NUMERIC(15,0)   ENCODE az64
	,transfer_price NUMERIC(28,10)   ENCODE az64
	,expense_account_id NUMERIC(15,0)   ENCODE az64
	,cogs_recognition_percent NUMERIC(15,0)   ENCODE az64
	,so_issue_account_type NUMERIC(15,0)   ENCODE az64
	,opm_costed_flag VARCHAR(1)   ENCODE lzo
	,material_expense_account NUMERIC(15,0)   ENCODE az64
	,MCC_CODE VARCHAR(30)   ENCODE lzo
	,TRANSACTION_EXTRACTED VARCHAR(1)   ENCODE lzo
	,XML_DOCUMENT_ID NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
	,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
 )
DISTSTYLE AUTO
;

insert into bec_ods.MTL_MATERIAL_TRANSACTIONS
(
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
	KCA_OPERATION,
	IS_DELETED_FLG,
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
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
 	kca_seq_date
from bec_ods_stg.MTL_MATERIAL_TRANSACTIONS
);

end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='mtl_material_transactions'; 
commit;