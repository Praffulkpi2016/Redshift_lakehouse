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

DROP TABLE if exists bec_ods.WIP_DISCRETE_JOBS;

CREATE TABLE IF NOT EXISTS bec_ods.WIP_DISCRETE_JOBS
(
	wip_entity_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,source_line_id NUMERIC(15,0)   ENCODE az64
	,source_code VARCHAR(30)   ENCODE lzo
	,description VARCHAR(240)   ENCODE lzo
	,status_type NUMERIC(15,0)   ENCODE az64
	,primary_item_id NUMERIC(15,0)   ENCODE az64
	,firm_planned_flag NUMERIC(15,0)   ENCODE az64
	,job_type NUMERIC(15,0)   ENCODE az64
	,wip_supply_type NUMERIC(15,0)   ENCODE az64
	,class_code VARCHAR(10)   ENCODE lzo
	,material_account NUMERIC(15,0)   ENCODE az64
	,material_overhead_account NUMERIC(15,0)   ENCODE az64
	,resource_account NUMERIC(15,0)   ENCODE az64
	,outside_processing_account NUMERIC(15,0)   ENCODE az64
	,material_variance_account NUMERIC(15,0)   ENCODE az64
	,resource_variance_account NUMERIC(15,0)   ENCODE az64
	,outside_proc_variance_account NUMERIC(15,0)   ENCODE az64
	,std_cost_adjustment_account NUMERIC(15,0)   ENCODE az64
	,overhead_account NUMERIC(15,0)   ENCODE az64
	,overhead_variance_account NUMERIC(15,0)   ENCODE az64
	,scheduled_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,date_released TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,scheduled_completion_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,date_completed TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,date_closed TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,start_quantity NUMERIC(28,10)   ENCODE az64
	,quantity_completed NUMERIC(28,10)   ENCODE az64
	,quantity_scrapped NUMERIC(28,10)   ENCODE az64
	,net_quantity NUMERIC(28,10)   ENCODE az64
	,bom_reference_id NUMERIC(15,0)   ENCODE az64
	,routing_reference_id NUMERIC(15,0)   ENCODE az64
	,common_bom_sequence_id NUMERIC(15,0)   ENCODE az64
	,common_routing_sequence_id NUMERIC(15,0)   ENCODE az64
	,bom_revision VARCHAR(3)   ENCODE lzo
	,routing_revision VARCHAR(3)   ENCODE lzo
	,bom_revision_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,routing_revision_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,lot_number VARCHAR(80)   ENCODE lzo
	,alternate_bom_designator VARCHAR(10)   ENCODE lzo
	,alternate_routing_designator VARCHAR(10)   ENCODE lzo
	,completion_subinventory VARCHAR(10)   ENCODE lzo
	,completion_locator_id NUMERIC(15,0)   ENCODE az64
	,mps_scheduled_completion_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,mps_net_quantity NUMERIC(28,10)   ENCODE az64
	,demand_class VARCHAR(30)   ENCODE lzo
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
	,schedule_group_id NUMERIC(15,0)   ENCODE az64
	,build_sequence NUMERIC(15,0)   ENCODE az64
	,line_id NUMERIC(15,0)   ENCODE az64
	,project_id NUMERIC(15,0)   ENCODE az64
	,task_id NUMERIC(15,0)   ENCODE az64
	,kanban_card_id NUMERIC(15,0)   ENCODE az64
	,overcompletion_tolerance_type NUMERIC(15,0)   ENCODE az64
	,overcompletion_tolerance_value NUMERIC(28,10)   ENCODE az64
	,end_item_unit_number VARCHAR(30)   ENCODE lzo
	,po_creation_time NUMERIC(28,10)   ENCODE az64
	,priority NUMERIC(15,0)   ENCODE az64
	,due_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,est_scrap_account NUMERIC(28,10)   ENCODE az64
	,est_scrap_var_account NUMERIC(28,10)   ENCODE az64
	,est_scrap_prior_qty NUMERIC(28,10)   ENCODE az64
	,due_date_penalty NUMERIC(15,0)   ENCODE az64
	,due_date_tolerance NUMERIC(15,0)   ENCODE az64
	,coproducts_supply NUMERIC(28,10)   ENCODE az64
	,parent_wip_entity_id NUMERIC(15,0)   ENCODE az64
	,asset_number VARCHAR(30)   ENCODE lzo
	,asset_group_id NUMERIC(15,0)   ENCODE az64
	,rebuild_item_id NUMERIC(15,0)   ENCODE az64
	,rebuild_serial_number VARCHAR(30)   ENCODE lzo
	,manual_rebuild_flag VARCHAR(1)   ENCODE lzo
	,shutdown_type VARCHAR(30)   ENCODE lzo
	,estimation_status NUMERIC(15,0)   ENCODE az64
	,requested_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,notification_required VARCHAR(1)   ENCODE lzo
	,work_order_type VARCHAR(30)   ENCODE lzo
	,owning_department NUMERIC(15,0)   ENCODE az64
	,activity_type VARCHAR(30)   ENCODE lzo
	,activity_cause VARCHAR(30)   ENCODE lzo
	,tagout_required VARCHAR(1)   ENCODE lzo
	,plan_maintenance VARCHAR(1)   ENCODE lzo
	,pm_schedule_id NUMERIC(15,0)   ENCODE az64
	,last_estimation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_estimation_req_id NUMERIC(15,0)   ENCODE az64
	,activity_source VARCHAR(30)   ENCODE lzo
	,serialization_start_op NUMERIC(15,0)   ENCODE az64
	,maintenance_object_id NUMERIC(15,0)   ENCODE az64
	,maintenance_object_type NUMERIC(28,10)   ENCODE az64
	,maintenance_object_source NUMERIC(28,10)   ENCODE az64
	,material_issue_by_mo VARCHAR(1)   ENCODE lzo
	,scheduling_request_id NUMERIC(15,0)   ENCODE az64
	,issue_zero_cost_flag VARCHAR(1)   ENCODE lzo
	,eam_linear_location_id NUMERIC(15,0)   ENCODE az64
	,actual_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,expected_hold_release_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,expedited VARCHAR(1)   ENCODE lzo
	,job_note VARCHAR(4000)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.WIP_DISCRETE_JOBS (
   wip_entity_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	source_line_id,
	source_code,
	description,
	status_type,
	primary_item_id,
	firm_planned_flag,
	job_type,
	wip_supply_type,
	class_code,
	material_account,
	material_overhead_account,
	resource_account,
	outside_processing_account,
	material_variance_account,
	resource_variance_account,
	outside_proc_variance_account,
	std_cost_adjustment_account,
	overhead_account,
	overhead_variance_account,
	scheduled_start_date,
	date_released,
	scheduled_completion_date,
	date_completed,
	date_closed,
	start_quantity,
	quantity_completed,
	quantity_scrapped,
	net_quantity,
	bom_reference_id,
	routing_reference_id,
	common_bom_sequence_id,
	common_routing_sequence_id,
	bom_revision,
	routing_revision,
	bom_revision_date,
	routing_revision_date,
	lot_number,
	alternate_bom_designator,
	alternate_routing_designator,
	completion_subinventory,
	completion_locator_id,
	mps_scheduled_completion_date,
	mps_net_quantity,
	demand_class,
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
	schedule_group_id,
	build_sequence,
	line_id,
	project_id,
	task_id,
	kanban_card_id,
	overcompletion_tolerance_type,
	overcompletion_tolerance_value,
	end_item_unit_number,
	po_creation_time,
	priority,
	due_date,
	est_scrap_account,
	est_scrap_var_account,
	est_scrap_prior_qty,
	due_date_penalty,
	due_date_tolerance,
	coproducts_supply,
	parent_wip_entity_id,
	asset_number,
	asset_group_id,
	rebuild_item_id,
	rebuild_serial_number,
	manual_rebuild_flag,
	shutdown_type,
	estimation_status,
	requested_start_date,
	notification_required,
	work_order_type,
	owning_department,
	activity_type,
	activity_cause,
	tagout_required,
	plan_maintenance,
	pm_schedule_id,
	last_estimation_date,
	last_estimation_req_id,
	activity_source,
	serialization_start_op,
	maintenance_object_id,
	maintenance_object_type,
	maintenance_object_source,
	material_issue_by_mo,
	scheduling_request_id,
	issue_zero_cost_flag,
	eam_linear_location_id,
	actual_start_date,
	expected_hold_release_date,
	expedited,
	job_note,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
    SELECT
        wip_entity_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	source_line_id,
	source_code,
	description,
	status_type,
	primary_item_id,
	firm_planned_flag,
	job_type,
	wip_supply_type,
	class_code,
	material_account,
	material_overhead_account,
	resource_account,
	outside_processing_account,
	material_variance_account,
	resource_variance_account,
	outside_proc_variance_account,
	std_cost_adjustment_account,
	overhead_account,
	overhead_variance_account,
	scheduled_start_date,
	date_released,
	scheduled_completion_date,
	date_completed,
	date_closed,
	start_quantity,
	quantity_completed,
	quantity_scrapped,
	net_quantity,
	bom_reference_id,
	routing_reference_id,
	common_bom_sequence_id,
	common_routing_sequence_id,
	bom_revision,
	routing_revision,
	bom_revision_date,
	routing_revision_date,
	lot_number,
	alternate_bom_designator,
	alternate_routing_designator,
	completion_subinventory,
	completion_locator_id,
	mps_scheduled_completion_date,
	mps_net_quantity,
	demand_class,
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
	schedule_group_id,
	build_sequence,
	line_id,
	project_id,
	task_id,
	kanban_card_id,
	overcompletion_tolerance_type,
	overcompletion_tolerance_value,
	end_item_unit_number,
	po_creation_time,
	priority,
	due_date,
	est_scrap_account,
	est_scrap_var_account,
	est_scrap_prior_qty,
	due_date_penalty,
	due_date_tolerance,
	coproducts_supply,
	parent_wip_entity_id,
	asset_number,
	asset_group_id,
	rebuild_item_id,
	rebuild_serial_number,
	manual_rebuild_flag,
	shutdown_type,
	estimation_status,
	requested_start_date,
	notification_required,
	work_order_type,
	owning_department,
	activity_type,
	activity_cause,
	tagout_required,
	plan_maintenance,
	pm_schedule_id,
	last_estimation_date,
	last_estimation_req_id,
	activity_source,
	serialization_start_op,
	maintenance_object_id,
	maintenance_object_type,
	maintenance_object_source,
	material_issue_by_mo,
	scheduling_request_id,
	issue_zero_cost_flag,
	eam_linear_location_id,
	actual_start_date,
	expected_hold_release_date,
	expedited,
	job_note,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.WIP_DISCRETE_JOBS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'wip_discrete_jobs';
	
COMMIT;