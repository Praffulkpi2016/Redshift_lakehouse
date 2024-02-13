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

delete from bec_ods.WIP_DISCRETE_JOBS
where WIP_ENTITY_ID in (
select stg.WIP_ENTITY_ID
from bec_ods.WIP_DISCRETE_JOBS ods, bec_ods_stg.WIP_DISCRETE_JOBS stg
where ods.WIP_ENTITY_ID = stg.WIP_ENTITY_ID
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.WIP_DISCRETE_JOBS
       (wip_entity_id,
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
	,kca_seq_date)	
(
	select
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
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.WIP_DISCRETE_JOBS
	where kca_operation in ('INSERT','UPDATE') 
	and (WIP_ENTITY_ID,kca_seq_id) in 
	(select WIP_ENTITY_ID,max(kca_seq_id) from bec_ods_stg.WIP_DISCRETE_JOBS 
     where kca_operation in ('INSERT','UPDATE')
     group by WIP_ENTITY_ID)
);

commit;



-- Soft delete
update bec_ods.WIP_DISCRETE_JOBS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.WIP_DISCRETE_JOBS set IS_DELETED_FLG = 'Y'
where (WIP_ENTITY_ID)  in
(
select WIP_ENTITY_ID from bec_raw_dl_ext.WIP_DISCRETE_JOBS
where (WIP_ENTITY_ID,KCA_SEQ_ID)
in 
(
select WIP_ENTITY_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.WIP_DISCRETE_JOBS
group by WIP_ENTITY_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'wip_discrete_jobs';

commit;