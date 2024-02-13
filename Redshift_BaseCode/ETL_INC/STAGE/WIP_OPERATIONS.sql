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
	table bec_ods_stg.WIP_OPERATIONS;

insert
	into
	bec_ods_stg.WIP_OPERATIONS
    (wip_entity_id,
	operation_seq_num,
	organization_id,
	repetitive_schedule_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	operation_sequence_id,
	standard_operation_id,
	department_id,
	description,
	scheduled_quantity,
	quantity_in_queue,
	quantity_running,
	quantity_waiting_to_move,
	quantity_rejected,
	quantity_scrapped,
	quantity_completed,
	first_unit_start_date,
	first_unit_completion_date,
	last_unit_start_date,
	last_unit_completion_date,
	previous_operation_seq_num,
	next_operation_seq_num,
	count_point_type,
	backflush_flag,
	minimum_transfer_quantity,
	date_last_moved,
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
	wf_itemtype,
	wf_itemkey,
	operation_yield,
	operation_yield_enabled,
	pre_split_quantity,
	operation_completed,
	shutdown_type,
	x_pos,
	y_pos,
	previous_operation_seq_id,
	skip_flag,
	long_description,
	cumulative_scrap_quantity,
	disable_date,
	recommended,
	progress_percentage,
	wsm_op_seq_num,
	wsm_bonus_quantity,
	employee_id,
	actual_start_date,
	actual_completion_date,
	projected_completion_date,
	wsm_update_quantity_txn_id,
	wsm_costed_quantity_completed,
	lowest_acceptable_yield,
	check_skill,
	KCA_OPERATION,
	KCA_SEQ_ID
	,KCA_SEQ_DATE)
(
	select
		wip_entity_id,
		operation_seq_num,
		organization_id,
		repetitive_schedule_id,
		last_update_date,
		last_updated_by,
		creation_date,
		created_by,
		last_update_login,
		request_id,
		program_application_id,
		program_id,
		program_update_date,
		operation_sequence_id,
		standard_operation_id,
		department_id,
		description,
		scheduled_quantity,
		quantity_in_queue,
		quantity_running,
		quantity_waiting_to_move,
		quantity_rejected,
		quantity_scrapped,
		quantity_completed,
		first_unit_start_date,
		first_unit_completion_date,
		last_unit_start_date,
		last_unit_completion_date,
		previous_operation_seq_num,
		next_operation_seq_num,
		count_point_type,
		backflush_flag,
		minimum_transfer_quantity,
		date_last_moved,
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
		wf_itemtype,
		wf_itemkey,
		operation_yield,
		operation_yield_enabled,
		pre_split_quantity,
		operation_completed,
		shutdown_type,
		x_pos,
		y_pos,
		previous_operation_seq_id,
		skip_flag,
		long_description,
		cumulative_scrap_quantity,
		disable_date,
		recommended,
		progress_percentage,
		wsm_op_seq_num,
		wsm_bonus_quantity,
		employee_id,
		actual_start_date,
		actual_completion_date,
		projected_completion_date,
		wsm_update_quantity_txn_id,
		wsm_costed_quantity_completed,
		lowest_acceptable_yield,
		check_skill,
		KCA_OPERATION,
		KCA_SEQ_ID
		,KCA_SEQ_DATE
	from
		bec_raw_dl_ext.WIP_OPERATIONS
	where
		kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
		and (nvl(WIP_ENTITY_ID, 0),
		nvl(OPERATION_SEQ_NUM, 0),
		nvl(REPETITIVE_SCHEDULE_ID, 0),
		KCA_SEQ_ID) in 
	(
		select
			nvl(WIP_ENTITY_ID, 0) as WIP_ENTITY_ID,
			nvl(OPERATION_SEQ_NUM, 0) as OPERATION_SEQ_NUM,
			nvl(REPETITIVE_SCHEDULE_ID, 0) as REPETITIVE_SCHEDULE_ID,
			max(KCA_SEQ_ID)
		from
			bec_raw_dl_ext.WIP_OPERATIONS
		where
			kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
		group by
			WIP_ENTITY_ID,
			OPERATION_SEQ_NUM,
			REPETITIVE_SCHEDULE_ID)
		and (KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'wip_operations')

            )
		);
end;