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
	bec_ods.WIP_OPERATIONS
where
	(
	nvl(WIP_ENTITY_ID, 0),
	nvl(OPERATION_SEQ_NUM, 0),
	nvl(REPETITIVE_SCHEDULE_ID, 0)
	) in 
	(
	select
		NVL(stg.WIP_ENTITY_ID, 0) as WIP_ENTITY_ID,
		nvl(stg.OPERATION_SEQ_NUM, 0) as OPERATION_SEQ_NUM,
		nvl(stg.REPETITIVE_SCHEDULE_ID, 0) as REPETITIVE_SCHEDULE_ID
	from
		bec_ods.WIP_OPERATIONS ods,
		bec_ods_stg.WIP_OPERATIONS stg
	where
		    NVL(ods.WIP_ENTITY_ID, 0) = NVL(stg.WIP_ENTITY_ID, 0)
			and nvl(ods.OPERATION_SEQ_NUM, 0) = nvl(stg.OPERATION_SEQ_NUM, 0)
				and
			NVL(ods.REPETITIVE_SCHEDULE_ID, 0) = NVL(stg.REPETITIVE_SCHEDULE_ID, 0)
					and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.WIP_OPERATIONS (
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
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date)
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
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
		,kca_seq_date
	from
		bec_ods_stg.WIP_OPERATIONS
	where
		kca_operation in ('INSERT','UPDATE')
		and (
		nvl(WIP_ENTITY_ID, 0),
		nvl(OPERATION_SEQ_NUM, 0),
		nvl(REPETITIVE_SCHEDULE_ID, 0),
		KCA_SEQ_ID
		) in 
	(
		select
			nvl(WIP_ENTITY_ID, 0) as WIP_ENTITY_ID,
			nvl(OPERATION_SEQ_NUM, 0) as OPERATION_SEQ_NUM,
			nvl(REPETITIVE_SCHEDULE_ID, 0) as REPETITIVE_SCHEDULE_ID,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.WIP_OPERATIONS
		where
			kca_operation in ('INSERT','UPDATE')
		group by
			WIP_ENTITY_ID,
			OPERATION_SEQ_NUM,
			REPETITIVE_SCHEDULE_ID
			)	
	);

commit;

-- Soft delete
update bec_ods.WIP_OPERATIONS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.WIP_OPERATIONS set IS_DELETED_FLG = 'Y'
where (nvl(WIP_ENTITY_ID, 0),nvl(OPERATION_SEQ_NUM, 0),nvl(REPETITIVE_SCHEDULE_ID, 0))  in
(
select nvl(WIP_ENTITY_ID, 0),nvl(OPERATION_SEQ_NUM, 0),nvl(REPETITIVE_SCHEDULE_ID, 0) from bec_raw_dl_ext.WIP_OPERATIONS
where (nvl(WIP_ENTITY_ID, 0),nvl(OPERATION_SEQ_NUM, 0),nvl(REPETITIVE_SCHEDULE_ID, 0),KCA_SEQ_ID)
in 
(
select nvl(WIP_ENTITY_ID, 0),nvl(OPERATION_SEQ_NUM, 0),nvl(REPETITIVE_SCHEDULE_ID, 0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.WIP_OPERATIONS
group by nvl(WIP_ENTITY_ID, 0),nvl(OPERATION_SEQ_NUM, 0),nvl(REPETITIVE_SCHEDULE_ID, 0)
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
	ods_table_name = 'wip_operations';

commit;