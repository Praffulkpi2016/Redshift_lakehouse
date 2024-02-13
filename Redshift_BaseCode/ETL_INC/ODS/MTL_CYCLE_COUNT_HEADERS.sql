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
	bec_ods.MTL_CYCLE_COUNT_HEADERS
where
	(
	NVL(CYCLE_COUNT_HEADER_ID,0) 

	) in 
	(
	select
		NVL(stg.CYCLE_COUNT_HEADER_ID,0) AS CYCLE_COUNT_HEADER_ID 
	from
		bec_ods.MTL_CYCLE_COUNT_HEADERS ods,
		bec_ods_stg.MTL_CYCLE_COUNT_HEADERS stg
	where
	NVL(ods.CYCLE_COUNT_HEADER_ID,0) = NVL(stg.CYCLE_COUNT_HEADER_ID,0) 
	and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.MTL_CYCLE_COUNT_HEADERS
    (cycle_count_header_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	cycle_count_header_name,
	inventory_adjustment_account,
	orientation_code,
	abc_assignment_group_id,
	onhand_visible_flag,
	days_until_late,
	autoschedule_enabled_flag,
	schedule_interval_time,
	zero_count_flag,
	header_last_schedule_date,
	header_next_schedule_date,
	disable_date,
	approval_option_code,
	automatic_recount_flag,
	next_user_count_sequence,
	unscheduled_count_entry,
	cycle_count_calendar,
	calendar_exception_set,
	approval_tolerance_positive,
	approval_tolerance_negative,
	cost_tolerance_positive,
	cost_tolerance_negative,
	hit_miss_tolerance_positive,
	hit_miss_tolerance_negative,
	abc_initialization_status,
	description,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
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
	maximum_auto_recounts,
	serial_count_option,
	serial_detail_option,
	serial_adjustment_option,
	serial_discrepancy_option,
	container_adjustment_option,
	container_discrepancy_option,
	container_enabled_flag,
	cycle_count_type,
	schedule_empty_locations,
	default_num_counts_per_year,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
	select
	cycle_count_header_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	cycle_count_header_name,
	inventory_adjustment_account,
	orientation_code,
	abc_assignment_group_id,
	onhand_visible_flag,
	days_until_late,
	autoschedule_enabled_flag,
	schedule_interval_time,
	zero_count_flag,
	header_last_schedule_date,
	header_next_schedule_date,
	disable_date,
	approval_option_code,
	automatic_recount_flag,
	next_user_count_sequence,
	unscheduled_count_entry,
	cycle_count_calendar,
	calendar_exception_set,
	approval_tolerance_positive,
	approval_tolerance_negative,
	cost_tolerance_positive,
	cost_tolerance_negative,
	hit_miss_tolerance_positive,
	hit_miss_tolerance_negative,
	abc_initialization_status,
	description,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
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
	maximum_auto_recounts,
	serial_count_option,
	serial_detail_option,
	serial_adjustment_option,
	serial_discrepancy_option,
	container_adjustment_option,
	container_discrepancy_option,
	container_enabled_flag,
	cycle_count_type,
	schedule_empty_locations,
	default_num_counts_per_year,
	kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
	kca_seq_date
	from
		bec_ods_stg.MTL_CYCLE_COUNT_HEADERS
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		NVL(CYCLE_COUNT_HEADER_ID,0), 
		KCA_SEQ_ID
		) in 
	(
		select
			NVL(CYCLE_COUNT_HEADER_ID,0) AS CYCLE_COUNT_HEADER_ID ,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.MTL_CYCLE_COUNT_HEADERS
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			NVL(CYCLE_COUNT_HEADER_ID,0) 
			)	
	);

commit;
 
-- Soft delete
update bec_ods.MTL_CYCLE_COUNT_HEADERS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_CYCLE_COUNT_HEADERS set IS_DELETED_FLG = 'Y'
where (CYCLE_COUNT_HEADER_ID)  in
(
select CYCLE_COUNT_HEADER_ID from bec_raw_dl_ext.MTL_CYCLE_COUNT_HEADERS
where (CYCLE_COUNT_HEADER_ID,KCA_SEQ_ID)
in 
(
select CYCLE_COUNT_HEADER_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_CYCLE_COUNT_HEADERS
group by CYCLE_COUNT_HEADER_ID
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
	ods_table_name = 'mtl_cycle_count_headers';

commit;