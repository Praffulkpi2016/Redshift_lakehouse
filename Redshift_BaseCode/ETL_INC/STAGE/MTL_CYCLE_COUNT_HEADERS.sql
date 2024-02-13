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

truncate table bec_ods_stg.MTL_CYCLE_COUNT_HEADERS;

insert into	bec_ods_stg.MTL_CYCLE_COUNT_HEADERS
   (
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
	--ZD_SYNC,
	kca_operation,
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
	kca_seq_id,
	kca_seq_date from bec_raw_dl_ext.MTL_CYCLE_COUNT_HEADERS
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (nvl(CYCLE_COUNT_HEADER_ID,0)  ,kca_seq_id) in 
	(select nvl(CYCLE_COUNT_HEADER_ID,0) as cycle_count_header_id ,max(kca_seq_id) from bec_raw_dl_ext.MTL_CYCLE_COUNT_HEADERS 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by nvl(CYCLE_COUNT_HEADER_ID,0) )
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'mtl_cycle_count_headers')
			 
            )
);
end;