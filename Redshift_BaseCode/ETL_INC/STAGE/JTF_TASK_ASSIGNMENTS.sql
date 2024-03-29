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
	table bec_ods_stg.JTF_TASK_ASSIGNMENTS;

COMMIT;

insert
	into
	bec_ods_stg.JTF_TASK_ASSIGNMENTS
(	
	TASK_ASSIGNMENT_ID, 
	CREATED_BY, 
	CREATION_DATE, 
	LAST_UPDATED_BY, 
	LAST_UPDATE_DATE, 
	LAST_UPDATE_LOGIN, 
	OBJECT_VERSION_NUMBER, 
	TASK_ID, 
	RESOURCE_TYPE_CODE, 
	RESOURCE_ID, 
	ASSIGNMENT_STATUS_ID, 
	ACTUAL_EFFORT, 
	RESOURCE_TERRITORY_ID, 
	ACTUAL_EFFORT_UOM, 
	SCHEDULE_FLAG, 
	ALARM_TYPE_CODE, 
	ALARM_CONTACT, 
	SHIFT_CONSTRUCT_ID, 
	SCHED_TRAVEL_DISTANCE, 
	SCHED_TRAVEL_DURATION, 
	SCHED_TRAVEL_DURATION_UOM, 
	ACTUAL_TRAVEL_DISTANCE, 
	ACTUAL_TRAVEL_DURATION, 
	ACTUAL_TRAVEL_DURATION_UOM, 
	ACTUAL_START_DATE, 
	ACTUAL_END_DATE, 
	PALM_FLAG, 
	WINCE_FLAG, 
	LAPTOP_FLAG, 
	DEVICE1_FLAG, 
	DEVICE2_FLAG, 
	DEVICE3_FLAG, 
	ATTRIBUTE1, 
	ATTRIBUTE2, 
	ATTRIBUTE3, 
	ATTRIBUTE4, 
	ATTRIBUTE5, 
	ATTRIBUTE6, 
	ATTRIBUTE7, 
	ATTRIBUTE8, 
	ATTRIBUTE9, 
	ATTRIBUTE10, 
	ATTRIBUTE11, 
	ATTRIBUTE12, 
	ATTRIBUTE13, 
	ATTRIBUTE14, 
	ATTRIBUTE15, 
	ATTRIBUTE_CATEGORY, 
	SECURITY_GROUP_ID, 
	ORIG_SYSTEM_REFERENCE, 
	ORIG_SYSTEM_REFERENCE_ID, 
	UPDATE_STATUS_FLAG, 
	ASSIGNEE_ROLE, 
	SHOW_ON_CALENDAR, 
	CATEGORY_ID, 
	REMINDER_WF_ITEM_TYPE, 
	REMINDER_WF_ITEM_KEY, 
	FREE_BUSY_TYPE, 
	BOOKING_START_DATE, 
	BOOKING_END_DATE, 
	OBJECT_CAPACITY_ID,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
)
(
	select
		TASK_ASSIGNMENT_ID, 
		CREATED_BY, 
		CREATION_DATE, 
		LAST_UPDATED_BY, 
		LAST_UPDATE_DATE, 
		LAST_UPDATE_LOGIN, 
		OBJECT_VERSION_NUMBER, 
		TASK_ID, 
		RESOURCE_TYPE_CODE, 
		RESOURCE_ID, 
		ASSIGNMENT_STATUS_ID, 
		ACTUAL_EFFORT, 
		RESOURCE_TERRITORY_ID, 
		ACTUAL_EFFORT_UOM, 
		SCHEDULE_FLAG, 
		ALARM_TYPE_CODE, 
		ALARM_CONTACT, 
		SHIFT_CONSTRUCT_ID, 
		SCHED_TRAVEL_DISTANCE, 
		SCHED_TRAVEL_DURATION, 
		SCHED_TRAVEL_DURATION_UOM, 
		ACTUAL_TRAVEL_DISTANCE, 
		ACTUAL_TRAVEL_DURATION, 
		ACTUAL_TRAVEL_DURATION_UOM, 
		ACTUAL_START_DATE, 
		ACTUAL_END_DATE, 
		PALM_FLAG, 
		WINCE_FLAG, 
		LAPTOP_FLAG, 
		DEVICE1_FLAG, 
		DEVICE2_FLAG, 
		DEVICE3_FLAG, 
		ATTRIBUTE1, 
		ATTRIBUTE2, 
		ATTRIBUTE3, 
		ATTRIBUTE4, 
		ATTRIBUTE5, 
		ATTRIBUTE6, 
		ATTRIBUTE7, 
		ATTRIBUTE8, 
		ATTRIBUTE9, 
		ATTRIBUTE10, 
		ATTRIBUTE11, 
		ATTRIBUTE12, 
		ATTRIBUTE13, 
		ATTRIBUTE14, 
		ATTRIBUTE15, 
		ATTRIBUTE_CATEGORY, 
		SECURITY_GROUP_ID, 
		ORIG_SYSTEM_REFERENCE, 
		ORIG_SYSTEM_REFERENCE_ID, 
		UPDATE_STATUS_FLAG, 
		ASSIGNEE_ROLE, 
		SHOW_ON_CALENDAR, 
		CATEGORY_ID, 
		REMINDER_WF_ITEM_TYPE, 
		REMINDER_WF_ITEM_KEY, 
		FREE_BUSY_TYPE, 
		BOOKING_START_DATE, 
		BOOKING_END_DATE, 
		OBJECT_CAPACITY_ID,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.JTF_TASK_ASSIGNMENTS
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (nvl(TASK_ASSIGNMENT_ID, 0),
		kca_seq_id) in 
(
		select
			nvl(TASK_ASSIGNMENT_ID,0) as TASK_ASSIGNMENT_ID,
			max(kca_seq_id) as kca_seq_id
		from
			bec_raw_dl_ext.JTF_TASK_ASSIGNMENTS
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(TASK_ASSIGNMENT_ID, 0))
		and 
		kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'jtf_task_assignments')
);
end;