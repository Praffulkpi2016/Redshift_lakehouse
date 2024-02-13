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
	table bec_ods_stg.CSF_DEBRIEF_HEADERS;

COMMIT;

insert
	into
	bec_ods_stg.CSF_DEBRIEF_HEADERS
(	
	DEBRIEF_HEADER_ID, 
	DEBRIEF_NUMBER, 
	DEBRIEF_DATE, 
	DEBRIEF_STATUS_ID, 
	TASK_ASSIGNMENT_ID, 
	CREATED_BY, 
	CREATION_DATE, 
	LAST_UPDATED_BY, 
	LAST_UPDATE_DATE, 
	LAST_UPDATE_LOGIN, 
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
	STATISTICS_UPDATED, 
	PROCESSED_FLAG, 
	TRAVEL_START_TIME, 
	TRAVEL_END_TIME, 
	OBJECT_VERSION_NUMBER, 
	TRAVEL_DISTANCE_IN_KM,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
)
(
	select
		DEBRIEF_HEADER_ID, 
		DEBRIEF_NUMBER, 
		DEBRIEF_DATE, 
		DEBRIEF_STATUS_ID, 
		TASK_ASSIGNMENT_ID, 
		CREATED_BY, 
		CREATION_DATE, 
		LAST_UPDATED_BY, 
		LAST_UPDATE_DATE, 
		LAST_UPDATE_LOGIN, 
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
		STATISTICS_UPDATED, 
		PROCESSED_FLAG, 
		TRAVEL_START_TIME, 
		TRAVEL_END_TIME, 
		OBJECT_VERSION_NUMBER, 
		TRAVEL_DISTANCE_IN_KM,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.CSF_DEBRIEF_HEADERS
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (nvl(DEBRIEF_HEADER_ID, 0),
		kca_seq_id) in 
(
		select
			nvl(DEBRIEF_HEADER_ID,0) as DEBRIEF_HEADER_ID,
			max(kca_seq_id) as kca_seq_id
		from
			bec_raw_dl_ext.CSF_DEBRIEF_HEADERS
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(DEBRIEF_HEADER_ID, 0))
		and 
		kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'csf_debrief_headers')
);
end;