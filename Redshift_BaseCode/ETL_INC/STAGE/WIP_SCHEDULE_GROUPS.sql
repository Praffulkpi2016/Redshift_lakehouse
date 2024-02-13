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
	table bec_ods_stg.WIP_SCHEDULE_GROUPS;

insert
	into
	bec_ods_stg.WIP_SCHEDULE_GROUPS
   (
	schedule_group_name,
	schedule_group_id,
	organization_id,
	description,
	inactive_on,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	attribute_category,
	request_id,
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
	KCA_OPERATION,
	kca_seq_id
	,KCA_SEQ_DATE)
(
	select
		schedule_group_name,
		schedule_group_id,
		organization_id,
		description,
		inactive_on,
		last_update_date,
		last_updated_by,
		creation_date,
		created_by,
		last_update_login,
		program_application_id,
		program_id,
		program_update_date,
		attribute_category,
		request_id,
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
		KCA_OPERATION,
		kca_seq_id
		,KCA_SEQ_DATE
	from
		bec_raw_dl_ext.WIP_SCHEDULE_GROUPS
	where
		kca_operation != 'DELETE'
		and (SCHEDULE_GROUP_ID,
		kca_seq_id) in 
	(
		select
			SCHEDULE_GROUP_ID,
			max(kca_seq_id)
		from
			bec_raw_dl_ext.WIP_SCHEDULE_GROUPS
		where
			kca_operation != 'DELETE'
		group by
			SCHEDULE_GROUP_ID)
		and (KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'wip_schedule_groups')
            )
);
end;