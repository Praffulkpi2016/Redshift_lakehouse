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
	table bec_ods_stg.JTF_TASKS_TL;

COMMIT;

insert
	into
	bec_ods_stg.JTF_TASKS_TL
(	
	TASK_ID, 
	LANGUAGE, 
	SOURCE_LANG, 
	TASK_NAME, 
	DESCRIPTION, 
	CREATED_BY, 
	CREATION_DATE, 
	LAST_UPDATED_BY, 
	LAST_UPDATE_DATE, 
	LAST_UPDATE_LOGIN, 
	SECURITY_GROUP_ID, 
	REJECTION_MESSAGE,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
)
(
	select
		TASK_ID, 
		LANGUAGE, 
		SOURCE_LANG, 
		TASK_NAME, 
		DESCRIPTION, 
		CREATED_BY, 
		CREATION_DATE, 
		LAST_UPDATED_BY, 
		LAST_UPDATE_DATE, 
		LAST_UPDATE_LOGIN, 
		SECURITY_GROUP_ID, 
		REJECTION_MESSAGE,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.JTF_TASKS_TL
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (nvl(TASK_ID, 0),
		nvl(LANGUAGE, 'NA'),
		kca_seq_id) in 
(
		select
			nvl(TASK_ID,0) as TASK_ID,
			nvl(LANGUAGE, 'NA') as LANGUAGE,
			max(kca_seq_id) as kca_seq_id
		from
			bec_raw_dl_ext.JTF_TASKS_TL
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(TASK_ID, 0),
			nvl(LANGUAGE, 'NA'))
		and 
		kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'jtf_tasks_tl')
);
end;