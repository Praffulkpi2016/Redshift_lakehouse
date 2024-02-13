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
	table bec_ods_stg.PA_PROJECT_ACCUM_HEADERS;

insert
	into
	bec_ods_stg.PA_PROJECT_ACCUM_HEADERS
    (
	PROJECT_ACCUM_ID
,      PROJECT_ID
,      TASK_ID
,      ACCUM_PERIOD
,      RESOURCE_ID
,      RESOURCE_LIST_ASSIGNMENT_ID
,      RESOURCE_LIST_ID
,      RESOURCE_LIST_MEMBER_ID
,      LAST_UPDATED_BY
,      LAST_UPDATE_DATE
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      TASKS_RESTRUCTURED_FLAG
,      SUM_EXCEPTION_CODE
	,KCA_OPERATION,
	KCA_SEQ_ID
	,kca_seq_date)
(
	select
	PROJECT_ACCUM_ID
,      PROJECT_ID
,      TASK_ID
,      ACCUM_PERIOD
,      RESOURCE_ID
,      RESOURCE_LIST_ASSIGNMENT_ID
,      RESOURCE_LIST_ID
,      RESOURCE_LIST_MEMBER_ID
,      LAST_UPDATED_BY
,      LAST_UPDATE_DATE
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      TASKS_RESTRUCTURED_FLAG
,      SUM_EXCEPTION_CODE
		,KCA_OPERATION,
		KCA_SEQ_ID
		,kca_seq_date
	from
		bec_raw_dl_ext.PA_PROJECT_ACCUM_HEADERS
	where
		kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and ( nvl(PROJECT_ACCUM_ID, 0) ,
		KCA_SEQ_ID) in 
	(
		select
			nvl(PROJECT_ACCUM_ID, 0) as PROJECT_ACCUM_ID,
			max(KCA_SEQ_ID)
		from
			bec_raw_dl_ext.PA_PROJECT_ACCUM_HEADERS
		where
			kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(PROJECT_ACCUM_ID, 0) 
	)
		and kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'pa_project_accum_headers'));
end;