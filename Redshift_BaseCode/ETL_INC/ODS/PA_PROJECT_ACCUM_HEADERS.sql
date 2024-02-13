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
	bec_ods.PA_PROJECT_ACCUM_HEADERS
where
	(nvl(PROJECT_ACCUM_ID, 0)) in 
	(
	select
		NVL(stg.PROJECT_ACCUM_ID, 0) as PROJECT_ACCUM_ID

	from
		bec_ods.PA_PROJECT_ACCUM_HEADERS ods,
		bec_ods_stg.PA_PROJECT_ACCUM_HEADERS stg
	where
		NVL(ods.PROJECT_ACCUM_ID, 0) = NVL(stg.PROJECT_ACCUM_ID, 0)
				and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.PA_PROJECT_ACCUM_HEADERS (
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
	,kca_operation,
	IS_DELETED_FLG,
	kca_seq_id
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
		,kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
		,kca_seq_date
	from
		bec_ods_stg.PA_PROJECT_ACCUM_HEADERS
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		NVL(PROJECT_ACCUM_ID, 0),
		KCA_SEQ_ID
		) in 
	(
		select
			NVL(PROJECT_ACCUM_ID, 0) as PROJECT_ACCUM_ID,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.PA_PROJECT_ACCUM_HEADERS
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			NVL(PROJECT_ACCUM_ID, 0)
			)	
	);

commit;

-- Soft delete

update bec_ods.PA_PROJECT_ACCUM_HEADERS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.PA_PROJECT_ACCUM_HEADERS set IS_DELETED_FLG = 'Y'
where (PROJECT_ACCUM_ID)  in
(
select PROJECT_ACCUM_ID from bec_raw_dl_ext.PA_PROJECT_ACCUM_HEADERS
where (PROJECT_ACCUM_ID,KCA_SEQ_ID)
in 
(
select PROJECT_ACCUM_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.PA_PROJECT_ACCUM_HEADERS
group by PROJECT_ACCUM_ID
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
	ods_table_name = 'pa_project_accum_headers';

commit;