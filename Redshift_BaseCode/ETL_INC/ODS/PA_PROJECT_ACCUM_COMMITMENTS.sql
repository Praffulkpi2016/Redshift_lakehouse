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

delete from bec_ods.pa_project_accum_commitments
where (nvl(PROJECT_ACCUM_ID,0)) in (
select nvl(stg.PROJECT_ACCUM_ID,0) as PROJECT_ACCUM_ID from bec_ods.pa_project_accum_commitments ods, bec_ods_stg.pa_project_accum_commitments stg
where nvl(ods.PROJECT_ACCUM_ID,0) = nvl(stg.PROJECT_ACCUM_ID,0) and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.pa_project_accum_commitments
       (	
		PROJECT_ACCUM_ID,
		CMT_RAW_COST_ITD,
		CMT_RAW_COST_YTD,
		CMT_RAW_COST_PP,
		CMT_RAW_COST_PTD,
		CMT_BURDENED_COST_ITD,
		CMT_BURDENED_COST_YTD,
		CMT_BURDENED_COST_PP,
		CMT_BURDENED_COST_PTD,
		CMT_QUANTITY_ITD,
		CMT_QUANTITY_YTD,
		CMT_QUANTITY_PP,
		CMT_QUANTITY_PTD,
		CMT_UNIT_OF_MEASURE,
		LAST_UPDATED_BY,
		LAST_UPDATE_DATE,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_LOGIN,
		REQUEST_ID,
		PROGRAM_APPLICATION_ID,
		PROGRAM_ID,
		PROGRAM_UPDATE_DATE,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id
		,kca_seq_date)	
(
	select
		PROJECT_ACCUM_ID,
		CMT_RAW_COST_ITD,
		CMT_RAW_COST_YTD,
		CMT_RAW_COST_PP,
		CMT_RAW_COST_PTD,
		CMT_BURDENED_COST_ITD,
		CMT_BURDENED_COST_YTD,
		CMT_BURDENED_COST_PP,
		CMT_BURDENED_COST_PTD,
		CMT_QUANTITY_ITD,
		CMT_QUANTITY_YTD,
		CMT_QUANTITY_PP,
		CMT_QUANTITY_PTD,
		CMT_UNIT_OF_MEASURE,
		LAST_UPDATED_BY,
		LAST_UPDATE_DATE,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_LOGIN,
		REQUEST_ID,
		PROGRAM_APPLICATION_ID,
		PROGRAM_ID,
		PROGRAM_UPDATE_DATE,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
	from bec_ods_stg.pa_project_accum_commitments
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(PROJECT_ACCUM_ID,0),kca_seq_id) in 
	(select nvl(PROJECT_ACCUM_ID,0) as PROJECT_ACCUM_ID,max(kca_seq_id) from bec_ods_stg.pa_project_accum_commitments 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(PROJECT_ACCUM_ID,0))
);

commit;

-- Soft delete

update bec_ods.pa_project_accum_commitments set IS_DELETED_FLG = 'N';
commit;
update bec_ods.pa_project_accum_commitments set IS_DELETED_FLG = 'Y'
where (PROJECT_ACCUM_ID)  in
(
select PROJECT_ACCUM_ID from bec_raw_dl_ext.pa_project_accum_commitments
where (PROJECT_ACCUM_ID,KCA_SEQ_ID)
in 
(
select PROJECT_ACCUM_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.pa_project_accum_commitments
group by PROJECT_ACCUM_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'pa_project_accum_commitments';

commit;