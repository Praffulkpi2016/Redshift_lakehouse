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

truncate table bec_ods_stg.pa_project_accum_commitments;

insert into	bec_ods_stg.pa_project_accum_commitments
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
		kca_seq_id
		,kca_seq_date
	from bec_raw_dl_ext.pa_project_accum_commitments
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (nvl(PROJECT_ACCUM_ID,0),kca_seq_id) in 
	(select nvl(PROJECT_ACCUM_ID,0) as PROJECT_ACCUM_ID,max(kca_seq_id) from bec_raw_dl_ext.pa_project_accum_commitments 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by nvl(PROJECT_ACCUM_ID,0))
        and	(kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'pa_project_accum_commitments'))
);
end;