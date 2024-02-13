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

truncate table bec_ods_stg.WF_ACTIVITIES;

insert into	bec_ods_stg.WF_ACTIVITIES
   (	
	item_type,
	"name",
	version,
	"type",
	rerun,
	expand_role,
	protect_level,
	custom_level,
	begin_date,
	end_date,
	"function",
	result_type,
	cost,
	read_role,
	write_role,
	execute_role,
	icon_name,
	message,
	error_process,
	error_item_type,
	runnable_flag,
	function_type,
	event_name,
	direction,
	security_group_id,
	zd_edition_name,
	zd_sync,
    KCA_OPERATION,
	kca_seq_id
	,KCA_SEQ_DATE)
(
	select
	item_type,
	"name",
	version,
	"type",
	rerun,
	expand_role,
	protect_level,
	custom_level,
	begin_date,
	end_date,
	"function",
	result_type,
	cost,
	read_role,
	write_role,
	execute_role,
	icon_name,
	message,
	error_process,
	error_item_type,
	runnable_flag,
	function_type,
	event_name,
	direction,
	security_group_id,
	zd_edition_name,
	zd_sync,
        KCA_OPERATION,
		kca_seq_id
		,KCA_SEQ_DATE
	from bec_raw_dl_ext.WF_ACTIVITIES
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (nvl(ITEM_TYPE, 'NA'),
	nvl(NAME, 'NA'),
	nvl(VERSION, 0),kca_seq_id) in 
	(select nvl(ITEM_TYPE, 'NA') as ITEM_TYPE,
		nvl(NAME, 'NA') as NAME,
		nvl(VERSION, 0) as VERSION,max(kca_seq_id) from bec_raw_dl_ext.WF_ACTIVITIES 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by nvl(ITEM_TYPE, 'NA'),
	nvl(NAME, 'NA'),
	nvl(VERSION, 0))
        and	(
            KCA_SEQ_DATE > (
        select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'wf_activities'))
);
end;