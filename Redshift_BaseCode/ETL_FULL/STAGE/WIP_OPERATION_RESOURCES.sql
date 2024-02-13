/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for stage.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_ods_stg.WIP_OPERATION_RESOURCES;

create table bec_ods_stg.WIP_OPERATION_RESOURCES 
DISTKEY (WIP_ENTITY_ID)
SORTKEY (WIP_ENTITY_ID, OPERATION_SEQ_NUM, RESOURCE_SEQ_NUM, REPETITIVE_SCHEDULE_ID, last_update_date)
as 
select
	*
	from
	bec_raw_dl_ext.WIP_OPERATION_RESOURCES
where
	kca_operation != 'DELETE'
	and (
	nvl(WIP_ENTITY_ID,0) ,nvl(OPERATION_SEQ_NUM,0),nvl(RESOURCE_SEQ_NUM,0),nvl(REPETITIVE_SCHEDULE_ID,0), last_update_date
	) 
	in 
(
select
		nvl(WIP_ENTITY_ID,0) as WIP_ENTITY_ID,nvl(OPERATION_SEQ_NUM,0) as OPERATION_SEQ_NUM,
		nvl(RESOURCE_SEQ_NUM,0) as RESOURCE_SEQ_NUM,nvl(REPETITIVE_SCHEDULE_ID,0) as REPETITIVE_SCHEDULE_ID,
		max(last_update_date)
	from
		bec_raw_dl_ext.WIP_OPERATION_RESOURCES
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		nvl(WIP_ENTITY_ID,0) ,nvl(OPERATION_SEQ_NUM,0),nvl(RESOURCE_SEQ_NUM,0),nvl(REPETITIVE_SCHEDULE_ID,0));
end;