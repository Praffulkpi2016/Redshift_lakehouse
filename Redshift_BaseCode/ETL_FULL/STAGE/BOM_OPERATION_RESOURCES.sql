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

drop table if exists bec_ods_stg.BOM_OPERATION_RESOURCES;

create table bec_ods_stg.bom_operation_resources 
DISTKEY (OPERATION_SEQUENCE_ID)
SORTKEY (OPERATION_SEQUENCE_ID, RESOURCE_SEQ_NUM, LAST_UPDATE_DATE)
	as (
select
	*
from
	bec_raw_dl_ext.bom_operation_resources
where
	kca_operation != 'DELETE'
	and 
( nvl(OPERATION_SEQUENCE_ID, 0) ,nvl(RESOURCE_SEQ_NUM, 0) ,
	last_update_date)
in (
	select
		nvl(OPERATION_SEQUENCE_ID, 0) as OPERATION_SEQUENCE_ID ,
		nvl(RESOURCE_SEQ_NUM, 0) as RESOURCE_SEQ_NUM ,
		max(last_update_date)
	from
		bec_raw_dl_ext.bom_operation_resources
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		nvl(OPERATION_SEQUENCE_ID, 0),nvl(RESOURCE_SEQ_NUM, 0)
)
);
end;