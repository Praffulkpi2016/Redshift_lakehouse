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

drop table if exists bec_ods_stg.MRP_SR_ASSIGNMENTS ;

create table bec_ods_stg.MRP_SR_ASSIGNMENTS  
	DISTKEY (ASSIGNMENT_ID)
SORTKEY ( ASSIGNMENT_ID, last_update_date)
	as (
select
	*
	from
	bec_raw_dl_ext.MRP_SR_ASSIGNMENTS 
where
	kca_operation != 'DELETE'
	and 
(nvl(ASSIGNMENT_ID, 0),
	last_update_date)
in (
	select
		nvl(ASSIGNMENT_ID, 0) as ASSIGNMENT_ID,
		max(last_update_date)
	from
		bec_raw_dl_ext.MRP_SR_ASSIGNMENTS 
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		nvl(ASSIGNMENT_ID, 0)

		
)
);
end;