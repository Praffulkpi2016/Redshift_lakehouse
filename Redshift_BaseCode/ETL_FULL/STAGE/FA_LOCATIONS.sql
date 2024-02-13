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

drop table if exists bec_ods_stg.fa_locations;

create table bec_ods_stg.fa_locations 
DISTKEY (LOCATION_ID)
SORTKEY (LOCATION_ID, last_update_date)
	as (
select
	*
	from
	bec_raw_dl_ext.fa_locations
where
	kca_operation != 'DELETE'
	and 
(nvl(LOCATION_ID, 0),
	last_update_date)
in (
	select
		nvl(LOCATION_ID, 0) as LOCATION_ID,
		max(last_update_date)
	from
		bec_raw_dl_ext.fa_locations
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		nvl(LOCATION_ID, 0)  
)
);
end;