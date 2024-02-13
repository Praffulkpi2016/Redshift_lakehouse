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

drop table if exists bec_ods_stg.fa_asset_history;

create table bec_ods_stg.fa_asset_history 
DISTKEY (ASSET_ID)
SORTKEY (ASSET_ID, DATE_EFFECTIVE, last_update_date)
	as (
select
	*
	from
	bec_raw_dl_ext.fa_asset_history
where
	kca_operation != 'DELETE'
	and 
(   nvl(ASSET_ID, 0),
	nvl(DATE_EFFECTIVE,'1900-01-01 00:00:00'),
	last_update_date)
in (
	select
		nvl(ASSET_ID, 0) as ASSET_ID,
		nvl(DATE_EFFECTIVE,'1900-01-01 00:00:00') as DATE_EFFECTIVE,
		max(last_update_date)
	from
		bec_raw_dl_ext.fa_asset_history
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		nvl(ASSET_ID, 0),
	    nvl(DATE_EFFECTIVE,'1900-01-01 00:00:00') 
)
);
end;