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

drop table if exists bec_ods_stg.WIP_PERIOD_BALANCES;

create table bec_ods_stg.WIP_PERIOD_BALANCES 
DISTSTYLE ALL
SORTKEY (WIP_ENTITY_ID, REPETITIVE_SCHEDULE_ID, ACCT_PERIOD_ID, last_update_date)

as 
select
	*
from
bec_raw_dl_ext.WIP_PERIOD_BALANCES
where
	kca_operation != 'DELETE'
	and (
nvl(WIP_ENTITY_ID, 0),
	nvl(REPETITIVE_SCHEDULE_ID, 0),
	nvl(ACCT_PERIOD_ID, 0),
	last_update_date
) in 
(
	select
		nvl(WIP_ENTITY_ID, 0) as WIP_ENTITY_ID,
		nvl(REPETITIVE_SCHEDULE_ID, 0) as REPETITIVE_SCHEDULE_ID,
		nvl(ACCT_PERIOD_ID, 0) as ACCT_PERIOD_ID,
		max(last_update_date)
	from
		bec_raw_dl_ext.WIP_PERIOD_BALANCES
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		WIP_ENTITY_ID,
		REPETITIVE_SCHEDULE_ID,
		ACCT_PERIOD_ID);
end;