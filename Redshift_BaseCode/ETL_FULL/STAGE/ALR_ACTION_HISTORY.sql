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

drop table if exists bec_ods_stg.ALR_ACTION_HISTORY;

create table bec_ods_stg.alr_action_history
DISTKEY (APPLICATION_ID)
SORTKEY (last_update_date, APPLICATION_ID, ACTION_HISTORY_ID)
 as (
select
	*
from
	bec_raw_dl_ext.alr_action_history
where
	kca_operation != 'DELETE'
	and 
( nvl(APPLICATION_ID, 0) ,nvl(ACTION_HISTORY_ID, 0) ,
	last_update_date)
in (
	select
		nvl(APPLICATION_ID, 0) as APPLICATION_ID ,
		nvl(ACTION_HISTORY_ID, 0) as ACTION_HISTORY_ID ,
		max(last_update_date)
	from
		bec_raw_dl_ext.alr_action_history
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		nvl(APPLICATION_ID, 0),nvl(ACTION_HISTORY_ID, 0)
)
);
end;