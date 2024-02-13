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

drop table if exists bec_ods_stg.CST_COST_GROUP_ACCOUNTS;

create table bec_ods_stg.CST_COST_GROUP_ACCOUNTS 
DISTKEY (COST_GROUP_ID)
SORTKEY ( COST_GROUP_ID, ORGANIZATION_ID, LAST_UPDATE_DATE)
as
select
	*
from
bec_raw_dl_ext.CST_COST_GROUP_ACCOUNTS
where
	kca_operation != 'DELETE'
	and (COST_GROUP_ID,
	ORGANIZATION_ID,
	LAST_UPDATE_DATE) in 
(
	select
		COST_GROUP_ID,
		ORGANIZATION_ID,
		max(LAST_UPDATE_DATE)
	from
		bec_raw_dl_ext.CST_COST_GROUP_ACCOUNTS
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		COST_GROUP_ID,
		ORGANIZATION_ID);
end;