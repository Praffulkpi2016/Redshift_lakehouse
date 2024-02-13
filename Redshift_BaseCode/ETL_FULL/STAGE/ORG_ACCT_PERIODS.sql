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

drop table if exists bec_ods_stg.ORG_ACCT_PERIODS;

create table bec_ods_stg.ORG_ACCT_PERIODS 
	DISTKEY (ACCT_PERIOD_ID)
SORTKEY (ACCT_PERIOD_ID, ORGANIZATION_ID, last_update_date)
	as (
select
	*
	from
	bec_raw_dl_ext.ORG_ACCT_PERIODS
where
	kca_operation != 'DELETE'
	and 
( nvl(ACCT_PERIOD_ID, 0) ,nvl(ORGANIZATION_ID, 0) ,
	last_update_date)
in (
	select
		nvl(ACCT_PERIOD_ID, 0) as ACCT_PERIOD_ID ,
		nvl(ORGANIZATION_ID, 0) as ORGANIZATION_ID ,
		max(last_update_date)
	from
		bec_raw_dl_ext.ORG_ACCT_PERIODS
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		nvl(ACCT_PERIOD_ID, 0),nvl(ORGANIZATION_ID, 0)
)
);
end;