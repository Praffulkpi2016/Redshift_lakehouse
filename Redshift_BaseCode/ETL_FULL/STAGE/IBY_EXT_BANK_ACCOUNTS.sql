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

drop table if exists bec_ods_stg.IBY_EXT_BANK_ACCOUNTS;

create table bec_ods_stg.IBY_EXT_BANK_ACCOUNTS 
	DISTKEY (EXT_BANK_ACCOUNT_ID)
SORTKEY (EXT_BANK_ACCOUNT_ID, last_update_date)
	as (
select
	*
from
	bec_raw_dl_ext.IBY_EXT_BANK_ACCOUNTS
where
	kca_operation != 'DELETE'
	and 
( nvl(EXT_BANK_ACCOUNT_ID, 0),
	last_update_date)
in (
	select
		nvl(EXT_BANK_ACCOUNT_ID,0) as EXT_BANK_ACCOUNT_ID,
		max(last_update_date)
	from
		bec_raw_dl_ext.IBY_EXT_BANK_ACCOUNTS
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		EXT_BANK_ACCOUNT_ID
		
)
);
end;