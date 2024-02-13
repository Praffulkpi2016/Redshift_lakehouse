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

drop table if exists bec_ods_stg.MTL_UNIT_TRANSACTIONS;

create table bec_ods_stg.MTL_UNIT_TRANSACTIONS 
DISTKEY (TRANSACTION_ID)
SORTKEY (SERIAL_NUMBER, last_update_date)
as 
select
	*
from
	bec_raw_dl_ext.MTL_UNIT_TRANSACTIONS
where
	kca_operation != 'DELETE'
	and (TRANSACTION_ID,
	SERIAL_NUMBER,
	last_update_date) in (
	select
		TRANSACTION_ID,
		SERIAL_NUMBER,
		max(last_update_date)
	from
		bec_raw_dl_ext.MTL_UNIT_TRANSACTIONS
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		TRANSACTION_ID,
		SERIAL_NUMBER);
end;