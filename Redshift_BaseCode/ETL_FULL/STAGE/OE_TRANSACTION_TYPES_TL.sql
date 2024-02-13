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

drop table if exists bec_ods_stg.OE_TRANSACTION_TYPES_TL;

create table bec_ods_stg.OE_TRANSACTION_TYPES_TL 
	DISTKEY (TRANSACTION_TYPE_ID)
SORTKEY (TRANSACTION_TYPE_ID, language, last_update_date)
	as (
select
	*
from
	bec_raw_dl_ext.OE_TRANSACTION_TYPES_TL
where
	kca_operation != 'DELETE'
	and 
( nvl(TRANSACTION_TYPE_ID, 0) ,
	nvl(language, 'NA'),
	last_update_date)
in (
	select
			nvl(TRANSACTION_TYPE_ID, 0) as TRANSACTION_TYPE_ID ,
			nvl(language, 'NA') as language,
		max(last_update_date)
	from
		bec_raw_dl_ext.OE_TRANSACTION_TYPES_TL
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		TRANSACTION_TYPE_ID,
		language
)
);
end;