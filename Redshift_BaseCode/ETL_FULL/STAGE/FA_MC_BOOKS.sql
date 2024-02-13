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

drop table if exists bec_ods_stg.fa_mc_books;

create table bec_ods_stg.fa_mc_books 
	DISTKEY (SET_OF_BOOKS_ID)
SORTKEY ( SET_OF_BOOKS_ID, TRANSACTION_HEADER_ID_IN, last_update_date)
as (
select
	*
from
bec_raw_dl_ext.fa_mc_books
where
	kca_operation != 'DELETE'
	and 
(nvl(SET_OF_BOOKS_ID, 0),
	nvl(TRANSACTION_HEADER_ID_IN, 0),
	last_update_date)
in (
	select
		nvl(SET_OF_BOOKS_ID, 0) as SET_OF_BOOKS_ID,
		nvl(TRANSACTION_HEADER_ID_IN, 0) as TRANSACTION_HEADER_ID_IN,
		max(last_update_date)
	from
		bec_raw_dl_ext.fa_mc_books
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		nvl(SET_OF_BOOKS_ID, 0),
		nvl(TRANSACTION_HEADER_ID_IN, 0)  
)
);
end;