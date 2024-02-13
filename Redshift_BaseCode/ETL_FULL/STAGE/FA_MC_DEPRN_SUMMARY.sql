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

drop table if exists bec_ods_stg.fa_mc_deprn_summary;

create table bec_ods_stg.fa_mc_deprn_summary 
	DISTKEY (ASSET_ID)  
SORTKEY (BOOK_TYPE_CODE, ASSET_ID, PERIOD_COUNTER, SET_OF_BOOKS_ID)
as (
select
	*
	from
	bec_raw_dl_ext.fa_mc_deprn_summary
where
	kca_operation != 'DELETE'
	and 
(nvl(BOOK_TYPE_CODE, 'NA'),
	nvl(ASSET_ID, 0),
	nvl(PERIOD_COUNTER, 0),
	nvl(SET_OF_BOOKS_ID, 0) )
in (
	select
		nvl(BOOK_TYPE_CODE, 'NA') as BOOK_TYPE_CODE,
		nvl(ASSET_ID, 0) as ASSET_ID,
		nvl(PERIOD_COUNTER, 0) as PERIOD_COUNTER,
		nvl(SET_OF_BOOKS_ID, 0) as SET_OF_BOOKS_ID 
	from
		bec_raw_dl_ext.fa_mc_deprn_summary
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		nvl(BOOK_TYPE_CODE, 'NA'),
		nvl(ASSET_ID, 0),
		nvl(PERIOD_COUNTER, 0),
		nvl(SET_OF_BOOKS_ID, 0)  
)
);
end;