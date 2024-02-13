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

drop table if exists bec_ods_stg.MTL_UNITS_OF_MEASURE_TL;

create table bec_ods_stg.MTL_UNITS_OF_MEASURE_TL 
DISTKEY (UNIT_OF_MEASURE)
SORTKEY (UNIT_OF_MEASURE, language, last_update_date)
as 
select
	*
	from
	bec_raw_dl_ext.MTL_UNITS_OF_MEASURE_TL
where
	kca_operation != 'DELETE'
	and ( nvl(UNIT_OF_MEASURE, 'NA'),
		nvl(language, 'NA'),
		last_update_date) in 
	(
	select
		nvl(UNIT_OF_MEASURE, 'NA') as UNIT_OF_MEASURE,
		nvl(language, 'NA') as language,
		max(last_update_date)
	from
		bec_raw_dl_ext.MTL_UNITS_OF_MEASURE_TL
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		nvl(UNIT_OF_MEASURE, 'NA'),
		nvl(language, 'NA')
	);
end;