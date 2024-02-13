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

drop table if exists bec_ods_stg.WF_ACTIVITIES_TL;

create table bec_ods_stg.WF_ACTIVITIES_TL 
DISTKEY (ITEM_TYPE)
SORTKEY (NAME, VERSION, language)
as 
select
	*
	from
	bec_raw_dl_ext.WF_ACTIVITIES_TL
where
	kca_operation != 'DELETE'
	and (nvl(ITEM_TYPE, 'NA'),
	nvl(NAME, 'NA'),
	nvl(VERSION, 0),
	nvl(language, 'NA')) in 
(
	select
		nvl(ITEM_TYPE, 'NA') as ITEM_TYPE,
		nvl(NAME, 'NA') as NAME,
		nvl(VERSION, 0) as VERSION,
		nvl(language, 'NA') as language
	from
		bec_raw_dl_ext.WF_ACTIVITIES_TL
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		nvl(ITEM_TYPE, 'NA'),
		nvl(NAME, 'NA'),
		nvl(VERSION, 0),
		nvl(language, 'NA'));
end;