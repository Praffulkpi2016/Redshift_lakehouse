/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
begin;

truncate table bec_ods_stg.FA_LOOKUPS_TL;

insert into	bec_ods_stg.FA_LOOKUPS_TL
   (
	LOOKUP_TYPE, 
	LOOKUP_CODE, 
	LANGUAGE, 
	SOURCE_LANG, 
	MEANING, 
	DESCRIPTION, 
	LAST_UPDATE_DATE, 
	LAST_UPDATED_BY, 
	CREATED_BY, 
	CREATION_DATE, 
	LAST_UPDATE_LOGIN, 
	ZD_EDITION_NAME, 
	ZD_SYNC,
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		LOOKUP_TYPE, 
		LOOKUP_CODE, 
		LANGUAGE, 
		SOURCE_LANG, 
		MEANING, 
		DESCRIPTION, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATED_BY, 
		CREATION_DATE, 
		LAST_UPDATE_LOGIN, 
		ZD_EDITION_NAME, 
		ZD_SYNC,
        KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from bec_raw_dl_ext.FA_LOOKUPS_TL
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (nvl(LOOKUP_TYPE,'NA'),nvl(LOOKUP_CODE,'NA'),nvl(language,'NA'),kca_seq_id) in 
	(select nvl(LOOKUP_TYPE,'NA') as LOOKUP_TYPE,nvl(LOOKUP_CODE,'NA') as LOOKUP_CODE,nvl(language,'NA') as language,max(kca_seq_id) as kca_seq_id from bec_raw_dl_ext.FA_LOOKUPS_TL 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by nvl(LOOKUP_TYPE,'NA'),nvl(LOOKUP_CODE,'NA'),nvl(language,'NA'))
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'fa_lookups_tl')
);
end;