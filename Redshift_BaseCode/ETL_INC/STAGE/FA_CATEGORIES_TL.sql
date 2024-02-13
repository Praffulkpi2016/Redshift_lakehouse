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

truncate table bec_ods_stg.fa_categories_tl;

insert into	bec_ods_stg.fa_categories_tl
   (CATEGORY_ID,
	LANGUAGE,
	SOURCE_LANG,
	DESCRIPTION,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	CREATED_BY,
	CREATION_DATE,
	LAST_UPDATE_LOGIN,
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		CATEGORY_ID,
		LANGUAGE,
		SOURCE_LANG,
		DESCRIPTION,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		CREATED_BY,
		CREATION_DATE,
		LAST_UPDATE_LOGIN,
        KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from bec_raw_dl_ext.fa_categories_tl
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (nvl(CATEGORY_ID,0),nvl(LANGUAGE,'NA'),kca_seq_id) in 
	(select nvl(CATEGORY_ID,0) as CATEGORY_ID,nvl(LANGUAGE,'NA') as LANGUAGE,max(kca_seq_id) from bec_raw_dl_ext.fa_categories_tl 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by nvl(CATEGORY_ID,0),nvl(LANGUAGE,'NA'))
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'fa_categories_tl')
);
end;