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

truncate table bec_ods_stg.WF_ITEM_TYPES_TL;

insert into	bec_ods_stg.WF_ITEM_TYPES_TL
   (	
	"name",
	"language",
	display_name,
	protect_level,
	custom_level,
	description,
	source_lang,
	security_group_id,
	zd_edition_name,
	zd_sync,
    KCA_OPERATION,
	kca_seq_id
	,KCA_SEQ_DATE)
(
	select
		"name",
	"language",
	display_name,
	protect_level,
	custom_level,
	description,
	source_lang,
	security_group_id,
	zd_edition_name,
	zd_sync,
        KCA_OPERATION,
		kca_seq_id
		,KCA_SEQ_DATE
	from bec_raw_dl_ext.WF_ITEM_TYPES_TL
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (nvl(NAME,'NA'),nvl(LANGUAGE,'NA'),kca_seq_id) in 
	(select nvl(NAME,'NA') AS NAME ,nvl(LANGUAGE,'NA') AS LANGUAGE,max(kca_seq_id) from bec_raw_dl_ext.WF_ITEM_TYPES_TL 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by nvl(NAME,'NA'),nvl(LANGUAGE,'NA'))
        and	KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'wf_item_types_tl')
);
end;