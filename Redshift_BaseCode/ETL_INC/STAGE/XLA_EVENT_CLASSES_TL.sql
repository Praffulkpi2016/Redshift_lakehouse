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

truncate table bec_ods_stg.XLA_EVENT_CLASSES_TL;

insert into	bec_ods_stg.XLA_EVENT_CLASSES_TL
   (	
	APPLICATION_ID, 
	ENTITY_CODE, 
	EVENT_CLASS_CODE, 
	LANGUAGE, 
	NAME, 
	DESCRIPTION, 
	SOURCE_LANG, 
	CREATION_DATE, 
	CREATED_BY, 
	LAST_UPDATE_DATE, 
	LAST_UPDATED_BY, 
	LAST_UPDATE_LOGIN, 
	ZD_EDITION_NAME, 
	ZD_SYNC,
    KCA_OPERATION,
	kca_seq_id
	,KCA_SEQ_DATE)
(
	select
		APPLICATION_ID, 
		ENTITY_CODE, 
		EVENT_CLASS_CODE, 
		LANGUAGE, 
		NAME, 
		DESCRIPTION, 
		SOURCE_LANG, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		LAST_UPDATE_LOGIN, 
		ZD_EDITION_NAME, 
		ZD_SYNC,
        KCA_OPERATION,
		kca_seq_id
		,KCA_SEQ_DATE
	from bec_raw_dl_ext.XLA_EVENT_CLASSES_TL
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (nvl(APPLICATION_ID,0),nvl(ENTITY_CODE,'NA'),nvl(EVENT_CLASS_CODE,'NA'),nvl(LANGUAGE,'NA'),kca_seq_id) in 
	(select nvl(APPLICATION_ID,0),nvl(ENTITY_CODE,'NA'),nvl(EVENT_CLASS_CODE,'NA'),nvl(LANGUAGE,'NA'),max(kca_seq_id) from bec_raw_dl_ext.XLA_EVENT_CLASSES_TL 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by nvl(APPLICATION_ID,0),nvl(ENTITY_CODE,'NA'),nvl(EVENT_CLASS_CODE,'NA'),nvl(LANGUAGE,'NA'))
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'xla_event_classes_tl')

            )
);
end;