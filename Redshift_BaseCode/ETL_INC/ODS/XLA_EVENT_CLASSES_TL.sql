/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
begin;

-- Delete Records

delete from bec_ods.XLA_EVENT_CLASSES_TL
where (nvl(APPLICATION_ID,0),nvl(ENTITY_CODE,'NA'),nvl(EVENT_CLASS_CODE,'NA'),nvl(LANGUAGE,'NA')) in (
select nvl(stg.APPLICATION_ID,0) as APPLICATION_ID,nvl(stg.ENTITY_CODE,'NA') as ENTITY_CODE,nvl(stg.EVENT_CLASS_CODE,'NA') as EVENT_CLASS_CODE,nvl(stg.LANGUAGE,'NA') as LANGUAGE from bec_ods.XLA_EVENT_CLASSES_TL ods, bec_ods_stg.XLA_EVENT_CLASSES_TL stg
where nvl(ods.APPLICATION_ID,0) = nvl(stg.APPLICATION_ID,0) and nvl(ods.ENTITY_CODE,'NA') = nvl(stg.ENTITY_CODE,'NA') and nvl(ods.EVENT_CLASS_CODE,'NA') = nvl(stg.EVENT_CLASS_CODE,'NA') and nvl(ods.LANGUAGE,'NA') = nvl(stg.LANGUAGE,'NA') and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.XLA_EVENT_CLASSES_TL
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
        IS_DELETED_FLG,
		kca_seq_id
		,kca_seq_date)	
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
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.XLA_EVENT_CLASSES_TL
	where kca_operation in ('INSERT','UPDATE') 
	and (nvl(APPLICATION_ID,0),nvl(ENTITY_CODE,'NA'),nvl(EVENT_CLASS_CODE,'NA'),nvl(LANGUAGE,'NA'),kca_seq_id) in 
	(select nvl(APPLICATION_ID,0) as APPLICATION_ID,nvl(ENTITY_CODE,'NA') as ENTITY_CODE,nvl(EVENT_CLASS_CODE,'NA') as EVENT_CLASS_CODE,nvl(LANGUAGE,'NA') as LANGUAGE,max(kca_seq_id) from bec_ods_stg.XLA_EVENT_CLASSES_TL 
     where kca_operation in ('INSERT','UPDATE')
     group by nvl(APPLICATION_ID,0),nvl(ENTITY_CODE,'NA'),nvl(EVENT_CLASS_CODE,'NA'),nvl(LANGUAGE,'NA'))
);

commit;



-- Soft delete
update bec_ods.XLA_EVENT_CLASSES_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.XLA_EVENT_CLASSES_TL set IS_DELETED_FLG = 'Y'
where (nvl(APPLICATION_ID,0),nvl(ENTITY_CODE,'NA'),nvl(EVENT_CLASS_CODE,'NA'),nvl(LANGUAGE,'NA'))  in
(
select nvl(APPLICATION_ID,0),nvl(ENTITY_CODE,'NA'),nvl(EVENT_CLASS_CODE,'NA'),nvl(LANGUAGE,'NA') from bec_raw_dl_ext.XLA_EVENT_CLASSES_TL
where (nvl(APPLICATION_ID,0),nvl(ENTITY_CODE,'NA'),nvl(EVENT_CLASS_CODE,'NA'),nvl(LANGUAGE,'NA'),KCA_SEQ_ID)
in 
(
select nvl(APPLICATION_ID,0),nvl(ENTITY_CODE,'NA'),nvl(EVENT_CLASS_CODE,'NA'),nvl(LANGUAGE,'NA'),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.XLA_EVENT_CLASSES_TL
group by nvl(APPLICATION_ID,0),nvl(ENTITY_CODE,'NA'),nvl(EVENT_CLASS_CODE,'NA'),nvl(LANGUAGE,'NA')
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'xla_event_classes_tl';

commit;