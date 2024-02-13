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

delete from bec_ods.xla_event_types_tl
where (APPLICATION_ID,ENTITY_CODE,EVENT_CLASS_CODE,EVENT_TYPE_CODE,LANGUAGE) in (
select stg.APPLICATION_ID,stg.ENTITY_CODE,stg.EVENT_CLASS_CODE,stg.EVENT_TYPE_CODE,stg.LANGUAGE from bec_ods.xla_event_types_tl ods, bec_ods_stg.xla_event_types_tl stg
where ods.APPLICATION_ID = stg.APPLICATION_ID 
and ods.ENTITY_CODE = stg.ENTITY_CODE 
and ods.EVENT_CLASS_CODE = stg.EVENT_CLASS_CODE 
and ods.EVENT_TYPE_CODE = stg.EVENT_TYPE_CODE 
and ods.LANGUAGE = stg.LANGUAGE 
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.xla_event_types_tl
       (APPLICATION_ID,
		ENTITY_CODE,
		EVENT_CLASS_CODE,
		EVENT_TYPE_CODE,
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
		EVENT_TYPE_CODE,
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
	from bec_ods_stg.xla_event_types_tl
	where kca_operation in ('INSERT','UPDATE') 
	and (APPLICATION_ID,ENTITY_CODE,EVENT_CLASS_CODE,EVENT_TYPE_CODE,LANGUAGE,kca_seq_id) in 
	(select APPLICATION_ID,ENTITY_CODE,EVENT_CLASS_CODE,EVENT_TYPE_CODE,LANGUAGE,max(kca_seq_id) from bec_ods_stg.xla_event_types_tl 
     where kca_operation in ('INSERT','UPDATE')
     group by APPLICATION_ID,ENTITY_CODE,EVENT_CLASS_CODE,EVENT_TYPE_CODE,LANGUAGE)
);

commit;



-- Soft delete
update bec_ods.xla_event_types_tl set IS_DELETED_FLG = 'N';
commit;
update bec_ods.xla_event_types_tl set IS_DELETED_FLG = 'Y'
where (APPLICATION_ID,ENTITY_CODE,EVENT_CLASS_CODE,EVENT_TYPE_CODE,LANGUAGE)  in
(
select APPLICATION_ID,ENTITY_CODE,EVENT_CLASS_CODE,EVENT_TYPE_CODE,LANGUAGE from bec_raw_dl_ext.xla_event_types_tl
where (APPLICATION_ID,ENTITY_CODE,EVENT_CLASS_CODE,EVENT_TYPE_CODE,LANGUAGE,KCA_SEQ_ID)
in 
(
select APPLICATION_ID,ENTITY_CODE,EVENT_CLASS_CODE,EVENT_TYPE_CODE,LANGUAGE,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.xla_event_types_tl
group by APPLICATION_ID,ENTITY_CODE,EVENT_CLASS_CODE,EVENT_TYPE_CODE,LANGUAGE
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'xla_event_types_tl';

commit;