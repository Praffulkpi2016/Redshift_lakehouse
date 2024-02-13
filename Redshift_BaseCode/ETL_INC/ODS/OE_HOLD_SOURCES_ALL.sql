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

delete from bec_ods.OE_HOLD_SOURCES_ALL
where nvl(HOLD_SOURCE_ID,0) in (
select nvl(stg.HOLD_SOURCE_ID,0) as HOLD_SOURCE_ID from bec_ods.OE_HOLD_SOURCES_ALL ods, bec_ods_stg.OE_HOLD_SOURCES_ALL stg
where nvl(ods.HOLD_SOURCE_ID,0) = nvl(stg.HOLD_SOURCE_ID,0) and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.OE_HOLD_SOURCES_ALL
       (	
		HOLD_SOURCE_ID
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      LAST_UPDATE_LOGIN
,      CREATION_DATE
,      CREATED_BY
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      REQUEST_ID
,      HOLD_ID
,      HOLD_ENTITY_CODE
,      HOLD_ENTITY_ID
,      HOLD_ENTITY_CODE2
,      HOLD_ENTITY_ID2
,      HOLD_UNTIL_DATE
,      RELEASED_FLAG
,      HOLD_COMMENT
,      ORG_ID
,      CONTEXT
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
,      HOLD_RELEASE_ID,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id
		,kca_seq_date)	
(
	select
		HOLD_SOURCE_ID
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      LAST_UPDATE_LOGIN
,      CREATION_DATE
,      CREATED_BY
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      REQUEST_ID
,      HOLD_ID
,      HOLD_ENTITY_CODE
,      HOLD_ENTITY_ID
,      HOLD_ENTITY_CODE2
,      HOLD_ENTITY_ID2
,      HOLD_UNTIL_DATE
,      RELEASED_FLAG
,      HOLD_COMMENT
,      ORG_ID
,      CONTEXT
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
,      HOLD_RELEASE_ID,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.OE_HOLD_SOURCES_ALL
	where kca_operation in ('INSERT','UPDATE') 
	and (nvl(HOLD_SOURCE_ID,0),kca_seq_id) in 
	(select nvl(HOLD_SOURCE_ID,0) as HOLD_SOURCE_ID,max(kca_seq_id) from bec_ods_stg.OE_HOLD_SOURCES_ALL 
     where kca_operation in ('INSERT','UPDATE')
     group by nvl(HOLD_SOURCE_ID,0))
);

commit;



-- Soft delete
update bec_ods.OE_HOLD_SOURCES_ALL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.OE_HOLD_SOURCES_ALL set IS_DELETED_FLG = 'Y'
where (HOLD_SOURCE_ID)  in
(
select HOLD_SOURCE_ID from bec_raw_dl_ext.OE_HOLD_SOURCES_ALL
where (HOLD_SOURCE_ID,KCA_SEQ_ID)
in 
(
select HOLD_SOURCE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.OE_HOLD_SOURCES_ALL
group by HOLD_SOURCE_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'oe_hold_sources_all';

commit;