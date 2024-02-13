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

delete from bec_ods.GL_LEGAL_ENTITIES_BSVS
where (LEGAL_ENTITY_ID,FLEX_VALUE_SET_ID,FLEX_SEGMENT_VALUE) in (
select stg.LEGAL_ENTITY_ID,stg.FLEX_VALUE_SET_ID,stg.FLEX_SEGMENT_VALUE from bec_ods.GL_LEGAL_ENTITIES_BSVS ods, bec_ods_stg.GL_LEGAL_ENTITIES_BSVS stg
where ods.LEGAL_ENTITY_ID = stg.LEGAL_ENTITY_ID and ods.FLEX_VALUE_SET_ID = stg.FLEX_VALUE_SET_ID and ods.FLEX_SEGMENT_VALUE = stg.FLEX_SEGMENT_VALUE and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.GL_LEGAL_ENTITIES_BSVS
(
 LEGAL_ENTITY_ID,
 FLEX_VALUE_SET_ID,
 FLEX_SEGMENT_VALUE,
 START_DATE,
 END_DATE, 
 LAST_UPDATE_DATE,
 LAST_UPDATED_BY, 
 LAST_UPDATE_LOGIN,
 CREATION_DATE, 
 CREATED_BY,
 KCA_OPERATION,
 IS_DELETED_FLG,
 kca_seq_id,
	kca_seq_date
)

(SELECT
 LEGAL_ENTITY_ID,
 FLEX_VALUE_SET_ID,
 FLEX_SEGMENT_VALUE,
 START_DATE,
 END_DATE, 
 LAST_UPDATE_DATE,
 LAST_UPDATED_BY, 
 LAST_UPDATE_LOGIN,
 CREATION_DATE, 
 CREATED_BY,
 KCA_OPERATION,
 'N' AS IS_DELETED_FLG,
cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from
	bec_ods_stg.GL_LEGAL_ENTITIES_BSVS
where kca_operation IN ('INSERT','UPDATE') 
	and (LEGAL_ENTITY_ID,FLEX_VALUE_SET_ID,FLEX_SEGMENT_VALUE,kca_seq_id) in 
	(select LEGAL_ENTITY_ID,FLEX_VALUE_SET_ID,FLEX_SEGMENT_VALUE,max(kca_seq_id) from bec_ods_stg.GL_LEGAL_ENTITIES_BSVS 
     where kca_operation IN ('INSERT','UPDATE')
     group by LEGAL_ENTITY_ID,FLEX_VALUE_SET_ID,FLEX_SEGMENT_VALUE)
);
commit;

-- Soft delete
update bec_ods.GL_LEGAL_ENTITIES_BSVS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.GL_LEGAL_ENTITIES_BSVS set IS_DELETED_FLG = 'Y'
where (LEGAL_ENTITY_ID,FLEX_VALUE_SET_ID,FLEX_SEGMENT_VALUE)  in
(
select LEGAL_ENTITY_ID,FLEX_VALUE_SET_ID,FLEX_SEGMENT_VALUE from bec_raw_dl_ext.GL_LEGAL_ENTITIES_BSVS
where (LEGAL_ENTITY_ID,FLEX_VALUE_SET_ID,FLEX_SEGMENT_VALUE,KCA_SEQ_ID)
in 
(
select LEGAL_ENTITY_ID,FLEX_VALUE_SET_ID,FLEX_SEGMENT_VALUE,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.GL_LEGAL_ENTITIES_BSVS
group by LEGAL_ENTITY_ID,FLEX_VALUE_SET_ID,FLEX_SEGMENT_VALUE
) 
and kca_operation= 'DELETE'
);
commit;

end;

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'gl_legal_entities_bsvs';

commit;