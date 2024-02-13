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

delete from bec_ods.GL_LE_VALUE_SETS
where (nvl(LEGAL_ENTITY_ID,0),nvl(FLEX_VALUE_SET_ID,0)) in (
select nvl(stg.LEGAL_ENTITY_ID,0) as LEGAL_ENTITY_ID,nvl(stg.FLEX_VALUE_SET_ID,0) as FLEX_VALUE_SET_ID from bec_ods.GL_LE_VALUE_SETS ods, bec_ods_stg.GL_LE_VALUE_SETS stg
where nvl(ods.LEGAL_ENTITY_ID,0) = nvl(stg.LEGAL_ENTITY_ID,0) and nvl(ods.FLEX_VALUE_SET_ID,0) = nvl(stg.FLEX_VALUE_SET_ID,0) and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.GL_LE_VALUE_SETS
       (	
		LEGAL_ENTITY_ID, 
		FLEX_VALUE_SET_ID, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		LAST_UPDATE_LOGIN, 
		CREATION_DATE, 
		CREATED_BY,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
	kca_seq_date)	
(
	select
		LEGAL_ENTITY_ID, 
		FLEX_VALUE_SET_ID, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		LAST_UPDATE_LOGIN, 
		CREATION_DATE, 
		CREATED_BY,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
	from bec_ods_stg.GL_LE_VALUE_SETS
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(LEGAL_ENTITY_ID,0),nvl(FLEX_VALUE_SET_ID,0),kca_seq_id) in 
	(select nvl(LEGAL_ENTITY_ID,0) as LEGAL_ENTITY_ID,nvl(FLEX_VALUE_SET_ID,0) as FLEX_VALUE_SET_ID,max(kca_seq_id) from bec_ods_stg.GL_LE_VALUE_SETS 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(LEGAL_ENTITY_ID,0),nvl(FLEX_VALUE_SET_ID,0))
);

commit;

 

-- Soft delete
update bec_ods.GL_LE_VALUE_SETS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.GL_LE_VALUE_SETS set IS_DELETED_FLG = 'Y'
where (nvl(LEGAL_ENTITY_ID,0),nvl(FLEX_VALUE_SET_ID,0))  in
(
select nvl(LEGAL_ENTITY_ID,0),nvl(FLEX_VALUE_SET_ID,0) from bec_raw_dl_ext.GL_LE_VALUE_SETS
where (nvl(LEGAL_ENTITY_ID,0),nvl(FLEX_VALUE_SET_ID,0),KCA_SEQ_ID)
in 
(
select nvl(LEGAL_ENTITY_ID,0),nvl(FLEX_VALUE_SET_ID,0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.GL_LE_VALUE_SETS
group by nvl(LEGAL_ENTITY_ID,0),nvl(FLEX_VALUE_SET_ID,0)
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'gl_le_value_sets';

commit;