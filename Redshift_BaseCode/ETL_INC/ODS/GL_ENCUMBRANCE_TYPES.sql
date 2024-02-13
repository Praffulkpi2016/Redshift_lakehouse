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

delete from bec_ods.GL_ENCUMBRANCE_TYPES
where (ENCUMBRANCE_TYPE_ID) in (
select stg.ENCUMBRANCE_TYPE_ID
from bec_ods.GL_ENCUMBRANCE_TYPES ods, bec_ods_stg.GL_ENCUMBRANCE_TYPES stg
where 
ods.ENCUMBRANCE_TYPE_ID = stg.ENCUMBRANCE_TYPE_ID  
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.GL_ENCUMBRANCE_TYPES
       (
	encumbrance_type_id,
	encumbrance_type,
	enabled_flag,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	description,
	encumbrance_type_key,
	zd_edition_name,
	zd_sync,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
    SELECT
	encumbrance_type_id,
	encumbrance_type,
	enabled_flag,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	description,
	encumbrance_type_key,
	zd_edition_name,
	zd_sync,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
	from bec_ods_stg.GL_ENCUMBRANCE_TYPES
	where kca_operation IN ('INSERT','UPDATE') 
	and (ENCUMBRANCE_TYPE_ID,kca_seq_id) in 
	(select ENCUMBRANCE_TYPE_ID,max(kca_seq_id) from bec_ods_stg.GL_ENCUMBRANCE_TYPES 
     where kca_operation IN ('INSERT','UPDATE')
     group by ENCUMBRANCE_TYPE_ID);

commit;

 

-- Soft delete
update bec_ods.GL_ENCUMBRANCE_TYPES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.GL_ENCUMBRANCE_TYPES set IS_DELETED_FLG = 'Y'
where (ENCUMBRANCE_TYPE_ID)  in
(
select ENCUMBRANCE_TYPE_ID from bec_raw_dl_ext.GL_ENCUMBRANCE_TYPES
where (ENCUMBRANCE_TYPE_ID,KCA_SEQ_ID)
in 
(
select ENCUMBRANCE_TYPE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.GL_ENCUMBRANCE_TYPES
group by ENCUMBRANCE_TYPE_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'gl_encumbrance_types';

commit;