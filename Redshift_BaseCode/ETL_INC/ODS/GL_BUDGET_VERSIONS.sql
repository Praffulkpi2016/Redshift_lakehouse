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

delete from bec_ods.GL_BUDGET_VERSIONS
where (BUDGET_VERSION_ID ) in (
select stg.BUDGET_VERSION_ID  
from bec_ods.GL_BUDGET_VERSIONS ods, bec_ods_stg.GL_BUDGET_VERSIONS stg
where ods.BUDGET_VERSION_ID = stg.BUDGET_VERSION_ID  
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.GL_BUDGET_VERSIONS
    (budget_version_id,
	last_update_date,
	last_updated_by,
	budget_type,
	budget_name,
	version_num,
	status,
	date_opened,
	creation_date,
	created_by,
	last_update_login,
	description,
	date_active,
	date_archived,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	context,
	control_budget_version_id,
	igi_bud_nyc_flag,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(select
	budget_version_id,
	last_update_date,
	last_updated_by,
	budget_type,
	budget_name,
	version_num,
	status,
	date_opened,
	creation_date,
	created_by,
	last_update_login,
	description,
	date_active,
	date_archived,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	context,
	control_budget_version_id,
	igi_bud_nyc_flag,
	KCA_OPERATION,
	'N' AS IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from
	bec_ods_stg.GL_BUDGET_VERSIONS
where kca_operation IN ('INSERT','UPDATE') 
	and (BUDGET_VERSION_ID,KCA_SEQ_ID) in 
	(select BUDGET_VERSION_ID,max(KCA_SEQ_ID) from bec_ods_stg.GL_BUDGET_VERSIONS 
     where kca_operation IN ('INSERT','UPDATE')
     group by BUDGET_VERSION_ID)	
	);

commit;

 

-- Soft delete
update bec_ods.GL_BUDGET_VERSIONS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.GL_BUDGET_VERSIONS set IS_DELETED_FLG = 'Y'
where (BUDGET_VERSION_ID)  in
(
select BUDGET_VERSION_ID from bec_raw_dl_ext.GL_BUDGET_VERSIONS
where (BUDGET_VERSION_ID,KCA_SEQ_ID)
in 
(
select BUDGET_VERSION_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.GL_BUDGET_VERSIONS
group by BUDGET_VERSION_ID
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
	ods_table_name = 'gl_budget_versions';

commit;