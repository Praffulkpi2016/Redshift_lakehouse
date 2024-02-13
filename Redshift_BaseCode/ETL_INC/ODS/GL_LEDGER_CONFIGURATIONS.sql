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

delete from bec_ods.GL_LEDGER_CONFIGURATIONS
where ( CONFIGURATION_ID ) in
(
select  stg.CONFIGURATION_ID  from bec_ods.GL_LEDGER_CONFIGURATIONS ods,
bec_ods_stg.GL_LEDGER_CONFIGURATIONS stg
where  ods.CONFIGURATION_ID =  stg.CONFIGURATION_ID  and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.GL_LEDGER_CONFIGURATIONS
       (
	configuration_id,
	"name",
	description,
	completion_status_code,
	acctg_environment_code,
	creation_date,
	created_by,
	last_update_login,
	last_update_date,
	last_updated_by,
	context,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
    kca_operation,
	IS_DELETED_FLG,
    kca_seq_id,
	kca_seq_date
	)	
(
	select
	configuration_id,
	"name",
	description,
	completion_status_code,
	acctg_environment_code,
	creation_date,
	created_by,
	last_update_login,
	last_update_date,
	last_updated_by,
	context,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
    kca_operation,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
	from bec_ods_stg.GL_LEDGER_CONFIGURATIONS
	where kca_operation IN ('INSERT','UPDATE') 
	and ( CONFIGURATION_ID ,kca_seq_id) in 
	(select  CONFIGURATION_ID ,max(kca_seq_id) from bec_ods_stg.GL_LEDGER_CONFIGURATIONS 
     where kca_operation IN ('INSERT','UPDATE')
     group by  CONFIGURATION_ID )
);

commit;
 
-- Soft delete
update bec_ods.GL_LEDGER_CONFIGURATIONS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.GL_LEDGER_CONFIGURATIONS set IS_DELETED_FLG = 'Y'
where (CONFIGURATION_ID)  in
(
select CONFIGURATION_ID from bec_raw_dl_ext.GL_LEDGER_CONFIGURATIONS
where (CONFIGURATION_ID,KCA_SEQ_ID)
in 
(
select CONFIGURATION_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.GL_LEDGER_CONFIGURATIONS
group by CONFIGURATION_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;
 

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'gl_ledger_configurations';

commit;