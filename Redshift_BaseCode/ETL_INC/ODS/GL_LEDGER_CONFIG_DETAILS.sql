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

delete from bec_ods.GL_LEDGER_CONFIG_DETAILS
where (CONFIGURATION_ID, OBJECT_ID, OBJECT_TYPE_CODE, SETUP_STEP_CODE)  in
(
select stg.CONFIGURATION_ID, stg.OBJECT_ID, stg.OBJECT_TYPE_CODE, stg.SETUP_STEP_CODE from bec_ods.GL_LEDGER_CONFIG_DETAILS ods,
bec_ods_stg.GL_LEDGER_CONFIG_DETAILS stg
where ods.CONFIGURATION_ID = stg.CONFIGURATION_ID and
	  ods.OBJECT_ID = stg.OBJECT_ID and 
	  ods.OBJECT_TYPE_CODE = stg.OBJECT_TYPE_CODE and 
	  ods.SETUP_STEP_CODE = stg.SETUP_STEP_CODE   and 
	  stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.GL_LEDGER_CONFIG_DETAILS
       (
	configuration_id,
	object_type_code,
	object_id,
	object_name,
	setup_step_code,
	next_action_code,
	status_code,
	created_by,
	last_update_login,
	last_update_date,
	last_updated_by,
	creation_date,
    kca_operation,
	IS_DELETED_FLG,
    kca_seq_id,
	kca_seq_date
	)	
(
	select
	configuration_id,
	object_type_code,
	object_id,
	object_name,
	setup_step_code,
	next_action_code,
	status_code,
	created_by,
	last_update_login,
	last_update_date,
	last_updated_by,
	creation_date,
    kca_operation,
    'N' AS IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
	from bec_ods_stg.GL_LEDGER_CONFIG_DETAILS
	where kca_operation IN ('INSERT','UPDATE') 
	and (CONFIGURATION_ID, OBJECT_ID, OBJECT_TYPE_CODE, SETUP_STEP_CODE ,kca_seq_id) in 
	(select CONFIGURATION_ID, OBJECT_ID, OBJECT_TYPE_CODE, SETUP_STEP_CODE,max(kca_seq_id) from bec_ods_stg.GL_LEDGER_CONFIG_DETAILS 
     where kca_operation IN ('INSERT','UPDATE')
     group by CONFIGURATION_ID, OBJECT_ID, OBJECT_TYPE_CODE, SETUP_STEP_CODE )
);

commit;

 
-- Soft delete
update bec_ods.GL_LEDGER_CONFIG_DETAILS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.GL_LEDGER_CONFIG_DETAILS set IS_DELETED_FLG = 'Y'
where (CONFIGURATION_ID, OBJECT_ID, OBJECT_TYPE_CODE, SETUP_STEP_CODE)  in
(
select CONFIGURATION_ID, OBJECT_ID, OBJECT_TYPE_CODE, SETUP_STEP_CODE from bec_raw_dl_ext.GL_LEDGER_CONFIG_DETAILS
where (CONFIGURATION_ID, OBJECT_ID, OBJECT_TYPE_CODE, SETUP_STEP_CODE,KCA_SEQ_ID)
in 
(
select CONFIGURATION_ID, OBJECT_ID, OBJECT_TYPE_CODE, SETUP_STEP_CODE,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.GL_LEDGER_CONFIG_DETAILS
group by CONFIGURATION_ID, OBJECT_ID, OBJECT_TYPE_CODE, SETUP_STEP_CODE
) 
and kca_operation= 'DELETE'
);
commit;
end;
 

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'gl_ledger_config_details';

commit;