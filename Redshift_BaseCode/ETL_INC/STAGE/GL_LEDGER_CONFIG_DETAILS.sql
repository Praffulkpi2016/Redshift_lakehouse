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

truncate table bec_ods_stg.GL_LEDGER_CONFIG_DETAILS;

insert into	bec_ods_stg.GL_LEDGER_CONFIG_DETAILS
   (configuration_id,
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
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
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
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
	from bec_raw_dl_ext.GL_LEDGER_CONFIG_DETAILS
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (CONFIGURATION_ID, OBJECT_ID, OBJECT_TYPE_CODE, SETUP_STEP_CODE,kca_seq_id) in 
	(select  CONFIGURATION_ID, OBJECT_ID, OBJECT_TYPE_CODE, SETUP_STEP_CODE,max(kca_seq_id) from bec_raw_dl_ext.GL_LEDGER_CONFIG_DETAILS 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by CONFIGURATION_ID, OBJECT_ID, OBJECT_TYPE_CODE, SETUP_STEP_CODE )
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'gl_ledger_config_details')
		 
            )
);
end;