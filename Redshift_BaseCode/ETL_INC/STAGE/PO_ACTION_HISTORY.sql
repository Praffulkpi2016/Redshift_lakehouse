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

truncate table bec_ods_stg.PO_ACTION_HISTORY ;

insert into	bec_ods_stg.PO_ACTION_HISTORY 
   (
			object_id,
			object_type_code,
			object_sub_type_code,
			sequence_num,
			last_update_date,
			last_updated_by,
			creation_date,
			created_by,
			action_code,
			action_date,
			employee_id,
			approval_path_id,
			note,
			object_revision_num,
			offline_code,
			last_update_login,
			request_id,
			program_application_id,
			program_id,
			program_update_date,
			program_date,
			approval_group_id,
			kca_operation,
			kca_seq_id
			,KCA_SEQ_DATE)
(
	select
			object_id,
			object_type_code,
			object_sub_type_code,
			sequence_num,
			last_update_date,
			last_updated_by,
			creation_date,
			created_by,
			action_code,
			action_date,
			employee_id,
			approval_path_id,
			note,
			object_revision_num,
			offline_code,
			last_update_login,
			request_id,
			program_application_id,
			program_id,
			program_update_date,
			program_date,
			approval_group_id,
			kca_operation,
			kca_seq_id 
			,KCA_SEQ_DATE
	from bec_raw_dl_ext.PO_ACTION_HISTORY 
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (OBJECT_ID,OBJECT_TYPE_CODE,OBJECT_SUB_TYPE_CODE,SEQUENCE_NUM,kca_seq_id) in 
	(select OBJECT_ID,OBJECT_TYPE_CODE,OBJECT_SUB_TYPE_CODE,SEQUENCE_NUM,max(kca_seq_id) from bec_raw_dl_ext.PO_ACTION_HISTORY  
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by OBJECT_ID,OBJECT_TYPE_CODE,OBJECT_SUB_TYPE_CODE,SEQUENCE_NUM)
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'po_action_history ')

            )	
);
end; 