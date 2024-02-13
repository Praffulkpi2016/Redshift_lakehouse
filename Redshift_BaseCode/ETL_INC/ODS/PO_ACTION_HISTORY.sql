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

delete from bec_ods.PO_ACTION_HISTORY 
where (OBJECT_ID,OBJECT_TYPE_CODE,OBJECT_SUB_TYPE_CODE,SEQUENCE_NUM) in (
select stg.OBJECT_ID,stg.OBJECT_TYPE_CODE,stg.OBJECT_SUB_TYPE_CODE,stg.SEQUENCE_NUM 
from bec_ods.PO_ACTION_HISTORY  ods, bec_ods_stg.PO_ACTION_HISTORY  stg
where 
ods.OBJECT_ID = stg.OBJECT_ID 
and ods.OBJECT_TYPE_CODE = stg.OBJECT_TYPE_CODE 
and ods.OBJECT_SUB_TYPE_CODE = stg.OBJECT_SUB_TYPE_CODE
and ods.SEQUENCE_NUM = stg.SEQUENCE_NUM 
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

INSERT INTO bec_ods.PO_ACTION_HISTORY  (
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
			is_deleted_flg,
			kca_seq_id
			,kca_seq_date
)
(    SELECT
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
			'N' as IS_DELETED_FLG,
			cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,  
			KCA_SEQ_DATE
    FROM
        bec_ods_stg.PO_ACTION_HISTORY 
	where kca_operation in ('INSERT','UPDATE') 
	and (OBJECT_ID,OBJECT_TYPE_CODE,OBJECT_SUB_TYPE_CODE,SEQUENCE_NUM,kca_seq_id) in 
	(select OBJECT_ID,OBJECT_TYPE_CODE,OBJECT_SUB_TYPE_CODE,SEQUENCE_NUM,max(kca_seq_id) from bec_ods_stg.PO_ACTION_HISTORY  
     where kca_operation in ('INSERT','UPDATE')
     group by OBJECT_ID,OBJECT_TYPE_CODE,OBJECT_SUB_TYPE_CODE,SEQUENCE_NUM)
);

commit;



-- Soft delete
update bec_ods.PO_ACTION_HISTORY set IS_DELETED_FLG = 'N';
commit;
update bec_ods.PO_ACTION_HISTORY set IS_DELETED_FLG = 'Y'
where (OBJECT_ID,OBJECT_TYPE_CODE,OBJECT_SUB_TYPE_CODE,SEQUENCE_NUM)  in
(
select OBJECT_ID,OBJECT_TYPE_CODE,OBJECT_SUB_TYPE_CODE,SEQUENCE_NUM from bec_raw_dl_ext.PO_ACTION_HISTORY
where (OBJECT_ID,OBJECT_TYPE_CODE,OBJECT_SUB_TYPE_CODE,SEQUENCE_NUM,KCA_SEQ_ID)
in 
(
select OBJECT_ID,OBJECT_TYPE_CODE,OBJECT_SUB_TYPE_CODE,SEQUENCE_NUM,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.PO_ACTION_HISTORY
group by OBJECT_ID,OBJECT_TYPE_CODE,OBJECT_SUB_TYPE_CODE,SEQUENCE_NUM
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'po_action_history ';

commit;