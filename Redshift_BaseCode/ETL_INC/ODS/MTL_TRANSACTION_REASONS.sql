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

delete from bec_ods.MTL_TRANSACTION_REASONS
where (reason_id ) in (
select stg.reason_id  
from bec_ods.MTL_TRANSACTION_REASONS ods, bec_ods_stg.MTL_TRANSACTION_REASONS stg
where 
ods.reason_id = stg.reason_id 


and stg.kca_operation IN ('INSERT','UPDATE')
);
commit;

-- Insert records

insert into	bec_ods.MTL_TRANSACTION_REASONS
       (
	reason_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	reason_name,
	description,
	disable_date,
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
	attribute_category,
	workflow_name,
	workflow_display_name,
	workflow_process,
	workflow_display_process,
	reason_type,
	reason_type_display,
	reason_context_code,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date)
	(
	select
reason_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	reason_name,
	description,
	disable_date,
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
	attribute_category,
	workflow_name,
	workflow_display_name,
	workflow_process,
	workflow_display_process,
	reason_type,
	reason_type_display,
	reason_context_code,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
	from bec_ods_stg.MTL_TRANSACTION_REASONS
	where kca_operation IN ('INSERT','UPDATE') 
	and (reason_id ,kca_seq_id) in 
	(select reason_id,max(kca_seq_id) from bec_ods_stg.MTL_TRANSACTION_REASONS 
     where kca_operation IN ('INSERT','UPDATE')
     group by reason_id  )
);
commit;
-- Soft delete
update bec_ods.MTL_TRANSACTION_REASONS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_TRANSACTION_REASONS set IS_DELETED_FLG = 'Y'
where (reason_id)  in
(
select reason_id from bec_raw_dl_ext.MTL_TRANSACTION_REASONS
where (reason_id,KCA_SEQ_ID)
in 
(
select reason_id,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_TRANSACTION_REASONS
group by reason_id
) 
and kca_operation= 'DELETE'
);
commit;

end;


update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'mtl_transaction_reasons';

commit;