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

truncate table bec_ods_stg.MTL_TRANSACTION_REASONS;
INSERT INTO bec_ods_stg.MTL_TRANSACTION_REASONS (
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
	kca_seq_id,
	kca_seq_date
)

(
	 SELECT
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
    kca_seq_id,
	kca_seq_date from bec_raw_dl_ext.MTL_TRANSACTION_REASONS
	where kca_operation != 'DELETE'   and nvl(kca_seq_id,'')!= ''
	and (reason_id ,kca_seq_id) in 
	(select reason_id  ,max(kca_seq_id) from bec_raw_dl_ext.MTL_TRANSACTION_REASONS 
     where kca_operation != 'DELETE'   
     group by reason_id  )
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'mtl_transaction_reasons')
		 
            )	
);

end;