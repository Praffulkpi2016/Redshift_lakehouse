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

truncate table bec_ods_stg.MTL_GENERIC_DISPOSITIONS;

insert into	bec_ods_stg.MTL_GENERIC_DISPOSITIONS
   (
	disposition_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	description,
	disable_date,
	effective_date,
	distribution_account,
	segment1,
	segment2,
	segment3,
	segment4,
	segment5,
	segment6,
	segment7,
	segment8,
	segment9,
	segment10,
	segment11,
	segment12,
	segment13,
	segment14,
	segment15,
	segment16,
	segment17,
	segment18,
	segment19,
	segment20,
	summary_flag,
	enabled_flag,
	start_date_active,
	end_date_active,
	attribute_category,
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
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	--ZD_EDITION_NAME,
	--ZD_SYNC,
	kca_operation,
	kca_seq_id,
	kca_seq_date)
(
	select		
	disposition_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	description,
	disable_date,
	effective_date,
	distribution_account,
	segment1,
	segment2,
	segment3,
	segment4,
	segment5,
	segment6,
	segment7,
	segment8,
	segment9,
	segment10,
	segment11,
	segment12,
	segment13,
	segment14,
	segment15,
	segment16,
	segment17,
	segment18,
	segment19,
	segment20,
	summary_flag,
	enabled_flag,
	start_date_active,
	end_date_active,
	attribute_category,
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
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	--ZD_EDITION_NAME,
	--ZD_SYNC,
	kca_operation,
	kca_seq_id ,
	kca_seq_date from bec_raw_dl_ext.MTL_GENERIC_DISPOSITIONS
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (DISPOSITION_ID,ORGANIZATION_ID,kca_seq_id) in 
	(select DISPOSITION_ID,ORGANIZATION_ID,max(kca_seq_id) from bec_raw_dl_ext.MTL_GENERIC_DISPOSITIONS 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by DISPOSITION_ID,ORGANIZATION_ID)
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'mtl_generic_dispositions')
		 
            )	
);
end;