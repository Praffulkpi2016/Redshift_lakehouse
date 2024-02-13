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

truncate table bec_ods_stg.GL_LEDGER_NORM_SEG_VALS;

insert into	bec_ods_stg.GL_LEDGER_NORM_SEG_VALS
   (
	ledger_id,
	segment_type_code,
	segment_value,
	segment_value_type_code,
	record_id,
	last_update_date,
	last_updated_by,
	last_update_login,
	creation_date,
	created_by,
	start_date,
	end_date,
	status_code,
	request_id,
	legal_entity_id,
	sla_sequencing_flag,
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
	kca_seq_id,
	kca_seq_date)
(
	select		
	ledger_id,
	segment_type_code,
	segment_value,
	segment_value_type_code,
	record_id,
	last_update_date,
	last_updated_by,
	last_update_login,
	creation_date,
	created_by,
	start_date,
	end_date,
	status_code,
	request_id,
	legal_entity_id,
	sla_sequencing_flag,
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
	kca_seq_id,
	kca_seq_date from bec_raw_dl_ext.GL_LEDGER_NORM_SEG_VALS
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (RECORD_ID,kca_seq_id) in 
	(select RECORD_ID,max(kca_seq_id) from bec_raw_dl_ext.GL_LEDGER_NORM_SEG_VALS 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by RECORD_ID)
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'gl_ledger_norm_seg_vals')
		 
            )
);
end;