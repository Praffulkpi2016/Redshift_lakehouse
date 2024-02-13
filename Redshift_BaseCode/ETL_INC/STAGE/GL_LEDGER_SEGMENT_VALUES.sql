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

truncate table bec_ods_stg.GL_LEDGER_SEGMENT_VALUES;

insert into	bec_ods_stg.GL_LEDGER_SEGMENT_VALUES
   (
	ledger_id,
	segment_type_code,
	segment_value,
	parent_record_id,
	last_update_date,
	last_updated_by,
	last_update_login,
	creation_date,
	created_by,
	end_date,
	start_date,
	status_code,
	legal_entity_id,
	sla_sequencing_flag,
	kca_operation,
	kca_seq_id,
	kca_seq_date)
(
	select		
	ledger_id,
	segment_type_code,
	segment_value,
	parent_record_id,
	last_update_date,
	last_updated_by,
	last_update_login,
	creation_date,
	created_by,
	end_date,
	start_date,
	status_code,
	legal_entity_id,
	sla_sequencing_flag,
	kca_operation,
	kca_seq_id,
	kca_seq_date from bec_raw_dl_ext.GL_LEDGER_SEGMENT_VALUES
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (LEDGER_ID,SEGMENT_TYPE_CODE,SEGMENT_VALUE,kca_seq_id) in 
	(select LEDGER_ID,SEGMENT_TYPE_CODE,SEGMENT_VALUE,max(kca_seq_id) from bec_raw_dl_ext.GL_LEDGER_SEGMENT_VALUES 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by LEDGER_ID,SEGMENT_TYPE_CODE,SEGMENT_VALUE)
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'gl_ledger_segment_values')
		 
            )
);
end;