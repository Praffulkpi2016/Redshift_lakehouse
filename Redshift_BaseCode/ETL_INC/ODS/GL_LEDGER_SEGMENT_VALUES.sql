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

delete from bec_ods.GL_LEDGER_SEGMENT_VALUES
where (LEDGER_ID,SEGMENT_TYPE_CODE,SEGMENT_VALUE) in (
select stg.LEDGER_ID,stg.SEGMENT_TYPE_CODE,stg.SEGMENT_VALUE
from bec_ods.GL_LEDGER_SEGMENT_VALUES ods, bec_ods_stg.GL_LEDGER_SEGMENT_VALUES stg
where 
ods.LEDGER_ID = stg.LEDGER_ID and 
ods.SEGMENT_TYPE_CODE = stg.SEGMENT_TYPE_CODE and 
ods.SEGMENT_VALUE = stg.SEGMENT_VALUE  
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.GL_LEDGER_SEGMENT_VALUES
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
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
    SELECT
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
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
	from bec_ods_stg.GL_LEDGER_SEGMENT_VALUES
	where kca_operation IN ('INSERT','UPDATE') 
	and (LEDGER_ID,SEGMENT_TYPE_CODE,SEGMENT_VALUE,kca_seq_id) in 
	(select LEDGER_ID,SEGMENT_TYPE_CODE,SEGMENT_VALUE,max(kca_seq_id) from bec_ods_stg.GL_LEDGER_SEGMENT_VALUES 
     where kca_operation IN ('INSERT','UPDATE')
     group by LEDGER_ID,SEGMENT_TYPE_CODE,SEGMENT_VALUE);

commit;

 

-- Soft delete
update bec_ods.GL_LEDGER_SEGMENT_VALUES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.GL_LEDGER_SEGMENT_VALUES set IS_DELETED_FLG = 'Y'
where (LEDGER_ID,SEGMENT_TYPE_CODE,SEGMENT_VALUE)  in
(
select LEDGER_ID,SEGMENT_TYPE_CODE,SEGMENT_VALUE from bec_raw_dl_ext.GL_LEDGER_SEGMENT_VALUES
where (LEDGER_ID,SEGMENT_TYPE_CODE,SEGMENT_VALUE,KCA_SEQ_ID)
in 
(
select LEDGER_ID,SEGMENT_TYPE_CODE,SEGMENT_VALUE,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.GL_LEDGER_SEGMENT_VALUES
group by LEDGER_ID,SEGMENT_TYPE_CODE,SEGMENT_VALUE
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'gl_ledger_segment_values';

commit;