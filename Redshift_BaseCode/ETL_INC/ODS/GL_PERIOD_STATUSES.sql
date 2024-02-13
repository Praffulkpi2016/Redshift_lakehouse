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

delete from bec_ods.gl_period_statuses
where (APPLICATION_ID, LEDGER_ID, PERIOD_NAME) in (
select STG.APPLICATION_ID, STG.LEDGER_ID, STG.PERIOD_NAME from bec_ods.gl_period_statuses ods, bec_ods_stg.gl_period_statuses stg
where ods.APPLICATION_ID = stg.APPLICATION_ID AND ods.LEDGER_ID = stg.LEDGER_ID AND ods.PERIOD_NAME = stg.PERIOD_NAME
and stg.kca_operation IN ('INSERT','UPDATE'));

commit;


-- Insert records

insert
	into
	bec_ods.GL_PERIOD_STATUSES
	(
		application_id,
	set_of_books_id,
	period_name,
	last_update_date,
	last_updated_by,
	closing_status,
	start_date,
	end_date,
	year_start_date,
	quarter_num,
	quarter_start_date,
	period_type,
	period_year,
	effective_period_num,
	period_num,
	adjustment_period_flag,
	creation_date,
	created_by,
	last_update_login,
	elimination_confirmed_flag,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	context,
	chronological_seq_status_code,
	ledger_id,
	migration_status_code,
	track_bc_ytd_flag,
	KCA_OPERATION,
IS_DELETED_FLG
,kca_seq_id,
	kca_seq_date
)
(
select
	application_id,
	set_of_books_id,
	period_name,
	last_update_date,
	last_updated_by,
	closing_status,
	start_date,
	end_date,
	year_start_date,
	quarter_num,
	quarter_start_date,
	period_type,
	period_year,
	effective_period_num,
	period_num,
	adjustment_period_flag,
	creation_date,
	created_by,
	last_update_login,
	elimination_confirmed_flag,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	context,
	chronological_seq_status_code,
	ledger_id,
	migration_status_code,
	track_bc_ytd_flag,
	KCA_OPERATION,
'N' AS IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from
	bec_ods_stg.gl_period_statuses
where kca_operation IN ('INSERT','UPDATE') and (APPLICATION_ID, LEDGER_ID, PERIOD_NAME,kca_seq_id) in 
(select APPLICATION_ID, LEDGER_ID, PERIOD_NAME,max(kca_seq_id) from bec_ods_stg.gl_period_statuses 
where kca_operation IN ('INSERT','UPDATE')
group by APPLICATION_ID, LEDGER_ID, PERIOD_NAME)


);
commit;

 
-- Soft delete
update bec_ods.gl_period_statuses set IS_DELETED_FLG = 'N';
commit;
update bec_ods.gl_period_statuses set IS_DELETED_FLG = 'Y'
where (APPLICATION_ID, LEDGER_ID, PERIOD_NAME)  in
(
select APPLICATION_ID, LEDGER_ID, PERIOD_NAME from bec_raw_dl_ext.gl_period_statuses
where (APPLICATION_ID, LEDGER_ID, PERIOD_NAME,KCA_SEQ_ID)
in 
(
select APPLICATION_ID, LEDGER_ID, PERIOD_NAME,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.gl_period_statuses
group by APPLICATION_ID, LEDGER_ID, PERIOD_NAME
) 
and kca_operation= 'DELETE'
);
commit;

end;



update
	bec_etl_ctrl.batch_ods_info
set load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'gl_period_statuses';

commit;