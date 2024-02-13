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

truncate table bec_ods_stg.GL_PERIOD_STATUSES;
	
	insert into	bec_ods_stg.GL_PERIOD_STATUSES
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
	KCA_OPERATION
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
	KCA_OPERATION
	,kca_seq_id,
	kca_seq_date
	from
		bec_raw_dl_ext.gl_period_statuses
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' and (APPLICATION_ID, LEDGER_ID, PERIOD_NAME,kca_seq_id) in 
(select APPLICATION_ID, LEDGER_ID, PERIOD_NAME,max(kca_seq_id) from bec_raw_dl_ext.gl_period_statuses 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
group by APPLICATION_ID, LEDGER_ID, PERIOD_NAME)
and 
( kca_seq_date > (
select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name = 'gl_period_statuses')
 
            )
);
end;