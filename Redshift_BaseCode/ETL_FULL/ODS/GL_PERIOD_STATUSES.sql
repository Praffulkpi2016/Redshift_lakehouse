/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS.
# File Version: KPI v1.0
*/
begin;

DROP TABLE if exists bec_ods.GL_PERIOD_STATUSES;
CREATE TABLE IF NOT EXISTS bec_ods.GL_PERIOD_STATUSES
(
	application_id NUMERIC(15,0)   ENCODE az64
	,set_of_books_id NUMERIC(15,0)   ENCODE az64
	,period_name VARCHAR(15)   ENCODE lzo
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,closing_status VARCHAR(1)   ENCODE lzo
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,year_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,quarter_num NUMERIC(15,0)   ENCODE az64
	,quarter_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,period_type VARCHAR(15)   ENCODE lzo
	,period_year NUMERIC(15,0)   ENCODE az64
	,effective_period_num NUMERIC(15,0)   ENCODE az64
	,period_num NUMERIC(15,0)   ENCODE az64
	,adjustment_period_flag VARCHAR(1)   ENCODE lzo
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,elimination_confirmed_flag VARCHAR(1)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,context VARCHAR(150)   ENCODE lzo
	,chronological_seq_status_code VARCHAR(1)   ENCODE lzo
	,ledger_id NUMERIC(15,0)   ENCODE az64
	,migration_status_code VARCHAR(30)   ENCODE lzo
	,track_bc_ytd_flag VARCHAR(1)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO;
insert
	into
	bec_ods.gl_period_statuses
(application_id,
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
 IS_DELETED_FLG,
 kca_seq_id,
	kca_seq_date)
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
'N' as IS_DELETED_FLG,
cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date 
from
	bec_ods_stg.gl_period_statuses;
	
end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'gl_period_statuses';
	
commit;
	 