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

DROP TABLE if exists bec_ods.ORG_ACCT_PERIODS;

CREATE TABLE IF NOT EXISTS bec_ods.ORG_ACCT_PERIODS
(

    acct_period_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,period_set_name VARCHAR(15)   ENCODE lzo
	,period_year NUMERIC(15,0)   ENCODE az64
	,period_num NUMERIC(15,0)   ENCODE az64
	,period_name VARCHAR(15)   ENCODE lzo
	,description VARCHAR(50)   ENCODE lzo
	,period_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,schedule_close_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,period_close_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,open_flag VARCHAR(1)   ENCODE lzo
	,global_attribute_category VARCHAR(30)   ENCODE lzo
	,global_attribute1 VARCHAR(150)   ENCODE lzo
	,global_attribute2 VARCHAR(150)   ENCODE lzo
	,global_attribute3 VARCHAR(150)   ENCODE lzo
	,global_attribute4 VARCHAR(150)   ENCODE lzo
	,global_attribute5 VARCHAR(150)   ENCODE lzo
	,global_attribute6 VARCHAR(150)   ENCODE lzo
	,global_attribute7 VARCHAR(150)   ENCODE lzo
	,global_attribute8 VARCHAR(150)   ENCODE lzo
	,global_attribute9 VARCHAR(150)   ENCODE lzo
	,global_attribute10 VARCHAR(150)   ENCODE lzo
	,global_attribute11 VARCHAR(150)   ENCODE lzo
	,global_attribute12 VARCHAR(150)   ENCODE lzo
	,global_attribute13 VARCHAR(150)   ENCODE lzo
	,global_attribute14 VARCHAR(150)   ENCODE lzo
	,global_attribute15 VARCHAR(150)   ENCODE lzo
	,global_attribute16 VARCHAR(150)   ENCODE lzo
	,global_attribute17 VARCHAR(150)   ENCODE lzo
	,global_attribute18 VARCHAR(150)   ENCODE lzo
	,global_attribute19 VARCHAR(150)   ENCODE lzo
	,global_attribute20 VARCHAR(150)   ENCODE lzo
	,summarized_flag VARCHAR(1)   ENCODE lzo 
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

insert
	into
	bec_ods.ORG_ACCT_PERIODS (
    acct_period_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	period_set_name,
	period_year,
	period_num,
	period_name,
	description,
	period_start_date,
	schedule_close_date,
	period_close_date,
	open_flag,
	global_attribute_category,
	global_attribute1,
	global_attribute2,
	global_attribute3,
	global_attribute4,
	global_attribute5,
	global_attribute6,
	global_attribute7,
	global_attribute8,
	global_attribute9,
	global_attribute10,
	global_attribute11,
	global_attribute12,
	global_attribute13,
	global_attribute14,
	global_attribute15,
	global_attribute16,
	global_attribute17,
	global_attribute18,
	global_attribute19,
	global_attribute20,
	summarized_flag,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    select
	acct_period_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	period_set_name,
	period_year,
	period_num,
	period_name,
	description,
	period_start_date,
	schedule_close_date,
	period_close_date,
	open_flag,
	global_attribute_category,
	global_attribute1,
	global_attribute2,
	global_attribute3,
	global_attribute4,
	global_attribute5,
	global_attribute6,
	global_attribute7,
	global_attribute8,
	global_attribute9,
	global_attribute10,
	global_attribute11,
	global_attribute12,
	global_attribute13,
	global_attribute14,
	global_attribute15,
	global_attribute16,
	global_attribute17,
	global_attribute18,
	global_attribute19,
	global_attribute20,
	summarized_flag,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
	kca_seq_date
from
	bec_ods_stg.ORG_ACCT_PERIODS;
end;

update
	bec_etl_ctrl.batch_ods_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'org_acct_periods';

commit;