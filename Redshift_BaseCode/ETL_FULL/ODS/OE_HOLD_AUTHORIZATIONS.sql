/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full incremental approach for ODS.
# File Version: KPI v1.0
*/
begin;
drop table if exists bec_ods.OE_HOLD_AUTHORIZATIONS;

CREATE TABLE IF NOT EXISTS bec_ods.OE_HOLD_AUTHORIZATIONS
(
	hold_id NUMERIC(15,0)   ENCODE az64
	,responsibility_id NUMERIC(15,0)   ENCODE az64
	,application_id NUMERIC(15,0)   ENCODE az64
	,authorized_action_code VARCHAR(30)   ENCODE lzo
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,start_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,context VARCHAR(30)   ENCODE lzo
	,attribute1 VARCHAR(240)   ENCODE lzo
	,attribute2 VARCHAR(240)   ENCODE lzo
	,attribute3 VARCHAR(240)   ENCODE lzo
	,attribute4 VARCHAR(240)   ENCODE lzo
	,attribute5 VARCHAR(240)   ENCODE lzo
	,attribute6 VARCHAR(240)   ENCODE lzo
	,attribute7 VARCHAR(240)   ENCODE lzo
	,attribute8 VARCHAR(240)   ENCODE lzo
	,attribute9 VARCHAR(240)   ENCODE lzo
	,attribute10 VARCHAR(240)   ENCODE lzo
	,attribute11 VARCHAR(240)   ENCODE lzo
	,attribute12 VARCHAR(240)   ENCODE lzo
	,attribute13 VARCHAR(240)   ENCODE lzo
	,attribute14 VARCHAR(240)   ENCODE lzo
	,attribute15 VARCHAR(240)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
	,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;

insert into bec_ods.OE_HOLD_AUTHORIZATIONS
(
	hold_id,
	responsibility_id,
	application_id,
	authorized_action_code,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	start_date_active,
	end_date_active,
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
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
(
select 
	hold_id,
	responsibility_id,
	application_id,
	authorized_action_code,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	start_date_active,
	end_date_active,
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
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID 
	,kca_seq_date
from bec_ods_stg.OE_HOLD_AUTHORIZATIONS
);

end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='oe_hold_authorizations'; 

commit;