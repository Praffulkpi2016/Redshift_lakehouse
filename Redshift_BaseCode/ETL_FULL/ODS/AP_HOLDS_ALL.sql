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

drop table if exists bec_ods.ap_holds_all;

CREATE TABLE IF NOT EXISTS bec_ods.ap_holds_all
(
	invoice_id NUMERIC(15,0)   ENCODE az64
	,line_location_id NUMERIC(15,0)   ENCODE az64
	,hold_lookup_code VARCHAR(25)   ENCODE lzo
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,held_by NUMERIC(15,0)   ENCODE az64
	,hold_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,hold_reason VARCHAR(240)   ENCODE lzo
	,release_lookup_code VARCHAR(25)   ENCODE lzo
	,release_reason VARCHAR(240)   ENCODE lzo
	,status_flag VARCHAR(25)   ENCODE lzo
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,attribute_category VARCHAR(150)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,attribute6 VARCHAR(150)   ENCODE lzo
	,attribute7 VARCHAR(150)   ENCODE lzo
	,attribute8 VARCHAR(150)   ENCODE lzo
	,attribute9 VARCHAR(150)   ENCODE lzo
	,attribute10 VARCHAR(150)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,responsibility_id NUMERIC(15,0)   ENCODE az64
	,rcv_transaction_id NUMERIC(15,0)   ENCODE az64
	,hold_details VARCHAR(2000)   ENCODE lzo
	,line_number NUMERIC(15,0)   ENCODE az64
	,hold_id NUMERIC(15,0)   ENCODE az64
	,wf_status VARCHAR(30)   ENCODE lzo
	,VALIDATION_REQUEST_ID  NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE AUTO;

insert
	into
	bec_ods.ap_holds_all
(invoice_id,
	line_location_id,
	hold_lookup_code,
	last_update_date,
	last_updated_by,
	held_by,
	hold_date,
	hold_reason,
	release_lookup_code,
	release_reason,
	status_flag,
	last_update_login,
	creation_date,
	created_by,
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
	org_id,
	responsibility_id,
	rcv_transaction_id,
	hold_details,
	line_number,
	hold_id,
	wf_status,
	validation_request_id,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
	)
select
	invoice_id,
	line_location_id,
	hold_lookup_code,
	last_update_date,
	last_updated_by,
	held_by,
	hold_date,
	hold_reason,
	release_lookup_code,
	release_reason,
	status_flag,
	last_update_login,
	creation_date,
	created_by,
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
	org_id,
	responsibility_id,
	rcv_transaction_id,
	hold_details,
	line_number,
	hold_id,
	wf_status,
	VALIDATION_REQUEST_ID,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from
	bec_ods_stg.ap_holds_all;
	
end;	

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ap_holds_all';
	
commit;