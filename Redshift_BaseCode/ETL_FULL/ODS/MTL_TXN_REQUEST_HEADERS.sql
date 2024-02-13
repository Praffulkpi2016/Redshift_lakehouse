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

DROP TABLE if exists bec_ods.MTL_TXN_REQUEST_HEADERS;

CREATE TABLE IF NOT EXISTS bec_ods.MTL_TXN_REQUEST_HEADERS
(
	header_id NUMERIC(15,0)   ENCODE az64
	,request_number VARCHAR(30)   ENCODE lzo
	,transaction_type_id NUMERIC(15,0)   ENCODE az64
	,move_order_type NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,description VARCHAR(240)   ENCODE lzo
	,date_required TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,from_subinventory_code VARCHAR(10)   ENCODE lzo
	,to_subinventory_code VARCHAR(10)   ENCODE lzo
	,to_account_id NUMERIC(15,0)   ENCODE az64
	,header_status NUMERIC(15,0)   ENCODE az64
	,status_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,grouping_rule_id NUMERIC(15,0)   ENCODE az64
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
	,attribute_category VARCHAR(30)   ENCODE lzo
	,ship_to_location_id NUMERIC(15,0)   ENCODE az64
	,freight_code VARCHAR(25)   ENCODE lzo
	,shipment_method VARCHAR(30)   ENCODE lzo
	,auto_receipt_flag VARCHAR(1)   ENCODE lzo
	,reference_id NUMERIC(15,0)   ENCODE az64
	,reference_detail_id NUMERIC(15,0)   ENCODE az64
	,assignment_id VARCHAR(240)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MTL_TXN_REQUEST_HEADERS (
	header_id,
	request_number,
	transaction_type_id,
	move_order_type,
	organization_id,
	description,
	date_required,
	from_subinventory_code,
	to_subinventory_code,
	to_account_id,
	header_status,
	status_date,
	last_updated_by,
	last_update_login,
	last_update_date,
	created_by,
	creation_date,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	grouping_rule_id,
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
	attribute_category,
	ship_to_location_id,
	freight_code,
	shipment_method,
	auto_receipt_flag,
	reference_id,
	reference_detail_id,
	assignment_id,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date)
    SELECT
	header_id,
	request_number,
	transaction_type_id,
	move_order_type,
	organization_id,
	description,
	date_required,
	from_subinventory_code,
	to_subinventory_code,
	to_account_id,
	header_status,
	status_date,
	last_updated_by,
	last_update_login,
	last_update_date,
	created_by,
	creation_date,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	grouping_rule_id,
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
	attribute_category,
	ship_to_location_id,
	freight_code,
	shipment_method,
	auto_receipt_flag,
	reference_id,
	reference_detail_id,
	assignment_id,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.MTL_TXN_REQUEST_HEADERS;
end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'mtl_txn_request_headers';
	
commit;