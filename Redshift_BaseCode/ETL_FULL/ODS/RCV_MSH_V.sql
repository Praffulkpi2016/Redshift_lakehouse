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

DROP TABLE if exists bec_ods.RCV_MSH_V;

CREATE TABLE IF NOT EXISTS bec_ods.RCV_MSH_V
(
	shipment_header_id NUMERIC(15,0)   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,ussgl_transaction_code VARCHAR(30)   ENCODE lzo
	,government_context VARCHAR(30)   ENCODE lzo
	,comments VARCHAR(720)   ENCODE lzo
	,bill_of_lading VARCHAR(25)   ENCODE lzo
	,expected_receipt_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,freight_carrier_code VARCHAR(25)   ENCODE lzo
	,from_organization_name VARCHAR(240)   ENCODE lzo
	,num_of_containers NUMERIC(28,10)   ENCODE az64
	,from_organization_id NUMERIC(15,0)   ENCODE az64
	,vendor_name VARCHAR(240)   ENCODE lzo
	,vendor_site VARCHAR(45)   ENCODE lzo
	,packing_slip VARCHAR(25)   ENCODE lzo
	,employee_id NUMERIC(9,0)   ENCODE az64
	,receipt_num VARCHAR(30)   ENCODE lzo
	,receipt_source_code VARCHAR(25)   ENCODE lzo
	,shipment_num VARCHAR(30)   ENCODE lzo
	,shipped_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,ship_to_location VARCHAR(60)   ENCODE lzo
	,ship_to_location_id NUMERIC(15,0)   ENCODE az64
	,waybill_airbill_num VARCHAR(40)   ENCODE lzo
	,asn_type VARCHAR(25)   ENCODE lzo
	,attribute_category VARCHAR(30)   ENCODE lzo
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
	,vendor_id NUMERIC(15,0)   ENCODE az64
	,ship_to_org_id NUMERIC(15,0)   ENCODE az64
	,vendor_site_id NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.RCV_MSH_V (
    shipment_header_id,
	last_updated_by,
	last_update_date,
	last_update_login,
	created_by,
	creation_date,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	ussgl_transaction_code,
	government_context,
	comments,
	bill_of_lading,
	expected_receipt_date,
	freight_carrier_code,
	from_organization_name,
	num_of_containers,
	from_organization_id,
	vendor_name,
	vendor_site,
	packing_slip,
	employee_id,
	receipt_num,
	receipt_source_code,
	shipment_num,
	shipped_date,
	ship_to_location,
	ship_to_location_id,
	waybill_airbill_num,
	asn_type,
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
	vendor_id,
	ship_to_org_id,
	vendor_site_id,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    SELECT
    shipment_header_id,
	last_updated_by,
	last_update_date,
	last_update_login,
	created_by,
	creation_date,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	ussgl_transaction_code,
	government_context,
	comments,
	bill_of_lading,
	expected_receipt_date,
	freight_carrier_code,
	from_organization_name,
	num_of_containers,
	from_organization_id,
	vendor_name,
	vendor_site,
	packing_slip,
	employee_id,
	receipt_num,
	receipt_source_code,
	shipment_num,
	shipped_date,
	ship_to_location,
	ship_to_location_id,
	waybill_airbill_num,
	asn_type,
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
	vendor_id,
	ship_to_org_id,
	vendor_site_id,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
    FROM
        bec_ods_stg.RCV_MSH_V;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'rcv_msh_v';
	
COMMIT;