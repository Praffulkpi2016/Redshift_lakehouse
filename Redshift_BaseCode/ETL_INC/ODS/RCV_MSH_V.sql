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


TRUNCATE TABLE bec_ods.RCV_MSH_V; 
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
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
    FROM
        bec_ods_stg.RCV_MSH_V;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'rcv_msh_v';