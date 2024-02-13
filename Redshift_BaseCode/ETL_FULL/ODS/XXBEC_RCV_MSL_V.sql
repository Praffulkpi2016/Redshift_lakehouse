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

DROP TABLE if exists bec_ods.XXBEC_RCV_MSL_V;

CREATE TABLE IF NOT EXISTS bec_ods.XXBEC_RCV_MSL_V
(
	source_type VARCHAR(9)   ENCODE lzo
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
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
	,charge_account_id NUMERIC(15,0)   ENCODE az64
	,comments VARCHAR(4000)   ENCODE lzo
	,deliver_to_location_id NUMERIC(15,0)   ENCODE az64
	,deliver_to_person_id NUMERIC(15,0)   ENCODE az64
	,destination_context VARCHAR(30)   ENCODE lzo
	,destination_type_code VARCHAR(25)   ENCODE lzo
	,destination_type VARCHAR(80)   ENCODE lzo
	,employee_id NUMERIC(15,0)   ENCODE az64
	,from_organization_id NUMERIC(15,0)   ENCODE az64
	,item_description VARCHAR(240)   ENCODE lzo
	,item_id NUMERIC(15,0)   ENCODE az64
	,item_revision VARCHAR(3)   ENCODE lzo
	,hazard_class VARCHAR(40)   ENCODE lzo
	,un_number VARCHAR(25)   ENCODE lzo
	,line_num NUMERIC(15,0)   ENCODE az64
	,item_category_id NUMERIC(15,0)   ENCODE az64
	,locator_id NUMERIC(15,0)   ENCODE az64
	,need_by_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,packing_slip VARCHAR(25)   ENCODE lzo
	,quantity_received NUMERIC(15,0)   ENCODE az64
	,quantity_shipped NUMERIC(15,0)   ENCODE az64
	,requisition_line_id NUMERIC(15,0)   ENCODE az64
	,requisition_header_id NUMERIC(15,0)   ENCODE az64
	,order_num VARCHAR(20)   ENCODE lzo
	,order_line_num NUMERIC(15,0)   ENCODE az64
	,req_distribution_id NUMERIC(15,0)   ENCODE az64
	,shipment_header_id NUMERIC(15,0)   ENCODE az64
	,shipment_line_id NUMERIC(15,0)   ENCODE az64
	,shipment_line_status_code VARCHAR(25)   ENCODE lzo
	,source_document_code VARCHAR(25)   ENCODE lzo
	,source_document_type VARCHAR(80)   ENCODE lzo
	,to_organization_id NUMERIC(15,0)   ENCODE az64
	,to_subinventory VARCHAR(10)   ENCODE lzo
	,transfer_cost NUMERIC(15,0)   ENCODE az64
	,transportation_account_id NUMERIC(15,0)   ENCODE az64
	,transportation_cost NUMERIC(28,10)   ENCODE az64
	,unit_of_measure VARCHAR(25)   ENCODE lzo
	,uom_conversion_rate NUMERIC(28,10)   ENCODE az64
	,routing_header_id NUMERIC(15,0)   ENCODE az64
	,routing_name VARCHAR(80)   ENCODE lzo
	,reason_id NUMERIC(15,0)   ENCODE az64
	,reason_name VARCHAR(30)   ENCODE lzo
	,location_code VARCHAR(60)   ENCODE lzo
	,deliver_to_person VARCHAR(240)   ENCODE lzo
	,po_header_id NUMERIC(15,0)   ENCODE az64
	,po_line_id NUMERIC(15,0)   ENCODE az64
	,po_line_location_id NUMERIC(15,0)   ENCODE az64
	,po_release_id NUMERIC(15,0)   ENCODE az64
	,release_num NUMERIC(15,0)   ENCODE az64
	,vendor_name VARCHAR(240)   ENCODE lzo
	,vendor_site_code VARCHAR(45)   ENCODE lzo
	,ship_to_location_id NUMERIC(15,0)   ENCODE az64
	,primary_unit_of_measure VARCHAR(25)   ENCODE lzo
	,vendor_id NUMERIC(15,0)   ENCODE az64
	,bar_code_label VARCHAR(35)   ENCODE lzo
	,truck_num VARCHAR(35)   ENCODE lzo
	,container_num VARCHAR(35)   ENCODE lzo
	,vendor_lot_num VARCHAR(30)   ENCODE lzo
	,secondary_quantity_received NUMERIC(15,0)   ENCODE az64
	,secondary_quantity_shipped NUMERIC(15,0)   ENCODE az64
	,secondary_unit_of_measure VARCHAR(25)   ENCODE lzo
	,qc_grade VARCHAR(150)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.XXBEC_RCV_MSL_V (
 source_type,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
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
	charge_account_id,
	comments,
	deliver_to_location_id,
	deliver_to_person_id,
	destination_context,
	destination_type_code,
	destination_type,
	employee_id,
	from_organization_id,
	item_description,
	item_id,
	item_revision,
	hazard_class,
	un_number,
	line_num,
	item_category_id,
	locator_id,
	need_by_date,
	packing_slip,
	quantity_received,
	quantity_shipped,
	requisition_line_id,
	requisition_header_id,
	order_num,
	order_line_num,
	req_distribution_id,
	shipment_header_id,
	shipment_line_id,
	shipment_line_status_code,
	source_document_code,
	source_document_type,
	to_organization_id,
	to_subinventory,
	transfer_cost,
	transportation_account_id,
	transportation_cost,
	unit_of_measure,
	uom_conversion_rate,
	routing_header_id,
	routing_name,
	reason_id,
	reason_name,
	location_code,
	deliver_to_person,
	po_header_id,
	po_line_id,
	po_line_location_id,
	po_release_id,
	release_num,
	vendor_name,
	vendor_site_code,
	ship_to_location_id,
	primary_unit_of_measure,
	vendor_id,
	bar_code_label,
	truck_num,
	container_num,
	vendor_lot_num,
	secondary_quantity_received,
	secondary_quantity_shipped,
	secondary_unit_of_measure,
	qc_grade,
	org_id,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    SELECT
 source_type,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
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
	charge_account_id,
	comments,
	deliver_to_location_id,
	deliver_to_person_id,
	destination_context,
	destination_type_code,
	destination_type,
	employee_id,
	from_organization_id,
	item_description,
	item_id,
	item_revision,
	hazard_class,
	un_number,
	line_num,
	item_category_id,
	locator_id,
	need_by_date,
	packing_slip,
	quantity_received,
	quantity_shipped,
	requisition_line_id,
	requisition_header_id,
	order_num,
	order_line_num,
	req_distribution_id,
	shipment_header_id,
	shipment_line_id,
	shipment_line_status_code,
	source_document_code,
	source_document_type,
	to_organization_id,
	to_subinventory,
	transfer_cost,
	transportation_account_id,
	transportation_cost,
	unit_of_measure,
	uom_conversion_rate,
	routing_header_id,
	routing_name,
	reason_id,
	reason_name,
	location_code,
	deliver_to_person,
	po_header_id,
	po_line_id,
	po_line_location_id,
	po_release_id,
	release_num,
	vendor_name,
	vendor_site_code,
	ship_to_location_id,
	primary_unit_of_measure,
	vendor_id,
	bar_code_label,
	truck_num,
	container_num,
	vendor_lot_num,
	secondary_quantity_received,
	secondary_quantity_shipped,
	secondary_unit_of_measure,
	qc_grade,
	org_id,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
    FROM
        bec_ods_stg.XXBEC_RCV_MSL_V;

end;		 
UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'xxbec_rcv_msl_v';
	
COMMIT;