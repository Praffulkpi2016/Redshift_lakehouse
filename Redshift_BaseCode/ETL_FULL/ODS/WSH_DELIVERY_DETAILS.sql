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

DROP TABLE if exists bec_ods.WSH_DELIVERY_DETAILS;

CREATE TABLE IF NOT EXISTS bec_ods.WSH_DELIVERY_DETAILS
(
delivery_detail_id NUMERIC(15,0)   ENCODE az64
	,source_code VARCHAR(30)   ENCODE lzo
	,source_header_id NUMERIC(15,0)   ENCODE az64
	,source_line_id NUMERIC(15,0)   ENCODE az64
	,source_header_type_id NUMERIC(15,0)   ENCODE az64
	,source_header_type_name VARCHAR(240)   ENCODE lzo
	,cust_po_number VARCHAR(50)   ENCODE lzo
	,customer_id NUMERIC(15,0)   ENCODE az64
	,sold_to_contact_id NUMERIC(15,0)   ENCODE az64
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,item_description VARCHAR(250)   ENCODE lzo
	,ship_set_id NUMERIC(15,0)   ENCODE az64
	,arrival_set_id NUMERIC(15,0)   ENCODE az64
	,top_model_line_id NUMERIC(15,0)   ENCODE az64
	,ato_line_id NUMERIC(15,0)   ENCODE az64
	,hold_code VARCHAR(1)   ENCODE lzo
	,ship_model_complete_flag VARCHAR(1)   ENCODE lzo
	,hazard_class_id NUMERIC(15,0)   ENCODE az64
	,country_of_origin VARCHAR(50)   ENCODE lzo
	,classification VARCHAR(30)   ENCODE lzo
	,ship_from_location_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,ship_to_location_id NUMERIC(15,0)   ENCODE az64
	,ship_to_contact_id NUMERIC(15,0)   ENCODE az64
	,deliver_to_location_id NUMERIC(15,0)   ENCODE az64
	,deliver_to_contact_id NUMERIC(15,0)   ENCODE az64
	,intmed_ship_to_location_id NUMERIC(15,0)   ENCODE az64
	,intmed_ship_to_contact_id NUMERIC(15,0)   ENCODE az64
	,ship_tolerance_above NUMERIC(28,10)   ENCODE az64
	,ship_tolerance_below NUMERIC(28,10)   ENCODE az64
	,src_requested_quantity NUMERIC(28,10)   ENCODE az64
	,src_requested_quantity_uom VARCHAR(3)   ENCODE lzo
	,cancelled_quantity NUMERIC(28,10)   ENCODE az64
	,requested_quantity NUMERIC(28,10)   ENCODE az64
	,requested_quantity_uom VARCHAR(3)   ENCODE lzo
	,shipped_quantity NUMERIC(28,10)   ENCODE az64
	,delivered_quantity NUMERIC(28,10)   ENCODE az64
	,quality_control_quantity NUMERIC(28,10)   ENCODE az64
	,cycle_count_quantity NUMERIC(28,10)   ENCODE az64
	,move_order_line_id NUMERIC(15,0)   ENCODE az64
	,subinventory VARCHAR(10)   ENCODE lzo
	,revision VARCHAR(3)   ENCODE lzo
	,lot_number VARCHAR(80)   ENCODE lzo
	,released_status VARCHAR(1)   ENCODE lzo
	,customer_requested_lot_flag VARCHAR(1)   ENCODE lzo
	,serial_number VARCHAR(30)   ENCODE lzo
	,locator_id NUMERIC(15,0)   ENCODE az64
	,date_requested TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,date_scheduled TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,master_container_item_id NUMERIC(15,0)   ENCODE az64
	,detail_container_item_id NUMERIC(15,0)   ENCODE az64
	,load_seq_number NUMERIC(15,0)   ENCODE az64
	,ship_method_code VARCHAR(30)   ENCODE lzo
	,carrier_id NUMERIC(15,0)   ENCODE az64
	,freight_terms_code VARCHAR(30)   ENCODE lzo
	,shipment_priority_code VARCHAR(30)   ENCODE lzo
	,fob_code VARCHAR(30)   ENCODE lzo
	,customer_item_id NUMERIC(15,0)   ENCODE az64
	,dep_plan_required_flag VARCHAR(1)   ENCODE lzo
	,customer_prod_seq VARCHAR(50)   ENCODE lzo
	,customer_dock_code VARCHAR(50)   ENCODE lzo
	,net_weight NUMERIC(28,10)   ENCODE az64
	,weight_uom_code VARCHAR(3)   ENCODE lzo
	,volume NUMERIC(28,10)   ENCODE az64
	,volume_uom_code VARCHAR(3)   ENCODE lzo
	,shipping_instructions VARCHAR(2000)   ENCODE lzo
	,packing_instructions VARCHAR(2000)   ENCODE lzo
	,project_id NUMERIC(15,0)   ENCODE az64
	,task_id NUMERIC(15,0)   ENCODE az64
	,org_id NUMERIC(15,0)   ENCODE az64
	,oe_interfaced_flag VARCHAR(1)   ENCODE lzo
	,mvt_stat_status VARCHAR(30)   ENCODE lzo
	,tracking_number VARCHAR(30)   ENCODE lzo
	,transaction_temp_id NUMERIC(15,0)   ENCODE az64
	,tp_attribute_category VARCHAR(240)   ENCODE lzo
	,tp_attribute1 VARCHAR(240)   ENCODE lzo
	,tp_attribute2 VARCHAR(240)   ENCODE lzo
	,tp_attribute3 VARCHAR(240)   ENCODE lzo
	,tp_attribute4 VARCHAR(240)   ENCODE lzo
	,tp_attribute5 VARCHAR(240)   ENCODE lzo
	,tp_attribute6 VARCHAR(240)   ENCODE lzo
	,tp_attribute7 VARCHAR(240)   ENCODE lzo
	,tp_attribute8 VARCHAR(240)   ENCODE lzo
	,tp_attribute9 VARCHAR(240)   ENCODE lzo
	,tp_attribute10 VARCHAR(240)   ENCODE lzo
	,tp_attribute11 VARCHAR(240)   ENCODE lzo
	,tp_attribute12 VARCHAR(240)   ENCODE lzo
	,tp_attribute13 VARCHAR(240)   ENCODE lzo
	,tp_attribute14 VARCHAR(240)   ENCODE lzo
	,tp_attribute15 VARCHAR(240)   ENCODE lzo
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
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,movement_id NUMERIC(15,0)   ENCODE az64
	,split_from_delivery_detail_id NUMERIC(15,0)   ENCODE az64
	,inv_interfaced_flag VARCHAR(1)   ENCODE lzo
	,seal_code VARCHAR(30)   ENCODE lzo
	,minimum_fill_percent NUMERIC(28,10)   ENCODE az64
	,maximum_volume NUMERIC(28,10)   ENCODE az64
	,maximum_load_weight NUMERIC(28,10)   ENCODE az64
	,master_serial_number VARCHAR(30)   ENCODE lzo
	,gross_weight NUMERIC(28,10)   ENCODE az64
	,fill_percent NUMERIC(28,10)   ENCODE az64
	,container_name VARCHAR(30)   ENCODE lzo
	,container_type_code VARCHAR(30)   ENCODE lzo
	,container_flag VARCHAR(1)   ENCODE lzo
	,preferred_grade VARCHAR(150)   ENCODE lzo
	,src_requested_quantity2 NUMERIC(28,10)   ENCODE az64
	,src_requested_quantity_uom2 VARCHAR(3)   ENCODE lzo
	,requested_quantity2 NUMERIC(28,10)   ENCODE az64
	,shipped_quantity2 NUMERIC(28,10)   ENCODE az64
	,delivered_quantity2 NUMERIC(28,10)   ENCODE az64
	,cancelled_quantity2 NUMERIC(28,10)   ENCODE az64
	,quality_control_quantity2 NUMERIC(28,10)   ENCODE az64
	,cycle_count_quantity2 NUMERIC(28,10)   ENCODE az64
	,requested_quantity_uom2 VARCHAR(3)   ENCODE lzo
	,sublot_number VARCHAR(32)   ENCODE lzo
	,unit_price NUMERIC(28,10)   ENCODE az64
	,currency_code VARCHAR(15)   ENCODE lzo
	,unit_number VARCHAR(30)   ENCODE lzo
	,freight_class_cat_id NUMERIC(15,0)   ENCODE az64
	,commodity_code_cat_id NUMERIC(15,0)   ENCODE az64
	,lpn_content_id NUMERIC(15,0)   ENCODE az64
	,ship_to_site_use_id NUMERIC(15,0)   ENCODE az64
	,deliver_to_site_use_id NUMERIC(15,0)   ENCODE az64
	,lpn_id NUMERIC(15,0)   ENCODE az64
	,inspection_flag VARCHAR(1)   ENCODE lzo
	,original_subinventory VARCHAR(10)   ENCODE lzo
	,source_header_number VARCHAR(150)   ENCODE lzo
	,source_line_number VARCHAR(150)   ENCODE lzo
	,pickable_flag VARCHAR(1)   ENCODE lzo
	,customer_production_line VARCHAR(50)   ENCODE lzo
	,customer_job VARCHAR(50)   ENCODE lzo
	,cust_model_serial_number VARCHAR(50)   ENCODE lzo
	,to_serial_number VARCHAR(30)   ENCODE lzo
	,picked_quantity NUMERIC(28,10)   ENCODE az64
	,picked_quantity2 NUMERIC(28,10)   ENCODE az64
	,received_quantity NUMERIC(28,10)   ENCODE az64
	,received_quantity2 NUMERIC(28,10)   ENCODE az64
	,source_line_set_id NUMERIC(15,0)   ENCODE az64
	,batch_id NUMERIC(15,0)   ENCODE az64
	,transaction_id NUMERIC(15,0)   ENCODE az64
	,service_level VARCHAR(30)   ENCODE lzo
	,mode_of_transport VARCHAR(30)   ENCODE lzo
	,earliest_pickup_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,latest_pickup_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,earliest_dropoff_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,latest_dropoff_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_date_type_code VARCHAR(30)   ENCODE lzo
	,tp_delivery_detail_id NUMERIC(15,0)   ENCODE az64
	,source_document_type_id NUMERIC(15,0)   ENCODE az64
	,vendor_id NUMERIC(15,0)   ENCODE az64
	,ship_from_site_id NUMERIC(15,0)   ENCODE az64
	,ignore_for_planning VARCHAR(1)   ENCODE lzo
	,line_direction VARCHAR(30)   ENCODE lzo
	,party_id NUMERIC(15,0)   ENCODE az64
	,routing_req_id NUMERIC(15,0)   ENCODE az64
	,shipping_control VARCHAR(30)   ENCODE lzo
	,source_blanket_reference_id NUMERIC(15,0)   ENCODE az64
	,source_blanket_reference_num NUMERIC(28,10)   ENCODE az64
	,po_shipment_line_id NUMERIC(15,0)   ENCODE az64
	,po_shipment_line_number NUMERIC(28,10)   ENCODE az64
	,scheduled_quantity NUMERIC(28,10)   ENCODE az64
	,returned_quantity NUMERIC(28,10)   ENCODE az64
	,scheduled_quantity2 NUMERIC(28,10)   ENCODE az64
	,returned_quantity2 NUMERIC(28,10)   ENCODE az64
	,source_line_type_code VARCHAR(50)   ENCODE lzo
	,rcv_shipment_line_id NUMERIC(15,0)   ENCODE az64
	,supplier_item_number VARCHAR(50)   ENCODE lzo
	,filled_volume NUMERIC(28,10)   ENCODE az64
	,unit_weight NUMERIC(28,10)   ENCODE az64
	,unit_volume NUMERIC(28,10)   ENCODE az64
	,wv_frozen_flag VARCHAR(1)   ENCODE lzo
	,po_revision_number NUMERIC(28,10)   ENCODE az64
	,release_revision_number NUMERIC(28,10)   ENCODE az64
	,replenishment_status VARCHAR(50)   ENCODE lzo
	,original_lot_number VARCHAR(50)   ENCODE lzo
	,original_revision VARCHAR(50)   ENCODE lzo
	,original_locator_id NUMERIC(15,0)   ENCODE az64
	,reference_number VARCHAR(30)   ENCODE lzo
	,reference_line_number VARCHAR(30)   ENCODE lzo
	,reference_line_quantity NUMERIC(28,10)   ENCODE az64
	,reference_line_quantity_uom VARCHAR(30)   ENCODE lzo
	,client_id NUMERIC(15,0)   ENCODE az64
	,shipment_batch_id NUMERIC(15,0)   ENCODE az64
	,shipment_line_number NUMERIC(15,0)   ENCODE az64
	,reference_line_id NUMERIC(15,0)   ENCODE az64
	,consignee_flag VARCHAR(1)   ENCODE lzo
	,equipment_id NUMERIC(15,0)   ENCODE az64
	,mcc_code VARCHAR(30)   ENCODE lzo
	,tms_sub_batch_id NUMERIC(15,0)   ENCODE az64
	,tms_sub_batch_line_num NUMERIC(15,0)   ENCODE az64
	,tms_interface_flag VARCHAR(3)   ENCODE lzo
	,tms_sshipunit_id VARCHAR(50)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64	
)
DISTSTYLE
auto;

INSERT INTO bec_ods.WSH_DELIVERY_DETAILS (
	delivery_detail_id,
	source_code,
	source_header_id,
	source_line_id,
	source_header_type_id,
	source_header_type_name,
	cust_po_number,
	customer_id,
	sold_to_contact_id,
	inventory_item_id,
	item_description,
	ship_set_id,
	arrival_set_id,
	top_model_line_id,
	ato_line_id,
	hold_code,
	ship_model_complete_flag,
	hazard_class_id,
	country_of_origin,
	classification,
	ship_from_location_id,
	organization_id,
	ship_to_location_id,
	ship_to_contact_id,
	deliver_to_location_id,
	deliver_to_contact_id,
	intmed_ship_to_location_id,
	intmed_ship_to_contact_id,
	ship_tolerance_above,
	ship_tolerance_below,
	src_requested_quantity,
	src_requested_quantity_uom,
	cancelled_quantity,
	requested_quantity,
	requested_quantity_uom,
	shipped_quantity,
	delivered_quantity,
	quality_control_quantity,
	cycle_count_quantity,
	move_order_line_id,
	subinventory,
	revision,
	lot_number,
	released_status,
	customer_requested_lot_flag,
	serial_number,
	locator_id,
	date_requested,
	date_scheduled,
	master_container_item_id,
	detail_container_item_id,
	load_seq_number,
	ship_method_code,
	carrier_id,
	freight_terms_code,
	shipment_priority_code,
	fob_code,
	customer_item_id,
	dep_plan_required_flag,
	customer_prod_seq,
	customer_dock_code,
	net_weight,
	weight_uom_code,
	volume,
	volume_uom_code,
	shipping_instructions,
	packing_instructions,
	project_id,
	task_id,
	org_id,
	oe_interfaced_flag,
	mvt_stat_status,
	tracking_number,
	transaction_temp_id,
	tp_attribute_category,
	tp_attribute1,
	tp_attribute2,
	tp_attribute3,
	tp_attribute4,
	tp_attribute5,
	tp_attribute6,
	tp_attribute7,
	tp_attribute8,
	tp_attribute9,
	tp_attribute10,
	tp_attribute11,
	tp_attribute12,
	tp_attribute13,
	tp_attribute14,
	tp_attribute15,
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
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	movement_id,
	split_from_delivery_detail_id,
	inv_interfaced_flag,
	seal_code,
	minimum_fill_percent,
	maximum_volume,
	maximum_load_weight,
	master_serial_number,
	gross_weight,
	fill_percent,
	container_name,
	container_type_code,
	container_flag,
	preferred_grade,
	src_requested_quantity2,
	src_requested_quantity_uom2,
	requested_quantity2,
	shipped_quantity2,
	delivered_quantity2,
	cancelled_quantity2,
	quality_control_quantity2,
	cycle_count_quantity2,
	requested_quantity_uom2,
	sublot_number,
	unit_price,
	currency_code,
	unit_number,
	freight_class_cat_id,
	commodity_code_cat_id,
	lpn_content_id,
	ship_to_site_use_id,
	deliver_to_site_use_id,
	lpn_id,
	inspection_flag,
	original_subinventory,
	source_header_number,
	source_line_number,
	pickable_flag,
	customer_production_line,
	customer_job,
	cust_model_serial_number,
	to_serial_number,
	picked_quantity,
	picked_quantity2,
	received_quantity,
	received_quantity2,
	source_line_set_id,
	batch_id,
	transaction_id,
	service_level,
	mode_of_transport,
	earliest_pickup_date,
	latest_pickup_date,
	earliest_dropoff_date,
	latest_dropoff_date,
	request_date_type_code,
	tp_delivery_detail_id,
	source_document_type_id,
	vendor_id,
	ship_from_site_id,
	ignore_for_planning,
	line_direction,
	party_id,
	routing_req_id,
	shipping_control,
	source_blanket_reference_id,
	source_blanket_reference_num,
	po_shipment_line_id,
	po_shipment_line_number,
	scheduled_quantity,
	returned_quantity,
	scheduled_quantity2,
	returned_quantity2,
	source_line_type_code,
	rcv_shipment_line_id,
	supplier_item_number,
	filled_volume,
	unit_weight,
	unit_volume,
	wv_frozen_flag,
	po_revision_number,
	release_revision_number,
	replenishment_status,
	original_lot_number,
	original_revision,
	original_locator_id,
	reference_number,
	reference_line_number,
	reference_line_quantity,
	reference_line_quantity_uom,
	client_id,
	shipment_batch_id,
	shipment_line_number,
	reference_line_id,
	consignee_flag,
	equipment_id,
	mcc_code,
	tms_sub_batch_id,
	tms_sub_batch_line_num,
	tms_interface_flag,
	tms_sshipunit_id,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
 SELECT
	delivery_detail_id,
	source_code,
	source_header_id,
	source_line_id,
	source_header_type_id,
	source_header_type_name,
	cust_po_number,
	customer_id,
	sold_to_contact_id,
	inventory_item_id,
	item_description,
	ship_set_id,
	arrival_set_id,
	top_model_line_id,
	ato_line_id,
	hold_code,
	ship_model_complete_flag,
	hazard_class_id,
	country_of_origin,
	classification,
	ship_from_location_id,
	organization_id,
	ship_to_location_id,
	ship_to_contact_id,
	deliver_to_location_id,
	deliver_to_contact_id,
	intmed_ship_to_location_id,
	intmed_ship_to_contact_id,
	ship_tolerance_above,
	ship_tolerance_below,
	src_requested_quantity,
	src_requested_quantity_uom,
	cancelled_quantity,
	requested_quantity,
	requested_quantity_uom,
	shipped_quantity,
	delivered_quantity,
	quality_control_quantity,
	cycle_count_quantity,
	move_order_line_id,
	subinventory,
	revision,
	lot_number,
	released_status,
	customer_requested_lot_flag,
	serial_number,
	locator_id,
	date_requested,
	date_scheduled,
	master_container_item_id,
	detail_container_item_id,
	load_seq_number,
	ship_method_code,
	carrier_id,
	freight_terms_code,
	shipment_priority_code,
	fob_code,
	customer_item_id,
	dep_plan_required_flag,
	customer_prod_seq,
	customer_dock_code,
	net_weight,
	weight_uom_code,
	volume,
	volume_uom_code,
	shipping_instructions,
	packing_instructions,
	project_id,
	task_id,
	org_id,
	oe_interfaced_flag,
	mvt_stat_status,
	tracking_number,
	transaction_temp_id,
	tp_attribute_category,
	tp_attribute1,
	tp_attribute2,
	tp_attribute3,
	tp_attribute4,
	tp_attribute5,
	tp_attribute6,
	tp_attribute7,
	tp_attribute8,
	tp_attribute9,
	tp_attribute10,
	tp_attribute11,
	tp_attribute12,
	tp_attribute13,
	tp_attribute14,
	tp_attribute15,
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
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	movement_id,
	split_from_delivery_detail_id,
	inv_interfaced_flag,
	seal_code,
	minimum_fill_percent,
	maximum_volume,
	maximum_load_weight,
	master_serial_number,
	gross_weight,
	fill_percent,
	container_name,
	container_type_code,
	container_flag,
	preferred_grade,
	src_requested_quantity2,
	src_requested_quantity_uom2,
	requested_quantity2,
	shipped_quantity2,
	delivered_quantity2,
	cancelled_quantity2,
	quality_control_quantity2,
	cycle_count_quantity2,
	requested_quantity_uom2,
	sublot_number,
	unit_price,
	currency_code,
	unit_number,
	freight_class_cat_id,
	commodity_code_cat_id,
	lpn_content_id,
	ship_to_site_use_id,
	deliver_to_site_use_id,
	lpn_id,
	inspection_flag,
	original_subinventory,
	source_header_number,
	source_line_number,
	pickable_flag,
	customer_production_line,
	customer_job,
	cust_model_serial_number,
	to_serial_number,
	picked_quantity,
	picked_quantity2,
	received_quantity,
	received_quantity2,
	source_line_set_id,
	batch_id,
	transaction_id,
	service_level,
	mode_of_transport,
	earliest_pickup_date,
	latest_pickup_date,
	earliest_dropoff_date,
	latest_dropoff_date,
	request_date_type_code,
	tp_delivery_detail_id,
	source_document_type_id,
	vendor_id,
	ship_from_site_id,
	ignore_for_planning,
	line_direction,
	party_id,
	routing_req_id,
	shipping_control,
	source_blanket_reference_id,
	source_blanket_reference_num,
	po_shipment_line_id,
	po_shipment_line_number,
	scheduled_quantity,
	returned_quantity,
	scheduled_quantity2,
	returned_quantity2,
	source_line_type_code,
	rcv_shipment_line_id,
	supplier_item_number,
	filled_volume,
	unit_weight,
	unit_volume,
	wv_frozen_flag,
	po_revision_number,
	release_revision_number,
	replenishment_status,
	original_lot_number,
	original_revision,
	original_locator_id,
	reference_number,
	reference_line_number,
	reference_line_quantity,
	reference_line_quantity_uom,
	client_id,
	shipment_batch_id,
	shipment_line_number,
	reference_line_id,
	consignee_flag,
	equipment_id,
	mcc_code,
	tms_sub_batch_id,
	tms_sub_batch_line_num,
	tms_interface_flag,
	tms_sshipunit_id,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.WSH_DELIVERY_DETAILS;
end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'wsh_delivery_details';

COMMIT;