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

DROP TABLE if exists bec_ods.wsh_deliverables_v;

CREATE TABLE IF NOT EXISTS bec_ods.wsh_deliverables_v
(	trip_id NUMERIC(15,0)   ENCODE az64
	,stop_id NUMERIC(15,0)   ENCODE az64
	,"type" VARCHAR(1)   ENCODE lzo
	,delivery_detail_id NUMERIC(15,0)   ENCODE az64
	,delivery_line_id NUMERIC(15,0)   ENCODE az64
	,container_instance_id NUMERIC(15,0)   ENCODE az64
	,container_name VARCHAR(30)   ENCODE lzo
	,delivery_id NUMERIC(15,0)   ENCODE az64
	,source_code VARCHAR(30)   ENCODE lzo
	,source_name VARCHAR(80)   ENCODE lzo
	,source_header_id NUMERIC(15,0)   ENCODE az64
	,source_line_id NUMERIC(15,0)   ENCODE az64
	,source_header_number VARCHAR(150)   ENCODE lzo
	,source_header_type_id NUMERIC(15,0)   ENCODE az64
	,source_header_type_name VARCHAR(240)   ENCODE lzo
	,source_line_number VARCHAR(150)   ENCODE lzo
	,src_requested_quantity NUMERIC(28,10)   ENCODE az64
	,src_requested_quantity_uom VARCHAR(3)   ENCODE lzo
	,customer_id NUMERIC(15,0)   ENCODE az64
	,sold_to_contact_id NUMERIC(15,0)   ENCODE az64
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,item_description VARCHAR(250)   ENCODE lzo
	,country_of_origin VARCHAR(50)   ENCODE lzo
	,classification VARCHAR(30)   ENCODE lzo
	,ship_from_location_id NUMERIC(15,0)   ENCODE az64
	,ship_to_location_id NUMERIC(15,0)   ENCODE az64
	,ship_to_contact_id NUMERIC(15,0)   ENCODE az64
	,deliver_to_location_id NUMERIC(15,0)   ENCODE az64
	,deliver_to_contact_id NUMERIC(15,0)   ENCODE az64
	,intmed_ship_to_location_id NUMERIC(15,0)   ENCODE az64
	,intmed_ship_to_contact_id NUMERIC(15,0)   ENCODE az64
	,ship_tolerance_above NUMERIC(28,10)   ENCODE az64
	,ship_tolerance_below NUMERIC(28,10)   ENCODE az64
	,requested_quantity NUMERIC(28,10)   ENCODE az64
	,shipped_quantity NUMERIC(28,10)   ENCODE az64
	,delivered_quantity NUMERIC(28,10)   ENCODE az64
	,cancelled_quantity NUMERIC(28,10)   ENCODE az64
	,requested_quantity_uom VARCHAR(3)   ENCODE lzo
	,subinventory VARCHAR(10)   ENCODE lzo
	,revision VARCHAR(3)   ENCODE lzo
	,lot_number VARCHAR(80)   ENCODE lzo
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
	,unit_weight NUMERIC(28,10)   ENCODE az64
	,unit_volume NUMERIC(28,10)   ENCODE az64
	,wv_frozen_flag VARCHAR(1)   ENCODE lzo
	,gross_weight NUMERIC(28,10)   ENCODE az64
	,tare_weight NUMERIC(28,10)   ENCODE az64
	,net_weight NUMERIC(28,10)   ENCODE az64
	,weight_uom_code VARCHAR(3)   ENCODE lzo
	,filled_volume NUMERIC(28,10)   ENCODE az64
	,volume NUMERIC(28,10)   ENCODE az64
	,volume_uom_code VARCHAR(3)   ENCODE lzo
	,fill_percent NUMERIC(28,10)   ENCODE az64
	,maximum_load_weight NUMERIC(28,10)   ENCODE az64
	,maximum_volume NUMERIC(28,10)   ENCODE az64
	,minimum_fill_percent NUMERIC(28,10)   ENCODE az64
	,mvt_stat_status VARCHAR(30)   ENCODE lzo
	,released_status VARCHAR(1)   ENCODE lzo
	,released_status_name VARCHAR(80)   ENCODE lzo
	,organization_id NUMERIC(15,0)   ENCODE az64
	,transaction_temp_id NUMERIC(15,0)   ENCODE az64
	,ship_set_id NUMERIC(15,0)   ENCODE az64
	,arrival_set_id NUMERIC(15,0)   ENCODE az64
	,ship_model_complete_flag VARCHAR(1)   ENCODE lzo
	,top_model_line_id NUMERIC(15,0)   ENCODE az64
	,hold_code VARCHAR(1)   ENCODE lzo
	,cust_po_number VARCHAR(50)   ENCODE lzo
	,ato_line_id NUMERIC(15,0)   ENCODE az64
	,move_order_line_id NUMERIC(15,0)   ENCODE az64
	,hazard_class_id NUMERIC(15,0)   ENCODE az64
	,quality_control_quantity NUMERIC(28,10)   ENCODE az64
	,cycle_count_quantity NUMERIC(28,10)   ENCODE az64
	,tracking_number VARCHAR(30)   ENCODE lzo
	,movement_id NUMERIC(15,0)   ENCODE az64
	,shipping_instructions VARCHAR(2000)   ENCODE lzo
	,packing_instructions VARCHAR(2000)   ENCODE lzo
	,project_id NUMERIC(15,0)   ENCODE az64
	,task_id NUMERIC(15,0)   ENCODE az64
	,org_id NUMERIC(15,0)   ENCODE az64
	,oe_interfaced_flag VARCHAR(1)   ENCODE lzo
	,split_from_delivery_detail_id NUMERIC(15,0)   ENCODE az64
	,inv_interfaced_flag VARCHAR(1)   ENCODE lzo
	,parent_container_instance_id NUMERIC(15,0)   ENCODE az64
	,master_serial_number VARCHAR(30)   ENCODE lzo
	,container_flag VARCHAR(1)   ENCODE lzo
	,container_type_code VARCHAR(30)   ENCODE lzo
	,seal_code VARCHAR(30)   ENCODE lzo
	,activity_code VARCHAR(30)   ENCODE lzo
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
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,preferred_grade VARCHAR(150)   ENCODE lzo
	,shipped_quantity2 NUMERIC(28,10)   ENCODE az64
	,delivered_quantity2 NUMERIC(28,10)   ENCODE az64
	,cancelled_quantity2 NUMERIC(28,10)   ENCODE az64
	,requested_quantity2 NUMERIC(28,10)   ENCODE az64
	,requested_quantity_uom2 VARCHAR(3)   ENCODE lzo
	,src_requested_quantity2 NUMERIC(28,10)   ENCODE az64
	,src_requested_quantity_uom2 VARCHAR(3)   ENCODE lzo
	,quality_control_quantity2 NUMERIC(28,10)   ENCODE az64
	,cycle_count_quantity2 NUMERIC(28,10)   ENCODE az64
	,unit_number VARCHAR(30)   ENCODE lzo
	,unit_price NUMERIC(28,10)   ENCODE az64
	,currency_code VARCHAR(15)   ENCODE lzo
	,freight_class_cat_id NUMERIC(15,0)   ENCODE az64
	,commodity_code_cat_id NUMERIC(15,0)   ENCODE az64
	,inspection_flag VARCHAR(1)   ENCODE lzo
	,pickable_flag VARCHAR(1)   ENCODE lzo
	,ship_to_site_use_id NUMERIC(15,0)   ENCODE az64
	,deliver_to_site_use_id NUMERIC(15,0)   ENCODE az64
	,to_serial_number VARCHAR(30)   ENCODE lzo
	,picked_quantity NUMERIC(28,10)   ENCODE az64
	,picked_quantity2 NUMERIC(28,10)   ENCODE az64
	,customer_production_line VARCHAR(50)   ENCODE lzo
	,customer_job VARCHAR(50)   ENCODE lzo
	,cust_model_serial_number VARCHAR(50)   ENCODE lzo
	,received_quantity NUMERIC(28,10)   ENCODE az64
	,received_quantity2 NUMERIC(28,10)   ENCODE az64
	,source_line_set_id NUMERIC(15,0)   ENCODE az64
	,batch_id NUMERIC(15,0)   ENCODE az64
	,shipment_batch_id NUMERIC(15,0)   ENCODE az64
	,lpn_id NUMERIC(15,0)   ENCODE az64
	,original_subinventory VARCHAR(10)   ENCODE lzo
	,service_level VARCHAR(30)   ENCODE lzo
	,mode_of_transport VARCHAR(30)   ENCODE lzo
	,ignore_for_planning VARCHAR(1)   ENCODE lzo
	,LINE_DIRECTION VARCHAR(30)   ENCODE lzo
	,party_id NUMERIC(15,0)   ENCODE az64
	,routing_req_id NUMERIC(15,0)   ENCODE az64
	,shipping_control VARCHAR(30)   ENCODE lzo
	,source_blanket_reference_id NUMERIC(15,0)   ENCODE az64
	,source_blanket_reference_num NUMERIC(28,10)   ENCODE az64
	,po_shipment_line_id NUMERIC(15,0)   ENCODE az64
	,po_shipment_line_number NUMERIC(28,10)   ENCODE az64
	,returned_quantity NUMERIC(28,10)   ENCODE az64
	,returned_quantity2 NUMERIC(28,10)   ENCODE az64
	,source_line_type_code VARCHAR(50)   ENCODE lzo
	,rcv_shipment_line_id NUMERIC(15,0)   ENCODE az64
	,supplier_item_number VARCHAR(50)   ENCODE lzo
	,vendor_id NUMERIC(15,0)   ENCODE az64
	,ship_from_site_id NUMERIC(15,0)   ENCODE az64
	,earliest_pickup_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,latest_pickup_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,earliest_dropoff_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,latest_dropoff_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_date_type_code VARCHAR(30)   ENCODE lzo
	,tp_delivery_detail_id NUMERIC(15,0)   ENCODE az64
	,source_document_type_id NUMERIC(15,0)   ENCODE az64
	,po_revision_number NUMERIC(28,10)   ENCODE az64
	,release_revision_number NUMERIC(28,10)   ENCODE az64
	,replenishment_status VARCHAR(30)   ENCODE lzo
	,reference_number VARCHAR(30)   ENCODE lzo
	,reference_line_number VARCHAR(30)   ENCODE lzo
	,reference_line_quantity NUMERIC(28,10)   ENCODE az64
	,reference_line_quantity_uom VARCHAR(3)   ENCODE lzo
	,original_locator_id NUMERIC(15,0)   ENCODE az64
	,original_revision VARCHAR(3)   ENCODE lzo
	,original_lot_number VARCHAR(32)   ENCODE lzo
	,client_id NUMERIC(15,0)   ENCODE az64
	,consignee_flag VARCHAR(1)   ENCODE lzo
	,equipment_id NUMERIC(15,0)   ENCODE az64
	,mcc_code VARCHAR(30)   ENCODE lzo
	,tms_sub_batch_id NUMERIC(15,0)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;
insert into bec_ods.wsh_deliverables_v
(
	trip_id,
	stop_id,
	"type",
	delivery_detail_id,
	delivery_line_id,
	container_instance_id,
	container_name,
	delivery_id,
	source_code,
	source_name,
	source_header_id,
	source_line_id,
	source_header_number,
	source_header_type_id,
	source_header_type_name,
	source_line_number,
	src_requested_quantity,
	src_requested_quantity_uom,
	customer_id,
	sold_to_contact_id,
	inventory_item_id,
	item_description,
	country_of_origin,
	classification,
	ship_from_location_id,
	ship_to_location_id,
	ship_to_contact_id,
	deliver_to_location_id,
	deliver_to_contact_id,
	intmed_ship_to_location_id,
	intmed_ship_to_contact_id,
	ship_tolerance_above,
	ship_tolerance_below,
	requested_quantity,
	shipped_quantity,
	delivered_quantity,
	cancelled_quantity,
	requested_quantity_uom,
	subinventory,
	revision,
	lot_number,
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
	unit_weight,
	unit_volume,
	wv_frozen_flag,
	gross_weight,
	tare_weight,
	net_weight,
	weight_uom_code,
	filled_volume,
	volume,
	volume_uom_code,
	fill_percent,
	maximum_load_weight,
	maximum_volume,
	minimum_fill_percent,
	mvt_stat_status,
	released_status,
	released_status_name,
	organization_id,
	transaction_temp_id,
	ship_set_id,
	arrival_set_id,
	ship_model_complete_flag,
	top_model_line_id,
	hold_code,
	cust_po_number,
	ato_line_id,
	move_order_line_id,
	hazard_class_id,
	quality_control_quantity,
	cycle_count_quantity,
	tracking_number,
	movement_id,
	shipping_instructions,
	packing_instructions,
	project_id,
	task_id,
	org_id,
	oe_interfaced_flag,
	split_from_delivery_detail_id,
	inv_interfaced_flag,
	parent_container_instance_id,
	master_serial_number,
	container_flag,
	container_type_code,
	seal_code,
	activity_code,
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
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	preferred_grade,
	shipped_quantity2,
	delivered_quantity2,
	cancelled_quantity2,
	requested_quantity2,
	requested_quantity_uom2,
	src_requested_quantity2,
	src_requested_quantity_uom2,
	quality_control_quantity2,
	cycle_count_quantity2,
	unit_number,
	unit_price,
	currency_code,
	freight_class_cat_id,
	commodity_code_cat_id,
	inspection_flag,
	pickable_flag,
	ship_to_site_use_id,
	deliver_to_site_use_id,
	to_serial_number,
	picked_quantity,
	picked_quantity2,
	customer_production_line,
	customer_job,
	cust_model_serial_number,
	received_quantity,
	received_quantity2,
	source_line_set_id,
	batch_id,
	shipment_batch_id,
	lpn_id,
	original_subinventory,
	service_level,
	mode_of_transport,
	ignore_for_planning,
	LINE_DIRECTION,
	party_id,
	routing_req_id,
	shipping_control,
	source_blanket_reference_id,
	source_blanket_reference_num,
	po_shipment_line_id,
	po_shipment_line_number,
	returned_quantity,
	returned_quantity2,
	source_line_type_code,
	rcv_shipment_line_id,
	supplier_item_number,
	vendor_id,
	ship_from_site_id,
	earliest_pickup_date,
	latest_pickup_date,
	earliest_dropoff_date,
	latest_dropoff_date,
	request_date_type_code,
	tp_delivery_detail_id,
	source_document_type_id,
	po_revision_number,
	release_revision_number,
	replenishment_status,
	reference_number,
	reference_line_number,
	reference_line_quantity,
	reference_line_quantity_uom,
	original_locator_id,
	original_revision,
	original_lot_number,
	client_id,
	consignee_flag,
	equipment_id,
	mcc_code,
	tms_sub_batch_id
	,KCA_OPERATION
	,IS_DELETED_FLG
	,kca_seq_id
	,kca_seq_date
 )
 SELECT
 	trip_id,
	stop_id,
	"type",
	delivery_detail_id,
	delivery_line_id,
	container_instance_id,
	container_name,
	delivery_id,
	source_code,
	source_name,
	source_header_id,
	source_line_id,
	source_header_number,
	source_header_type_id,
	source_header_type_name,
	source_line_number,
	src_requested_quantity,
	src_requested_quantity_uom,
	customer_id,
	sold_to_contact_id,
	inventory_item_id,
	item_description,
	country_of_origin,
	classification,
	ship_from_location_id,
	ship_to_location_id,
	ship_to_contact_id,
	deliver_to_location_id,
	deliver_to_contact_id,
	intmed_ship_to_location_id,
	intmed_ship_to_contact_id,
	ship_tolerance_above,
	ship_tolerance_below,
	requested_quantity,
	shipped_quantity,
	delivered_quantity,
	cancelled_quantity,
	requested_quantity_uom,
	subinventory,
	revision,
	lot_number,
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
	unit_weight,
	unit_volume,
	wv_frozen_flag,
	gross_weight,
	tare_weight,
	net_weight,
	weight_uom_code,
	filled_volume,
	volume,
	volume_uom_code,
	fill_percent,
	maximum_load_weight,
	maximum_volume,
	minimum_fill_percent,
	mvt_stat_status,
	released_status,
	released_status_name,
	organization_id,
	transaction_temp_id,
	ship_set_id,
	arrival_set_id,
	ship_model_complete_flag,
	top_model_line_id,
	hold_code,
	cust_po_number,
	ato_line_id,
	move_order_line_id,
	hazard_class_id,
	quality_control_quantity,
	cycle_count_quantity,
	tracking_number,
	movement_id,
	shipping_instructions,
	packing_instructions,
	project_id,
	task_id,
	org_id,
	oe_interfaced_flag,
	split_from_delivery_detail_id,
	inv_interfaced_flag,
	parent_container_instance_id,
	master_serial_number,
	container_flag,
	container_type_code,
	seal_code,
	activity_code,
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
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	preferred_grade,
	shipped_quantity2,
	delivered_quantity2,
	cancelled_quantity2,
	requested_quantity2,
	requested_quantity_uom2,
	src_requested_quantity2,
	src_requested_quantity_uom2,
	quality_control_quantity2,
	cycle_count_quantity2,
	unit_number,
	unit_price,
	currency_code,
	freight_class_cat_id,
	commodity_code_cat_id,
	inspection_flag,
	pickable_flag,
	ship_to_site_use_id,
	deliver_to_site_use_id,
	to_serial_number,
	picked_quantity,
	picked_quantity2,
	customer_production_line,
	customer_job,
	cust_model_serial_number,
	received_quantity,
	received_quantity2,
	source_line_set_id,
	batch_id,
	shipment_batch_id,
	lpn_id,
	original_subinventory,
	service_level,
	mode_of_transport,
	ignore_for_planning,
	LINE_DIRECTION,
	party_id,
	routing_req_id,
	shipping_control,
	source_blanket_reference_id,
	source_blanket_reference_num,
	po_shipment_line_id,
	po_shipment_line_number,
	returned_quantity,
	returned_quantity2,
	source_line_type_code,
	rcv_shipment_line_id,
	supplier_item_number,
	vendor_id,
	ship_from_site_id,
	earliest_pickup_date,
	latest_pickup_date,
	earliest_dropoff_date,
	latest_dropoff_date,
	request_date_type_code,
	tp_delivery_detail_id,
	source_document_type_id,
	po_revision_number,
	release_revision_number,
	replenishment_status,
	reference_number,
	reference_line_number,
	reference_line_quantity,
	reference_line_quantity_uom,
	original_locator_id,
	original_revision,
	original_lot_number,
	client_id,
	consignee_flag,
	equipment_id,
	mcc_code,
	tms_sub_batch_id
	,KCA_OPERATION
	,'N' as IS_DELETED_FLG
	,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
FROM bec_ods_stg.wsh_deliverables_v ;

end;

update
	bec_etl_ctrl.batch_ods_info
set load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'wsh_deliverables_v';
commit;