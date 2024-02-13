/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for ODS.
# File Version: KPI v1.0
*/

begin;

truncate table bec_ods.wsh_deliverables_v;

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
	,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	KCA_SEQ_DATE
FROM bec_ods_stg.wsh_deliverables_v ;

end;

update
	bec_etl_ctrl.batch_ods_info
set load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'wsh_deliverables_v';
commit;