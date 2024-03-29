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

-- Delete Records

delete from bec_ods.WSH_DELIVERY_DETAILS
where NVL(DELIVERY_DETAIL_ID,0) in (
select NVL(stg.DELIVERY_DETAIL_ID,0) 
from bec_ods.WSH_DELIVERY_DETAILS ods, bec_ods_stg.WSH_DELIVERY_DETAILS stg
where ods.DELIVERY_DETAIL_ID = stg.DELIVERY_DETAIL_ID
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into bec_ods.WSH_DELIVERY_DETAILS
(	delivery_detail_id,
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
	tms_sshipunit_id
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date)
(select
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
	tms_sshipunit_id
,KCA_OPERATION
,'N' AS IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
  KCA_SEQ_DATE
from bec_ods_stg.WSH_DELIVERY_DETAILS
where kca_operation in ('INSERT','UPDATE') 
	and (DELIVERY_DETAIL_ID,kca_seq_id) in 
	(select DELIVERY_DETAIL_ID,max(kca_seq_id) from bec_ods_stg.WSH_DELIVERY_DETAILS 
     where kca_operation in ('INSERT','UPDATE')
     group by DELIVERY_DETAIL_ID)
);

commit;

-- Soft delete
update bec_ods.WSH_DELIVERY_DETAILS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.WSH_DELIVERY_DETAILS set IS_DELETED_FLG = 'Y'
where (DELIVERY_DETAIL_ID)  in
(
select DELIVERY_DETAIL_ID from bec_raw_dl_ext.WSH_DELIVERY_DETAILS
where (DELIVERY_DETAIL_ID,KCA_SEQ_ID)
in 
(
select DELIVERY_DETAIL_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.WSH_DELIVERY_DETAILS
group by DELIVERY_DETAIL_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info 
set last_refresh_date = getdate() 
where ods_table_name='wsh_delivery_details';
commit;