/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
BEGIN;
TRUNCATE TABLE bec_ods_stg.WSH_NEW_DELIVERIES;


insert into bec_ods_stg.WSH_NEW_DELIVERIES
(
	delivery_id,
	"name",
	planned_flag,
	status_code,
	initial_pickup_date,
	initial_pickup_location_id,
	ultimate_dropoff_location_id,
	ultimate_dropoff_date,
	customer_id,
	intmed_ship_to_location_id,
	pooled_ship_to_location_id,
	freight_terms_code,
	fob_code,
	fob_location_id,
	waybill,
	acceptance_flag,
	accepted_by,
	accepted_date,
	acknowledged_by,
	confirmed_by,
	asn_date_sent,
	asn_status_code,
	asn_seq_number,
	reason_of_transport,
	description,
	gross_weight,
	net_weight,
	weight_uom_code,
	volume,
	volume_uom_code,
	additional_shipment_info,
	port_of_discharge,
	booking_number,
	cod_amount,
	cod_currency_code,
	service_contract,
	cod_remit_to,
	cod_charge_paid_by,
	problem_contact_reference,
	bill_freight_to,
	carried_by,
	port_of_loading,
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
	global_attribute_category,
	global_attribute1,
	global_attribute2,
	global_attribute3,
	global_attribute4,
	global_attribute5,
	global_attribute6,
	global_attribute7,
	global_attribute8,
	global_attribute9,
	global_attribute10,
	global_attribute11,
	global_attribute12,
	global_attribute13,
	global_attribute14,
	global_attribute15,
	global_attribute16,
	global_attribute17,
	global_attribute18,
	global_attribute19,
	global_attribute20,
	confirm_date,
	ship_method_code,
	dock_code,
	delivery_type,
	carrier_id,
	currency_code,
	organization_id,
	loading_sequence,
	loading_order_flag,
	number_of_lpn,
	batch_id,
	source_header_id,
	hash_value,
	ftz_number,
	routed_export_txn,
	entry_number,
	routing_instructions,
	in_bond_code,
	shipping_marks,
	service_level,
	mode_of_transport,
	assigned_to_fte_trips,
	auto_sc_exclude_flag,
	auto_ap_exclude_flag,
	ap_batch_id,
	tp_delivery_number,
	tp_plan_name,
	vendor_id,
	earliest_pickup_date,
	latest_pickup_date,
	earliest_dropoff_date,
	latest_dropoff_date,
	ignore_for_planning,
	party_id,
	routing_response_id,
	rcv_shipment_header_id,
	asn_shipment_header_id,
	shipment_direction,
	shipping_control,
	wv_frozen_flag,
	hash_string,
	itinerary_complete,
	delivered_date,
	delivery_wf_process,
	del_wf_intransit_attr,
	del_wf_interface_attr,
	del_wf_close_attr,
	delivery_scpod_wf_process,
	tms_version_number,
	tms_interface_flag,
	pending_advice_flag,
	client_id,
	consignee_flag,
	service_org_id,
	tms_sub_batch_id
,KCA_OPERATION
,KCA_SEQ_ID
,KCA_SEQ_DATE
) (SELECT 
	delivery_id,
	"name",
	planned_flag,
	status_code,
	initial_pickup_date,
	initial_pickup_location_id,
	ultimate_dropoff_location_id,
	ultimate_dropoff_date,
	customer_id,
	intmed_ship_to_location_id,
	pooled_ship_to_location_id,
	freight_terms_code,
	fob_code,
	fob_location_id,
	waybill,
	acceptance_flag,
	accepted_by,
	accepted_date,
	acknowledged_by,
	confirmed_by,
	asn_date_sent,
	asn_status_code,
	asn_seq_number,
	reason_of_transport,
	description,
	gross_weight,
	net_weight,
	weight_uom_code,
	volume,
	volume_uom_code,
	additional_shipment_info,
	port_of_discharge,
	booking_number,
	cod_amount,
	cod_currency_code,
	service_contract,
	cod_remit_to,
	cod_charge_paid_by,
	problem_contact_reference,
	bill_freight_to,
	carried_by,
	port_of_loading,
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
	global_attribute_category,
	global_attribute1,
	global_attribute2,
	global_attribute3,
	global_attribute4,
	global_attribute5,
	global_attribute6,
	global_attribute7,
	global_attribute8,
	global_attribute9,
	global_attribute10,
	global_attribute11,
	global_attribute12,
	global_attribute13,
	global_attribute14,
	global_attribute15,
	global_attribute16,
	global_attribute17,
	global_attribute18,
	global_attribute19,
	global_attribute20,
	confirm_date,
	ship_method_code,
	dock_code,
	delivery_type,
	carrier_id,
	currency_code,
	organization_id,
	loading_sequence,
	loading_order_flag,
	number_of_lpn,
	batch_id,
	source_header_id,
	hash_value,
	ftz_number,
	routed_export_txn,
	entry_number,
	routing_instructions,
	in_bond_code,
	shipping_marks,
	service_level,
	mode_of_transport,
	assigned_to_fte_trips,
	auto_sc_exclude_flag,
	auto_ap_exclude_flag,
	ap_batch_id,
	tp_delivery_number,
	tp_plan_name,
	vendor_id,
	earliest_pickup_date,
	latest_pickup_date,
	earliest_dropoff_date,
	latest_dropoff_date,
	ignore_for_planning,
	party_id,
	routing_response_id,
	rcv_shipment_header_id,
	asn_shipment_header_id,
	shipment_direction,
	shipping_control,
	wv_frozen_flag,
	hash_string,
	itinerary_complete,
	delivered_date,
	delivery_wf_process,
	del_wf_intransit_attr,
	del_wf_interface_attr,
	del_wf_close_attr,
	delivery_scpod_wf_process,
	tms_version_number,
	tms_interface_flag,
	pending_advice_flag,
	client_id,
	consignee_flag,
	service_org_id,
	tms_sub_batch_id
,KCA_OPERATION
,KCA_SEQ_ID
,KCA_SEQ_DATE
 from bec_raw_dl_ext.WSH_NEW_DELIVERIES 
 where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (DELIVERY_ID,KCA_SEQ_ID) in 
	(select DELIVERY_ID,max(KCA_SEQ_ID) from bec_raw_dl_ext.WSH_NEW_DELIVERIES 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by DELIVERY_ID)
     and  (KCA_SEQ_DATE > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='wsh_new_deliveries')
)
);
END;