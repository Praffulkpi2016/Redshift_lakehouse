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

DROP TABLE if exists bec_ods.WSH_NEW_DELIVERIES;

CREATE TABLE IF NOT EXISTS bec_ods.WSH_NEW_DELIVERIES
(
	delivery_id NUMERIC(15,0)   ENCODE az64
	,name VARCHAR(30)   ENCODE lzo
	,planned_flag VARCHAR(1)   ENCODE lzo
	,status_code VARCHAR(2)   ENCODE lzo
	,initial_pickup_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,initial_pickup_location_id NUMERIC(15,0)   ENCODE az64
	,ultimate_dropoff_location_id NUMERIC(15,0)   ENCODE az64
	,ultimate_dropoff_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,customer_id NUMERIC(15,0)   ENCODE az64
	,intmed_ship_to_location_id NUMERIC(15,0)   ENCODE az64
	,pooled_ship_to_location_id NUMERIC(15,0)   ENCODE az64
	,freight_terms_code VARCHAR(30)   ENCODE lzo
	,fob_code VARCHAR(30)   ENCODE lzo
	,fob_location_id NUMERIC(15,0)   ENCODE az64
	,waybill VARCHAR(30)   ENCODE lzo
	,acceptance_flag VARCHAR(1)   ENCODE lzo
	,accepted_by VARCHAR(150)   ENCODE lzo
	,accepted_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,acknowledged_by VARCHAR(150)   ENCODE lzo
	,confirmed_by VARCHAR(150)   ENCODE lzo
	,asn_date_sent TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,asn_status_code VARCHAR(15)   ENCODE lzo
	,asn_seq_number NUMERIC(28,10)   ENCODE az64
	,reason_of_transport VARCHAR(30)   ENCODE lzo
	,description VARCHAR(30)   ENCODE lzo
	,gross_weight NUMERIC(28,10)   ENCODE az64
	,net_weight NUMERIC(28,10)   ENCODE az64
	,weight_uom_code VARCHAR(3)   ENCODE lzo
	,volume NUMERIC(28,10)   ENCODE az64
	,volume_uom_code VARCHAR(3)   ENCODE lzo
	,additional_shipment_info VARCHAR(500)   ENCODE lzo
	,port_of_discharge VARCHAR(150)   ENCODE lzo
	,booking_number VARCHAR(30)   ENCODE lzo
	,cod_amount NUMERIC(28,10)   ENCODE az64
	,cod_currency_code VARCHAR(15)   ENCODE lzo
	,service_contract VARCHAR(30)   ENCODE lzo
	,cod_remit_to VARCHAR(150)   ENCODE lzo
	,cod_charge_paid_by VARCHAR(150)   ENCODE lzo
	,problem_contact_reference VARCHAR(500)   ENCODE lzo
	,bill_freight_to VARCHAR(1000)   ENCODE lzo
	,carried_by VARCHAR(150)   ENCODE lzo
	,port_of_loading VARCHAR(150)   ENCODE lzo
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
	,tp_attribute_category VARCHAR(150)   ENCODE lzo
	,tp_attribute1 VARCHAR(150)   ENCODE lzo
	,tp_attribute2 VARCHAR(150)   ENCODE lzo
	,tp_attribute3 VARCHAR(150)   ENCODE lzo
	,tp_attribute4 VARCHAR(150)   ENCODE lzo
	,tp_attribute5 VARCHAR(150)   ENCODE lzo
	,tp_attribute6 VARCHAR(150)   ENCODE lzo
	,tp_attribute7 VARCHAR(150)   ENCODE lzo
	,tp_attribute8 VARCHAR(150)   ENCODE lzo
	,tp_attribute9 VARCHAR(150)   ENCODE lzo
	,tp_attribute10 VARCHAR(150)   ENCODE lzo
	,tp_attribute11 VARCHAR(150)   ENCODE lzo
	,tp_attribute12 VARCHAR(150)   ENCODE lzo
	,tp_attribute13 VARCHAR(150)   ENCODE lzo
	,tp_attribute14 VARCHAR(150)   ENCODE lzo
	,tp_attribute15 VARCHAR(150)   ENCODE lzo
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,global_attribute_category VARCHAR(30)   ENCODE lzo
	,global_attribute1 VARCHAR(150)   ENCODE lzo
	,global_attribute2 VARCHAR(150)   ENCODE lzo
	,global_attribute3 VARCHAR(150)   ENCODE lzo
	,global_attribute4 VARCHAR(150)   ENCODE lzo
	,global_attribute5 VARCHAR(150)   ENCODE lzo
	,global_attribute6 VARCHAR(150)   ENCODE lzo
	,global_attribute7 VARCHAR(150)   ENCODE lzo
	,global_attribute8 VARCHAR(150)   ENCODE lzo
	,global_attribute9 VARCHAR(150)   ENCODE lzo
	,global_attribute10 VARCHAR(150)   ENCODE lzo
	,global_attribute11 VARCHAR(150)   ENCODE lzo
	,global_attribute12 VARCHAR(150)   ENCODE lzo
	,global_attribute13 VARCHAR(150)   ENCODE lzo
	,global_attribute14 VARCHAR(150)   ENCODE lzo
	,global_attribute15 VARCHAR(150)   ENCODE lzo
	,global_attribute16 VARCHAR(150)   ENCODE lzo
	,global_attribute17 VARCHAR(150)   ENCODE lzo
	,global_attribute18 VARCHAR(150)   ENCODE lzo
	,global_attribute19 VARCHAR(150)   ENCODE lzo
	,global_attribute20 VARCHAR(150)   ENCODE lzo
	,confirm_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,ship_method_code VARCHAR(30)   ENCODE lzo
	,dock_code VARCHAR(30)   ENCODE lzo
	,delivery_type VARCHAR(30)   ENCODE lzo
	,carrier_id NUMERIC(15,0)   ENCODE az64
	,currency_code VARCHAR(150)   ENCODE lzo
	,organization_id NUMERIC(15,0)   ENCODE az64
	,loading_sequence NUMERIC(28,10)   ENCODE az64
	,loading_order_flag VARCHAR(2)   ENCODE lzo
	,number_of_lpn NUMERIC(28,10)   ENCODE az64
	,batch_id NUMERIC(15,0)   ENCODE az64
	,source_header_id NUMERIC(15,0)   ENCODE az64
	,hash_value NUMERIC(28,10)   ENCODE az64
	,ftz_number VARCHAR(35)   ENCODE lzo
	,routed_export_txn VARCHAR(35)   ENCODE lzo
	,entry_number VARCHAR(35)   ENCODE lzo
	,routing_instructions VARCHAR(120)   ENCODE lzo
	,in_bond_code VARCHAR(35)   ENCODE lzo
	,shipping_marks VARCHAR(100)   ENCODE lzo
	,service_level VARCHAR(35)   ENCODE lzo
	,mode_of_transport VARCHAR(35)   ENCODE lzo
	,assigned_to_fte_trips VARCHAR(1)   ENCODE lzo
	,auto_sc_exclude_flag VARCHAR(1)   ENCODE lzo
	,auto_ap_exclude_flag VARCHAR(1)   ENCODE lzo
	,ap_batch_id NUMERIC(15,0)   ENCODE az64
	,tp_delivery_number NUMERIC(15,0)   ENCODE az64
	,tp_plan_name VARCHAR(150)   ENCODE lzo
	,vendor_id NUMERIC(15,0)   ENCODE az64
	,earliest_pickup_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,latest_pickup_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,earliest_dropoff_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,latest_dropoff_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,ignore_for_planning VARCHAR(1)   ENCODE lzo
	,party_id NUMERIC(15,0)   ENCODE az64
	,routing_response_id NUMERIC(15,0)   ENCODE az64
	,rcv_shipment_header_id NUMERIC(15,0)   ENCODE az64
	,asn_shipment_header_id NUMERIC(15,0)   ENCODE az64
	,shipment_direction VARCHAR(30)   ENCODE lzo
	,shipping_control VARCHAR(30)   ENCODE lzo
	,wv_frozen_flag VARCHAR(1)   ENCODE lzo
	,hash_string VARCHAR(1000)   ENCODE lzo
	,itinerary_complete VARCHAR(50)   ENCODE lzo
	,delivered_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,delivery_wf_process VARCHAR(30)   ENCODE lzo
	,del_wf_intransit_attr VARCHAR(1)   ENCODE lzo
	,del_wf_interface_attr VARCHAR(1)   ENCODE lzo
	,del_wf_close_attr VARCHAR(1)   ENCODE lzo
	,delivery_scpod_wf_process VARCHAR(30)   ENCODE lzo
	,tms_version_number NUMERIC(28,10)   ENCODE az64
	,tms_interface_flag VARCHAR(3)   ENCODE lzo
	,pending_advice_flag VARCHAR(1)   ENCODE lzo
	,client_id NUMERIC(15,0)   ENCODE az64
	,consignee_flag VARCHAR(150)   ENCODE lzo
	,service_org_id NUMERIC(15,0)   ENCODE az64
	,tms_sub_batch_id NUMERIC(15,0)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64	
)
DISTSTYLE
auto;

INSERT INTO bec_ods.WSH_NEW_DELIVERIES (
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
	tms_sub_batch_id,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
 SELECT
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
	tms_sub_batch_id,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.WSH_NEW_DELIVERIES;
end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'wsh_new_deliveries';

COMMIT;