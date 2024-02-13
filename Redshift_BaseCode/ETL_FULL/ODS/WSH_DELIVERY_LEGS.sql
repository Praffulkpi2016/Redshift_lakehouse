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

DROP TABLE if exists bec_ods.WSH_DELIVERY_LEGS;

CREATE TABLE IF NOT EXISTS bec_ods.WSH_DELIVERY_LEGS
(
	delivery_leg_id NUMERIC(15,0)   ENCODE az64
	,delivery_id NUMERIC(15,0)   ENCODE az64
	,sequence_number NUMERIC(15,0)   ENCODE az64
	,loading_order_flag VARCHAR(2)   ENCODE lzo
	,pick_up_stop_id NUMERIC(15,0)   ENCODE az64
	,drop_off_stop_id NUMERIC(15,0)   ENCODE az64
	,gross_weight NUMERIC(28,10)   ENCODE az64
	,net_weight NUMERIC(28,10)   ENCODE az64
	,weight_uom_code VARCHAR(3)   ENCODE lzo
	,volume NUMERIC(28,10)   ENCODE az64
	,volume_uom_code VARCHAR(3)   ENCODE lzo
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,load_tender_status VARCHAR(1)   ENCODE lzo
	,shipper_title VARCHAR(20)   ENCODE lzo
	,shipper_phone VARCHAR(20)   ENCODE lzo
	,pod_flag VARCHAR(1)   ENCODE lzo
	,pod_by VARCHAR(150)   ENCODE lzo
	,pod_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,expected_pod_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,booking_office VARCHAR(50)   ENCODE lzo
	,shipper_export_ref VARCHAR(30)   ENCODE lzo
	,carrier_export_ref VARCHAR(30)   ENCODE lzo
	,doc_notify_party VARCHAR(30)   ENCODE lzo
	,aetc_number VARCHAR(30)   ENCODE lzo
	,shipper_signed_by VARCHAR(150)   ENCODE lzo
	,shipper_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,carrier_signed_by VARCHAR(150)   ENCODE lzo
	,carrier_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,doc_issue_office VARCHAR(150)   ENCODE lzo
	,doc_issued_by VARCHAR(150)   ENCODE lzo
	,doc_date_issued TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,shipper_hm_by VARCHAR(150)   ENCODE lzo
	,shipper_hm_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,carrier_hm_by VARCHAR(150)   ENCODE lzo
	,carrier_hm_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,booking_number VARCHAR(30)   ENCODE lzo
	,port_of_loading VARCHAR(150)   ENCODE lzo
	,port_of_discharge VARCHAR(150)   ENCODE lzo
	,service_contract VARCHAR(30)   ENCODE lzo
	,bill_freight_to VARCHAR(1000)   ENCODE lzo
	,fte_trip_id NUMERIC(15,0)   ENCODE az64
	,reprice_required VARCHAR(1)   ENCODE lzo
	,actual_arrival_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,actual_departure_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,actual_receipt_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,tracking_drilldown_flag VARCHAR(1)   ENCODE lzo
	,status_code VARCHAR(30)   ENCODE lzo
	,tracking_remarks VARCHAR(4000)   ENCODE lzo
	,carrier_est_departure_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,carrier_est_arrival_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,loading_start_datetime TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,loading_end_datetime TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,unloading_start_datetime TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,unloading_end_datetime TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,delivered_quantity NUMERIC(28,10)   ENCODE az64
	,loaded_quantity NUMERIC(28,10)   ENCODE az64
	,received_quantity NUMERIC(28,10)   ENCODE az64
	,origin_stop_id NUMERIC(15,0)   ENCODE az64
	,destination_stop_id NUMERIC(15,0)   ENCODE az64
	,parent_delivery_leg_id NUMERIC(15,0)
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.WSH_DELIVERY_LEGS (
	delivery_leg_id,
	delivery_id,
	sequence_number,
	loading_order_flag,
	pick_up_stop_id,
	drop_off_stop_id,
	gross_weight,
	net_weight,
	weight_uom_code,
	volume,
	volume_uom_code,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	load_tender_status,
	shipper_title,
	shipper_phone,
	pod_flag,
	pod_by,
	pod_date,
	expected_pod_date,
	booking_office,
	shipper_export_ref,
	carrier_export_ref,
	doc_notify_party,
	aetc_number,
	shipper_signed_by,
	shipper_date,
	carrier_signed_by,
	carrier_date,
	doc_issue_office,
	doc_issued_by,
	doc_date_issued,
	shipper_hm_by,
	shipper_hm_date,
	carrier_hm_by,
	carrier_hm_date,
	booking_number,
	port_of_loading,
	port_of_discharge,
	service_contract,
	bill_freight_to,
	fte_trip_id,
	reprice_required,
	actual_arrival_date,
	actual_departure_date,
	actual_receipt_date,
	tracking_drilldown_flag,
	status_code,
	tracking_remarks,
	carrier_est_departure_date,
	carrier_est_arrival_date,
	loading_start_datetime,
	loading_end_datetime,
	unloading_start_datetime,
	unloading_end_datetime,
	delivered_quantity,
	loaded_quantity,
	received_quantity,
	origin_stop_id,
	destination_stop_id,
	parent_delivery_leg_id,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date 
)
    SELECT
		delivery_leg_id,
	delivery_id,
	sequence_number,
	loading_order_flag,
	pick_up_stop_id,
	drop_off_stop_id,
	gross_weight,
	net_weight,
	weight_uom_code,
	volume,
	volume_uom_code,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	load_tender_status,
	shipper_title,
	shipper_phone,
	pod_flag,
	pod_by,
	pod_date,
	expected_pod_date,
	booking_office,
	shipper_export_ref,
	carrier_export_ref,
	doc_notify_party,
	aetc_number,
	shipper_signed_by,
	shipper_date,
	carrier_signed_by,
	carrier_date,
	doc_issue_office,
	doc_issued_by,
	doc_date_issued,
	shipper_hm_by,
	shipper_hm_date,
	carrier_hm_by,
	carrier_hm_date,
	booking_number,
	port_of_loading,
	port_of_discharge,
	service_contract,
	bill_freight_to,
	fte_trip_id,
	reprice_required,
	actual_arrival_date,
	actual_departure_date,
	actual_receipt_date,
	tracking_drilldown_flag,
	status_code,
	tracking_remarks,
	carrier_est_departure_date,
	carrier_est_arrival_date,
	loading_start_datetime,
	loading_end_datetime,
	unloading_start_datetime,
	unloading_end_datetime,
	delivered_quantity,
	loaded_quantity,
	received_quantity,
	origin_stop_id,
	destination_stop_id,
	parent_delivery_leg_id,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
    FROM
        bec_ods_stg.WSH_DELIVERY_LEGS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'wsh_delivery_legs';
	
commit;