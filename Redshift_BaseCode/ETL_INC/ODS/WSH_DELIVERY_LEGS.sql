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

delete from bec_ods.WSH_DELIVERY_LEGS
where (nvl(DELIVERY_LEG_ID,0)) in (
select nvl(stg.DELIVERY_LEG_ID,0) as DELIVERY_LEG_ID from bec_ods.WSH_DELIVERY_LEGS ods, bec_ods_stg.WSH_DELIVERY_LEGS stg
where nvl(ods.DELIVERY_LEG_ID,0) = nvl(stg.DELIVERY_LEG_ID,0) and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.WSH_DELIVERY_LEGS
       (	
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
		,kca_seq_date)	
(
	select
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
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.WSH_DELIVERY_LEGS
	where kca_operation in ('INSERT','UPDATE') 
	and (nvl(DELIVERY_LEG_ID,0),kca_seq_id) in 
	(select nvl(DELIVERY_LEG_ID,0) as DELIVERY_LEG_ID,max(kca_seq_id) from bec_ods_stg.WSH_DELIVERY_LEGS 
     where kca_operation in ('INSERT','UPDATE')
     group by nvl(DELIVERY_LEG_ID,0))
);

commit;



-- Soft delete
update bec_ods.WSH_DELIVERY_LEGS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.WSH_DELIVERY_LEGS set IS_DELETED_FLG = 'Y'
where (DELIVERY_LEG_ID)  in
(
select DELIVERY_LEG_ID from bec_raw_dl_ext.WSH_DELIVERY_LEGS
where (DELIVERY_LEG_ID,KCA_SEQ_ID)
in 
(
select DELIVERY_LEG_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.WSH_DELIVERY_LEGS
group by DELIVERY_LEG_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'wsh_delivery_legs';

commit;