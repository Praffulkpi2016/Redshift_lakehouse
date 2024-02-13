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
begin;

truncate table bec_ods_stg.MTL_TXN_REQUEST_HEADERS;

insert into	bec_ods_stg.MTL_TXN_REQUEST_HEADERS
   (
	header_id,
	request_number,
	transaction_type_id,
	move_order_type,
	organization_id,
	description,
	date_required,
	from_subinventory_code,
	to_subinventory_code,
	to_account_id,
	header_status,
	status_date,
	last_updated_by,
	last_update_login,
	last_update_date,
	created_by,
	creation_date,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	grouping_rule_id,
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
	attribute_category,
	ship_to_location_id,
	freight_code,
	shipment_method,
	auto_receipt_flag,
	reference_id,
	reference_detail_id,
	assignment_id,
	--ZD_EDITION_NAME,
	--ZD_SYNC,
	kca_operation,
	kca_seq_id,
	kca_seq_date)
(
	select		
	header_id,
	request_number,
	transaction_type_id,
	move_order_type,
	organization_id,
	description,
	date_required,
	from_subinventory_code,
	to_subinventory_code,
	to_account_id,
	header_status,
	status_date,
	last_updated_by,
	last_update_login,
	last_update_date,
	created_by,
	creation_date,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	grouping_rule_id,
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
	attribute_category,
	ship_to_location_id,
	freight_code,
	shipment_method,
	auto_receipt_flag,
	reference_id,
	reference_detail_id,
	assignment_id,
	--ZD_EDITION_NAME,
	--ZD_SYNC,
	kca_operation,
	kca_seq_id,
	kca_seq_date from bec_raw_dl_ext.MTL_TXN_REQUEST_HEADERS
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (nvl(HEADER_ID,0),kca_seq_id) in 
	(select nvl(HEADER_ID,0) as HEADER_ID,max(kca_seq_id) from bec_raw_dl_ext.MTL_TXN_REQUEST_HEADERS 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by nvl(HEADER_ID,0))
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'mtl_txn_request_headers')
		 
            )	
);
end;