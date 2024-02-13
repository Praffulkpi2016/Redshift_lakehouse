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

delete
from
	bec_ods.MTL_TXN_REQUEST_HEADERS
where
	(
	NVL(HEADER_ID,0) 

	) in 
	(
	select
		NVL(stg.HEADER_ID,0) AS HEADER_ID 
	from
		bec_ods.MTL_TXN_REQUEST_HEADERS ods,
		bec_ods_stg.MTL_TXN_REQUEST_HEADERS stg
	where
	NVL(ods.HEADER_ID,0) = NVL(stg.HEADER_ID,0) 
	and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.MTL_TXN_REQUEST_HEADERS
    (header_id,
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
	kca_operation,
	IS_DELETED_FLG,
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
	kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
	kca_seq_date
	from
		bec_ods_stg.MTL_TXN_REQUEST_HEADERS
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		NVL(HEADER_ID,0), 
		KCA_SEQ_ID
		) in 
	(
		select
			NVL(HEADER_ID,0) AS HEADER_ID ,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.MTL_TXN_REQUEST_HEADERS
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			NVL(HEADER_ID,0) 
			)	
	);

commit;

 
-- Soft delete
update bec_ods.MTL_TXN_REQUEST_HEADERS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_TXN_REQUEST_HEADERS set IS_DELETED_FLG = 'Y'
where (HEADER_ID)  in
(
select HEADER_ID from bec_raw_dl_ext.MTL_TXN_REQUEST_HEADERS
where (HEADER_ID,KCA_SEQ_ID)
in 
(
select HEADER_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_TXN_REQUEST_HEADERS
group by HEADER_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;
 

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'mtl_txn_request_headers';

commit;