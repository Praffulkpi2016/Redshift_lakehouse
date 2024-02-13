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
	bec_ods.MTL_UNIT_TRANSACTIONS
where
	(TRANSACTION_ID,SERIAL_NUMBER) in (
	select
		stg.TRANSACTION_ID,stg.SERIAL_NUMBER
	from
		bec_ods.MTL_UNIT_TRANSACTIONS ods,
		bec_ods_stg.MTL_UNIT_TRANSACTIONS stg
	where
		ods.TRANSACTION_ID = stg.TRANSACTION_ID
		and ods.SERIAL_NUMBER = stg.SERIAL_NUMBER
		and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.MTL_UNIT_TRANSACTIONS
       (
    transaction_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	serial_number,
	inventory_item_id,
	organization_id,
	subinventory_code,
	locator_id,
	transaction_date,
	transaction_source_id,
	transaction_source_type_id,
	transaction_source_name,
	receipt_issue_type,
	customer_id,
	ship_id,
	serial_attribute_category,
	origination_date,
	c_attribute1,
	c_attribute2,
	c_attribute3,
	c_attribute4,
	c_attribute5,
	c_attribute6,
	c_attribute7,
	c_attribute8,
	c_attribute9,
	c_attribute10,
	c_attribute11,
	c_attribute12,
	c_attribute13,
	c_attribute14,
	c_attribute15,
	c_attribute16,
	c_attribute17,
	c_attribute18,
	c_attribute19,
	c_attribute20,
	d_attribute1,
	d_attribute2,
	d_attribute3,
	d_attribute4,
	d_attribute5,
	d_attribute6,
	d_attribute7,
	d_attribute8,
	d_attribute9,
	d_attribute10,
	n_attribute1,
	n_attribute2,
	n_attribute3,
	n_attribute4,
	n_attribute5,
	n_attribute6,
	n_attribute7,
	n_attribute8,
	n_attribute9,
	n_attribute10,
	status_id,
	territory_code,
	time_since_new,
	cycles_since_new,
	time_since_overhaul,
	cycles_since_overhaul,
	time_since_repair,
	cycles_since_repair,
	time_since_visit,
	cycles_since_visit,
	time_since_mark,
	cycles_since_mark,
	number_of_repairs,
	product_code,
	product_transaction_id,
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
	parent_object_type,
	parent_object_id,
	parent_object_number,
	parent_item_id,
	parent_object_type2,
	parent_object_id2,
	parent_object_number2,
	c_attribute21,
	c_attribute22,
	c_attribute23,
	c_attribute24,
	c_attribute25,
	c_attribute26,
	c_attribute27,
	c_attribute28,
	c_attribute29,
	c_attribute30,
	d_attribute11,
	d_attribute12,
	d_attribute13,
	d_attribute14,
	d_attribute15,
	d_attribute16,
	d_attribute17,
	d_attribute18,
	d_attribute19,
	d_attribute20,
	d_attribute21,
	d_attribute22,
	d_attribute23,
	d_attribute24,
	d_attribute25,
	d_attribute26,
	d_attribute27,
	d_attribute28,
	d_attribute29,
	d_attribute30,
	n_attribute11,
	n_attribute12,
	n_attribute13,
	n_attribute14,
	n_attribute15,
	n_attribute16,
	n_attribute17,
	n_attribute18,
	n_attribute19,
	n_attribute20,
	n_attribute21,
	n_attribute22,
	n_attribute23,
	n_attribute24,
	n_attribute25,
	n_attribute26,
	n_attribute27,
	n_attribute28,
	n_attribute29,
	n_attribute30,
	country_of_origin,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
(
	select
		transaction_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	serial_number,
	inventory_item_id,
	organization_id,
	subinventory_code,
	locator_id,
	transaction_date,
	transaction_source_id,
	transaction_source_type_id,
	transaction_source_name,
	receipt_issue_type,
	customer_id,
	ship_id,
	serial_attribute_category,
	origination_date,
	c_attribute1,
	c_attribute2,
	c_attribute3,
	c_attribute4,
	c_attribute5,
	c_attribute6,
	c_attribute7,
	c_attribute8,
	c_attribute9,
	c_attribute10,
	c_attribute11,
	c_attribute12,
	c_attribute13,
	c_attribute14,
	c_attribute15,
	c_attribute16,
	c_attribute17,
	c_attribute18,
	c_attribute19,
	c_attribute20,
	d_attribute1,
	d_attribute2,
	d_attribute3,
	d_attribute4,
	d_attribute5,
	d_attribute6,
	d_attribute7,
	d_attribute8,
	d_attribute9,
	d_attribute10,
	n_attribute1,
	n_attribute2,
	n_attribute3,
	n_attribute4,
	n_attribute5,
	n_attribute6,
	n_attribute7,
	n_attribute8,
	n_attribute9,
	n_attribute10,
	status_id,
	territory_code,
	time_since_new,
	cycles_since_new,
	time_since_overhaul,
	cycles_since_overhaul,
	time_since_repair,
	cycles_since_repair,
	time_since_visit,
	cycles_since_visit,
	time_since_mark,
	cycles_since_mark,
	number_of_repairs,
	product_code,
	product_transaction_id,
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
	parent_object_type,
	parent_object_id,
	parent_object_number,
	parent_item_id,
	parent_object_type2,
	parent_object_id2,
	parent_object_number2,
	c_attribute21,
	c_attribute22,
	c_attribute23,
	c_attribute24,
	c_attribute25,
	c_attribute26,
	c_attribute27,
	c_attribute28,
	c_attribute29,
	c_attribute30,
	d_attribute11,
	d_attribute12,
	d_attribute13,
	d_attribute14,
	d_attribute15,
	d_attribute16,
	d_attribute17,
	d_attribute18,
	d_attribute19,
	d_attribute20,
	d_attribute21,
	d_attribute22,
	d_attribute23,
	d_attribute24,
	d_attribute25,
	d_attribute26,
	d_attribute27,
	d_attribute28,
	d_attribute29,
	d_attribute30,
	n_attribute11,
	n_attribute12,
	n_attribute13,
	n_attribute14,
	n_attribute15,
	n_attribute16,
	n_attribute17,
	n_attribute18,
	n_attribute19,
	n_attribute20,
	n_attribute21,
	n_attribute22,
	n_attribute23,
	n_attribute24,
	n_attribute25,
	n_attribute26,
	n_attribute27,
	n_attribute28,
	n_attribute29,
	n_attribute30,
	country_of_origin,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
	,kca_seq_date
	from
		bec_ods_stg.MTL_UNIT_TRANSACTIONS
	where
		kca_operation in ('INSERT','UPDATE')
		and (TRANSACTION_ID,SERIAL_NUMBER,kca_seq_id) in 
	(
		select
			TRANSACTION_ID,SERIAL_NUMBER,max(kca_seq_id)
		from
			bec_ods_stg.MTL_UNIT_TRANSACTIONS
		where
			kca_operation in ('INSERT','UPDATE')
		group by
			TRANSACTION_ID,SERIAL_NUMBER)
);

commit;

-- Soft delete
update bec_ods.MTL_UNIT_TRANSACTIONS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_UNIT_TRANSACTIONS set IS_DELETED_FLG = 'Y'
where (TRANSACTION_ID,SERIAL_NUMBER)  in
(
select TRANSACTION_ID,SERIAL_NUMBER from bec_raw_dl_ext.MTL_UNIT_TRANSACTIONS
where (TRANSACTION_ID,SERIAL_NUMBER,KCA_SEQ_ID)
in 
(
select TRANSACTION_ID,SERIAL_NUMBER,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_UNIT_TRANSACTIONS
group by TRANSACTION_ID,SERIAL_NUMBER
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
	ods_table_name = 'mtl_unit_transactions';

commit;