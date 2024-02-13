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
drop table if exists bec_ods.MTL_UNIT_TRANSACTIONS;

CREATE TABLE IF NOT EXISTS bec_ods.MTL_UNIT_TRANSACTIONS
(
	transaction_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,serial_number VARCHAR(30)   ENCODE lzo
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,subinventory_code VARCHAR(10)   ENCODE lzo
	,locator_id NUMERIC(15,0)   ENCODE az64
	,transaction_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,transaction_source_id NUMERIC(15,0)   ENCODE az64
	,transaction_source_type_id NUMERIC(15,0)   ENCODE az64
	,transaction_source_name VARCHAR(240)   ENCODE lzo
	,receipt_issue_type NUMERIC(15,0)   ENCODE az64
	,customer_id NUMERIC(15,0)   ENCODE az64
	,ship_id NUMERIC(15,0)   ENCODE az64
	,serial_attribute_category VARCHAR(30)   ENCODE lzo
	,origination_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,c_attribute1 VARCHAR(150)   ENCODE lzo
	,c_attribute2 VARCHAR(150)   ENCODE lzo
	,c_attribute3 VARCHAR(150)   ENCODE lzo
	,c_attribute4 VARCHAR(150)   ENCODE lzo
	,c_attribute5 VARCHAR(150)   ENCODE lzo
	,c_attribute6 VARCHAR(150)   ENCODE lzo
	,c_attribute7 VARCHAR(150)   ENCODE lzo
	,c_attribute8 VARCHAR(150)   ENCODE lzo
	,c_attribute9 VARCHAR(150)   ENCODE lzo
	,c_attribute10 VARCHAR(150)   ENCODE lzo
	,c_attribute11 VARCHAR(150)   ENCODE lzo
	,c_attribute12 VARCHAR(150)   ENCODE lzo
	,c_attribute13 VARCHAR(150)   ENCODE lzo
	,c_attribute14 VARCHAR(150)   ENCODE lzo
	,c_attribute15 VARCHAR(150)   ENCODE lzo
	,c_attribute16 VARCHAR(150)   ENCODE lzo
	,c_attribute17 VARCHAR(150)   ENCODE lzo
	,c_attribute18 VARCHAR(150)   ENCODE lzo
	,c_attribute19 VARCHAR(150)   ENCODE lzo
	,c_attribute20 VARCHAR(150)   ENCODE lzo
	,d_attribute1 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute2 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute3 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute4 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute5 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute6 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute7 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute8 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute9 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute10 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,n_attribute1 NUMERIC(15,0)   ENCODE az64
	,n_attribute2 NUMERIC(15,0)   ENCODE az64
	,n_attribute3 NUMERIC(15,0)   ENCODE az64
	,n_attribute4 NUMERIC(15,0)   ENCODE az64
	,n_attribute5 NUMERIC(15,0)   ENCODE az64
	,n_attribute6 NUMERIC(15,0)   ENCODE az64
	,n_attribute7 NUMERIC(15,0)   ENCODE az64
	,n_attribute8 NUMERIC(15,0)   ENCODE az64
	,n_attribute9 NUMERIC(15,0)   ENCODE az64
	,n_attribute10 NUMERIC(15,0)   ENCODE az64
	,status_id NUMERIC(15,0)   ENCODE az64
	,territory_code VARCHAR(30)   ENCODE lzo
	,time_since_new NUMERIC(15,0)   ENCODE az64
	,cycles_since_new NUMERIC(15,0)   ENCODE az64
	,time_since_overhaul NUMERIC(15,0)   ENCODE az64
	,cycles_since_overhaul NUMERIC(15,0)   ENCODE az64
	,time_since_repair NUMERIC(15,0)   ENCODE az64
	,cycles_since_repair NUMERIC(15,0)   ENCODE az64
	,time_since_visit NUMERIC(15,0)   ENCODE az64
	,cycles_since_visit NUMERIC(15,0)   ENCODE az64
	,time_since_mark NUMERIC(15,0)   ENCODE az64
	,cycles_since_mark NUMERIC(15,0)   ENCODE az64
	,number_of_repairs NUMERIC(15,0)   ENCODE az64
	,product_code VARCHAR(5)   ENCODE lzo
	,product_transaction_id NUMERIC(15,0)   ENCODE az64
	,attribute_category VARCHAR(30)   ENCODE lzo
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
	,parent_object_type NUMERIC(15,0)   ENCODE az64
	,parent_object_id NUMERIC(15,0)   ENCODE az64
	,parent_object_number VARCHAR(240)   ENCODE lzo
	,parent_item_id NUMERIC(15,0)   ENCODE az64
	,parent_object_type2 NUMERIC(15,0)   ENCODE az64
	,parent_object_id2 NUMERIC(15,0)   ENCODE az64
	,parent_object_number2 VARCHAR(240)   ENCODE lzo
	,c_attribute21 VARCHAR(150)   ENCODE lzo
	,c_attribute22 VARCHAR(150)   ENCODE lzo
	,c_attribute23 VARCHAR(150)   ENCODE lzo
	,c_attribute24 VARCHAR(150)   ENCODE lzo
	,c_attribute25 VARCHAR(150)   ENCODE lzo
	,c_attribute26 VARCHAR(150)   ENCODE lzo
	,c_attribute27 VARCHAR(150)   ENCODE lzo
	,c_attribute28 VARCHAR(150)   ENCODE lzo
	,c_attribute29 VARCHAR(150)   ENCODE lzo
	,c_attribute30 VARCHAR(150)   ENCODE lzo
	,d_attribute11 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute12 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute13 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute14 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute15 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute16 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute17 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute18 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute19 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute20 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute21 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute22 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute23 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute24 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute25 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute26 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute27 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute28 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute29 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,d_attribute30 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,n_attribute11 NUMERIC(15,0)   ENCODE az64
	,n_attribute12 NUMERIC(15,0)   ENCODE az64
	,n_attribute13 NUMERIC(15,0)   ENCODE az64
	,n_attribute14 NUMERIC(15,0)   ENCODE az64
	,n_attribute15 NUMERIC(15,0)   ENCODE az64
	,n_attribute16 NUMERIC(15,0)   ENCODE az64
	,n_attribute17 NUMERIC(15,0)   ENCODE az64
	,n_attribute18 NUMERIC(15,0)   ENCODE az64
	,n_attribute19 NUMERIC(15,0)   ENCODE az64
	,n_attribute20 NUMERIC(15,0)   ENCODE az64
	,n_attribute21 NUMERIC(15,0)   ENCODE az64
	,n_attribute22 NUMERIC(15,0)   ENCODE az64
	,n_attribute23 NUMERIC(15,0)   ENCODE az64
	,n_attribute24 NUMERIC(15,0)   ENCODE az64
	,n_attribute25 NUMERIC(15,0)   ENCODE az64
	,n_attribute26 NUMERIC(15,0)   ENCODE az64
	,n_attribute27 NUMERIC(15,0)   ENCODE az64
	,n_attribute28 NUMERIC(15,0)   ENCODE az64
	,n_attribute29 NUMERIC(15,0)   ENCODE az64
	,n_attribute30 NUMERIC(15,0)   ENCODE az64
	,country_of_origin VARCHAR(30)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
	,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;

insert into bec_ods.MTL_UNIT_TRANSACTIONS
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
	KCA_OPERATION,
	IS_DELETED_FLG,
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
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID 
	,kca_seq_date
from bec_ods_stg.MTL_UNIT_TRANSACTIONS
);

end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='mtl_unit_transactions'; 

commit;