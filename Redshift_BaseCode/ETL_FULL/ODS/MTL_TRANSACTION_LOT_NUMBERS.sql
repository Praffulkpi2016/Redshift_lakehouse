/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full incremental approach for ODS.
# File Version: KPI v1.0
*/
begin;
drop table if exists bec_ods.MTL_TRANSACTION_LOT_NUMBERS;

CREATE TABLE IF NOT EXISTS bec_ods.MTL_TRANSACTION_LOT_NUMBERS
(
	transaction_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,transaction_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,transaction_source_id NUMERIC(15,0)   ENCODE az64
	,transaction_source_type_id NUMERIC(15,0)   ENCODE az64
	,transaction_source_name VARCHAR(240)   ENCODE lzo
	,transaction_quantity NUMERIC(38,10)   ENCODE az64
	,primary_quantity NUMERIC(38,10)   ENCODE az64
	,lot_number VARCHAR(80)   ENCODE lzo
	,serial_transaction_id NUMERIC(15,0)   ENCODE az64
	,description VARCHAR(256)   ENCODE lzo
	,vendor_name VARCHAR(240)   ENCODE lzo
	,supplier_lot_number VARCHAR(150)   ENCODE lzo
	,origination_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,date_code VARCHAR(150)   ENCODE lzo
	,grade_code VARCHAR(150)   ENCODE lzo
	,change_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,maturity_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,status_id NUMERIC(15,0)   ENCODE az64
	,retest_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,age NUMERIC(15,0)   ENCODE az64
	,item_size NUMERIC(15,0)   ENCODE az64
	,color VARCHAR(150)   ENCODE lzo
	,volume NUMERIC(15,0)   ENCODE az64
	,volume_uom VARCHAR(3)   ENCODE lzo
	,place_of_origin VARCHAR(150)   ENCODE lzo
	,best_by_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,length NUMERIC(28,10)   ENCODE az64
	,length_uom VARCHAR(3)   ENCODE lzo
	,width NUMERIC(28,10)   ENCODE az64
	,width_uom VARCHAR(3)   ENCODE lzo
	,recycled_content NUMERIC(28,10)   ENCODE az64
	,thickness NUMERIC(28,10)   ENCODE az64
	,thickness_uom VARCHAR(3)   ENCODE lzo
	,curl_wrinkle_fold VARCHAR(150)   ENCODE lzo
	,lot_attribute_category VARCHAR(30)   ENCODE lzo
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
	,vendor_id NUMERIC(15,0)   ENCODE az64
	,territory_code VARCHAR(30)   ENCODE lzo
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
	,expiration_action_code VARCHAR(32)   ENCODE lzo
	,expiration_action_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,hold_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,origination_type NUMERIC(15,0)   ENCODE az64
	,parent_lot_number VARCHAR(80)   ENCODE lzo
	,reason_id NUMERIC(15,0)   ENCODE az64
	,secondary_transaction_quantity NUMERIC(38,10)   ENCODE az64
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
	,kill_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,country_of_origin VARCHAR(30)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
	,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;

insert into bec_ods.MTL_TRANSACTION_LOT_NUMBERS
(
transaction_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	inventory_item_id,
	organization_id,
	transaction_date,
	transaction_source_id,
	transaction_source_type_id,
	transaction_source_name,
	transaction_quantity,
	primary_quantity,
	lot_number,
	serial_transaction_id,
	description,
	vendor_name,
	supplier_lot_number,
	origination_date,
	date_code,
	grade_code,
	change_date,
	maturity_date,
	status_id,
	retest_date,
	age,
	item_size,
	color,
	volume,
	volume_uom,
	place_of_origin,
	best_by_date,
	length,
	length_uom,
	width,
	width_uom,
	recycled_content,
	thickness,
	thickness_uom,
	curl_wrinkle_fold,
	lot_attribute_category,
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
	vendor_id,
	territory_code,
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
	expiration_action_code,
	expiration_action_date,
	hold_date,
	origination_type,
	parent_lot_number,
	reason_id,
	secondary_transaction_quantity,
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
	kill_date,
	country_of_origin,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
select 
	transaction_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	inventory_item_id,
	organization_id,
	transaction_date,
	transaction_source_id,
	transaction_source_type_id,
	transaction_source_name,
	transaction_quantity,
	primary_quantity,
	lot_number,
	serial_transaction_id,
	description,
	vendor_name,
	supplier_lot_number,
	origination_date,
	date_code,
	grade_code,
	change_date,
	maturity_date,
	status_id,
	retest_date,
	age,
	item_size,
	color,
	volume,
	volume_uom,
	place_of_origin,
	best_by_date,
	length,
	length_uom,
	width,
	width_uom,
	recycled_content,
	thickness,
	thickness_uom,
	curl_wrinkle_fold,
	lot_attribute_category,
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
	vendor_id,
	territory_code,
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
	expiration_action_code,
	expiration_action_date,
	hold_date,
	origination_type,
	parent_lot_number,
	reason_id,
	secondary_transaction_quantity,
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
	kill_date,
	country_of_origin,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date 
from bec_ods_stg.MTL_TRANSACTION_LOT_NUMBERS
);

end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='mtl_transaction_lot_numbers'; 

commit;