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
drop table if exists bec_ods.MTL_TRANSACTION_TYPES;

CREATE TABLE IF NOT EXISTS bec_ods.MTL_TRANSACTION_TYPES
(
	transaction_type_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,transaction_type_name VARCHAR(80)   ENCODE lzo
	,description VARCHAR(240)   ENCODE lzo
	,transaction_action_id NUMERIC(15,0)   ENCODE az64
	,transaction_source_type_id NUMERIC(15,0)   ENCODE az64
	,disable_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,user_defined_flag VARCHAR(1)   ENCODE lzo
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
	,attribute_category VARCHAR(30)   ENCODE lzo
	,type_class NUMERIC(15,0)   ENCODE az64
	,shortage_msg_background_flag VARCHAR(1)   ENCODE lzo
	,shortage_msg_online_flag VARCHAR(1)   ENCODE lzo
	,status_control_flag NUMERIC(15,0)   ENCODE az64
	,location_required_flag VARCHAR(1)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
	,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;

insert into bec_ods.MTL_TRANSACTION_TYPES
(
transaction_type_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	transaction_type_name,
	description,
	transaction_action_id,
	transaction_source_type_id,
	disable_date,
	user_defined_flag,
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
	type_class,
	shortage_msg_background_flag,
	shortage_msg_online_flag,
	status_control_flag,
	location_required_flag,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
select
transaction_type_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	transaction_type_name,
	description,
	transaction_action_id,
	transaction_source_type_id,
	disable_date,
	user_defined_flag,
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
	type_class,
	shortage_msg_background_flag,
	shortage_msg_online_flag,
	status_control_flag,
	location_required_flag,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date 
from bec_ods_stg.MTL_TRANSACTION_TYPES
);

end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='mtl_transaction_types'; 

commit;