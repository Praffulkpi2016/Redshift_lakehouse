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

DROP TABLE if exists bec_ods.MTL_SERIAL_NUMBERS_TEMP;

CREATE TABLE IF NOT EXISTS bec_ods.MTL_SERIAL_NUMBERS_TEMP
(
		transaction_temp_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,vendor_serial_number VARCHAR(30)   ENCODE lzo
	,vendor_lot_number VARCHAR(30)   ENCODE lzo
	,fm_serial_number VARCHAR(30)   ENCODE lzo
	,to_serial_number VARCHAR(30)   ENCODE lzo
	,serial_prefix VARCHAR(30)   ENCODE lzo
	,error_code VARCHAR(30)   ENCODE lzo
	,parent_serial_number VARCHAR(30)   ENCODE lzo
	,group_header_id NUMERIC(15,0)   ENCODE az64
	,end_item_unit_number VARCHAR(30)   ENCODE lzo
	,serial_attribute_category VARCHAR(30)   ENCODE lzo
	,country_of_origin VARCHAR(30)   ENCODE lzo
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
	,c_attribute21 VARCHAR(30)   ENCODE lzo
	,c_attribute22 VARCHAR(30)   ENCODE lzo
	,c_attribute23 VARCHAR(30)   ENCODE lzo
	,c_attribute24 VARCHAR(30)   ENCODE lzo
	,c_attribute25 VARCHAR(30)   ENCODE lzo
	,c_attribute26 VARCHAR(30)   ENCODE lzo
	,c_attribute27 VARCHAR(30)   ENCODE lzo
	,c_attribute28 VARCHAR(30)   ENCODE lzo
	,c_attribute29 VARCHAR(30)   ENCODE lzo
	,c_attribute30 VARCHAR(30)   ENCODE lzo
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
	,n_attribute1 NUMERIC(28,10)   ENCODE az64
	,n_attribute2 NUMERIC(28,10)   ENCODE az64
	,n_attribute3 NUMERIC(28,10)   ENCODE az64
	,n_attribute4 NUMERIC(28,10)   ENCODE az64
	,n_attribute5 NUMERIC(28,10)   ENCODE az64
	,n_attribute6 NUMERIC(28,10)   ENCODE az64
	,n_attribute7 NUMERIC(28,10)   ENCODE az64
	,n_attribute8 NUMERIC(28,10)   ENCODE az64
	,n_attribute9 NUMERIC(28,10)   ENCODE az64
	,n_attribute10 NUMERIC(28,10)   ENCODE az64
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
	,n_attribute30 NUMERIC(30,0)   ENCODE az64
	--,n_attribute15 NUMERIC(15,0)   ENCODE az64
	,status_id NUMERIC(15,0)   ENCODE az64
	,territory_code VARCHAR(30)   ENCODE lzo
	,time_since_new NUMERIC(28,10)   ENCODE az64
	,cycles_since_new NUMERIC(28,10)   ENCODE az64
	,time_since_overhaul NUMERIC(28,10)   ENCODE az64
	,cycles_since_overhaul NUMERIC(28,10)   ENCODE az64
	,time_since_repair NUMERIC(28,10)   ENCODE az64
	,cycles_since_repair NUMERIC(28,10)   ENCODE az64
	,time_since_visit NUMERIC(28,10)   ENCODE az64
	,cycles_since_visit NUMERIC(28,10)   ENCODE az64
	,time_since_mark NUMERIC(15,0)   ENCODE az64
	,cycles_since_mark NUMERIC(28,10)   ENCODE az64
	,numer_of_repairs VARCHAR(30)   ENCODE lzo
	,number_of_repairs NUMERIC(28,10)   ENCODE az64
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
	,dff_updated_flag VARCHAR(1)   ENCODE lzo
	,parent_object_type NUMERIC(15,0)   ENCODE az64
	,parent_object_id NUMERIC(15,0)   ENCODE az64
	,parent_object_number VARCHAR(240)   ENCODE lzo
	,parent_item_id NUMERIC(15,0)   ENCODE az64
	,parent_object_type2 NUMERIC(15,0)   ENCODE az64
	,parent_object_id2 NUMERIC(15,0)   ENCODE az64
	,parent_object_number2 VARCHAR(240)   ENCODE lzo
	,object_type2 NUMERIC(15,0)   ENCODE az64
	,object_number2 VARCHAR(240)   ENCODE lzo
	,last_consumed_serial VARCHAR(150)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MTL_SERIAL_NUMBERS_TEMP (
	TRANSACTION_TEMP_ID
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      VENDOR_SERIAL_NUMBER
,      VENDOR_LOT_NUMBER
,      FM_SERIAL_NUMBER
,      TO_SERIAL_NUMBER
,      SERIAL_PREFIX
,      ERROR_CODE
,      PARENT_SERIAL_NUMBER
,      GROUP_HEADER_ID
,      END_ITEM_UNIT_NUMBER
,      SERIAL_ATTRIBUTE_CATEGORY
,      COUNTRY_OF_ORIGIN
,      ORIGINATION_DATE
,      C_ATTRIBUTE1
,      C_ATTRIBUTE2
,      C_ATTRIBUTE3
,      C_ATTRIBUTE4
,      C_ATTRIBUTE5
,      C_ATTRIBUTE6
,      C_ATTRIBUTE7
,      C_ATTRIBUTE8
,      C_ATTRIBUTE9
,      C_ATTRIBUTE10
,      C_ATTRIBUTE11
,      C_ATTRIBUTE12
,      C_ATTRIBUTE13
,      C_ATTRIBUTE14
,      C_ATTRIBUTE15
,      C_ATTRIBUTE16
,      C_ATTRIBUTE17
,      C_ATTRIBUTE18
,      C_ATTRIBUTE19
,      C_ATTRIBUTE20
,      C_ATTRIBUTE21
,      C_ATTRIBUTE22
,      C_ATTRIBUTE23
,      C_ATTRIBUTE24
,      C_ATTRIBUTE25
,      C_ATTRIBUTE26
,      C_ATTRIBUTE27
,      C_ATTRIBUTE28
,      C_ATTRIBUTE29
,      C_ATTRIBUTE30
,      D_ATTRIBUTE1
,      D_ATTRIBUTE2
,      D_ATTRIBUTE3
,      D_ATTRIBUTE4
,      D_ATTRIBUTE5
,      D_ATTRIBUTE6
,      D_ATTRIBUTE7
,      D_ATTRIBUTE8
,      D_ATTRIBUTE9
,      D_ATTRIBUTE10
,      D_ATTRIBUTE11
,      D_ATTRIBUTE12
,      D_ATTRIBUTE13
,      D_ATTRIBUTE14
,      D_ATTRIBUTE15
,      D_ATTRIBUTE16
,      D_ATTRIBUTE17
,      D_ATTRIBUTE18
,      D_ATTRIBUTE19
,      D_ATTRIBUTE20
,      N_ATTRIBUTE1
,      N_ATTRIBUTE2
,      N_ATTRIBUTE3
,      N_ATTRIBUTE4
,      N_ATTRIBUTE5
,      N_ATTRIBUTE6
,      N_ATTRIBUTE7
,      N_ATTRIBUTE8
,      N_ATTRIBUTE9
,      N_ATTRIBUTE10
,      N_ATTRIBUTE11
,      N_ATTRIBUTE12
,      N_ATTRIBUTE13
,      N_ATTRIBUTE14
,      N_ATTRIBUTE15
,      N_ATTRIBUTE16
,      N_ATTRIBUTE17
,      N_ATTRIBUTE18
,      N_ATTRIBUTE19
,      N_ATTRIBUTE20
,      N_ATTRIBUTE21
,      N_ATTRIBUTE22
,      N_ATTRIBUTE23
,      N_ATTRIBUTE24
,      N_ATTRIBUTE25
,      N_ATTRIBUTE26
,      N_ATTRIBUTE27
,      N_ATTRIBUTE28
,      N_ATTRIBUTE29
,      N_ATTRIBUTE30
,      STATUS_ID
,      TERRITORY_CODE
,      TIME_SINCE_NEW
,      CYCLES_SINCE_NEW
,      TIME_SINCE_OVERHAUL
,      CYCLES_SINCE_OVERHAUL
,      TIME_SINCE_REPAIR
,      CYCLES_SINCE_REPAIR
,      TIME_SINCE_VISIT
,      CYCLES_SINCE_VISIT
,      TIME_SINCE_MARK
,      CYCLES_SINCE_MARK
,      NUMER_OF_REPAIRS
,      NUMBER_OF_REPAIRS
,      PRODUCT_CODE
,      PRODUCT_TRANSACTION_ID
,      ATTRIBUTE_CATEGORY
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
,      DFF_UPDATED_FLAG
,      PARENT_OBJECT_TYPE
,      PARENT_OBJECT_ID
,      PARENT_OBJECT_NUMBER
,      PARENT_ITEM_ID
,      PARENT_OBJECT_TYPE2
,      PARENT_OBJECT_ID2
,      PARENT_OBJECT_NUMBER2
,      OBJECT_TYPE2
,      OBJECT_NUMBER2
,      LAST_CONSUMED_SERIAL
,	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
		TRANSACTION_TEMP_ID
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      VENDOR_SERIAL_NUMBER
,      VENDOR_LOT_NUMBER
,      FM_SERIAL_NUMBER
,      TO_SERIAL_NUMBER
,      SERIAL_PREFIX
,      ERROR_CODE
,      PARENT_SERIAL_NUMBER
,      GROUP_HEADER_ID
,      END_ITEM_UNIT_NUMBER
,      SERIAL_ATTRIBUTE_CATEGORY
,      COUNTRY_OF_ORIGIN
,      ORIGINATION_DATE
,      C_ATTRIBUTE1
,      C_ATTRIBUTE2
,      C_ATTRIBUTE3
,      C_ATTRIBUTE4
,      C_ATTRIBUTE5
,      C_ATTRIBUTE6
,      C_ATTRIBUTE7
,      C_ATTRIBUTE8
,      C_ATTRIBUTE9
,      C_ATTRIBUTE10
,      C_ATTRIBUTE11
,      C_ATTRIBUTE12
,      C_ATTRIBUTE13
,      C_ATTRIBUTE14
,      C_ATTRIBUTE15
,      C_ATTRIBUTE16
,      C_ATTRIBUTE17
,      C_ATTRIBUTE18
,      C_ATTRIBUTE19
,      C_ATTRIBUTE20
,      C_ATTRIBUTE21
,      C_ATTRIBUTE22
,      C_ATTRIBUTE23
,      C_ATTRIBUTE24
,      C_ATTRIBUTE25
,      C_ATTRIBUTE26
,      C_ATTRIBUTE27
,      C_ATTRIBUTE28
,      C_ATTRIBUTE29
,      C_ATTRIBUTE30
,      D_ATTRIBUTE1
,      D_ATTRIBUTE2
,      D_ATTRIBUTE3
,      D_ATTRIBUTE4
,      D_ATTRIBUTE5
,      D_ATTRIBUTE6
,      D_ATTRIBUTE7
,      D_ATTRIBUTE8
,      D_ATTRIBUTE9
,      D_ATTRIBUTE10
,      D_ATTRIBUTE11
,      D_ATTRIBUTE12
,      D_ATTRIBUTE13
,      D_ATTRIBUTE14
,      D_ATTRIBUTE15
,      D_ATTRIBUTE16
,      D_ATTRIBUTE17
,      D_ATTRIBUTE18
,      D_ATTRIBUTE19
,      D_ATTRIBUTE20
,      N_ATTRIBUTE1
,      N_ATTRIBUTE2
,      N_ATTRIBUTE3
,      N_ATTRIBUTE4
,      N_ATTRIBUTE5
,      N_ATTRIBUTE6
,      N_ATTRIBUTE7
,      N_ATTRIBUTE8
,      N_ATTRIBUTE9
,      N_ATTRIBUTE10
,      N_ATTRIBUTE11
,      N_ATTRIBUTE12
,      N_ATTRIBUTE13
,      N_ATTRIBUTE14
,      N_ATTRIBUTE15
,      N_ATTRIBUTE16
,      N_ATTRIBUTE17
,      N_ATTRIBUTE18
,      N_ATTRIBUTE19
,      N_ATTRIBUTE20
,      N_ATTRIBUTE21
,      N_ATTRIBUTE22
,      N_ATTRIBUTE23
,      N_ATTRIBUTE24
,      N_ATTRIBUTE25
,      N_ATTRIBUTE26
,      N_ATTRIBUTE27
,      N_ATTRIBUTE28
,      N_ATTRIBUTE29
,      N_ATTRIBUTE30
,      STATUS_ID
,      TERRITORY_CODE
,      TIME_SINCE_NEW
,      CYCLES_SINCE_NEW
,      TIME_SINCE_OVERHAUL
,      CYCLES_SINCE_OVERHAUL
,      TIME_SINCE_REPAIR
,      CYCLES_SINCE_REPAIR
,      TIME_SINCE_VISIT
,      CYCLES_SINCE_VISIT
,      TIME_SINCE_MARK
,      CYCLES_SINCE_MARK
,      NUMER_OF_REPAIRS
,      NUMBER_OF_REPAIRS
,      PRODUCT_CODE
,      PRODUCT_TRANSACTION_ID
,      ATTRIBUTE_CATEGORY
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
,      DFF_UPDATED_FLAG
,      PARENT_OBJECT_TYPE
,      PARENT_OBJECT_ID
,      PARENT_OBJECT_NUMBER
,      PARENT_ITEM_ID
,      PARENT_OBJECT_TYPE2
,      PARENT_OBJECT_ID2
,      PARENT_OBJECT_NUMBER2
,      OBJECT_TYPE2
,      OBJECT_NUMBER2
,      LAST_CONSUMED_SERIAL
		,KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.MTL_SERIAL_NUMBERS_TEMP;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'mtl_serial_numbers_temp';
	
commit;