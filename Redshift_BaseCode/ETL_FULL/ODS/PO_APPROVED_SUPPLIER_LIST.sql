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

DROP TABLE if exists bec_ods.PO_APPROVED_SUPPLIER_LIST;

CREATE TABLE IF NOT EXISTS bec_ods.PO_APPROVED_SUPPLIER_LIST
(
	asl_id NUMERIC(15,0)   ENCODE az64
	,using_organization_id NUMERIC(15,0)   ENCODE az64
	,owning_organization_id NUMERIC(15,0)   ENCODE az64
	,vendor_business_type VARCHAR(25)   ENCODE lzo
	,asl_status_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,manufacturer_id NUMERIC(15,0)   ENCODE az64
	,vendor_id NUMERIC(15,0)   ENCODE az64
	,item_id NUMERIC(15,0)   ENCODE az64
	,category_id NUMERIC(15,0)   ENCODE az64
	,vendor_site_id NUMERIC(15,0)   ENCODE az64
	,primary_vendor_item VARCHAR(25)   ENCODE lzo
	,manufacturer_asl_id NUMERIC(15,0)   ENCODE az64
	,review_by_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,comments VARCHAR(240)   ENCODE lzo
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
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,DISABLE_FLAG  VARCHAR(1) ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64	
)
DISTSTYLE
auto;

INSERT INTO bec_ods.PO_APPROVED_SUPPLIER_LIST (
	asl_id,
	using_organization_id,
	owning_organization_id,
	vendor_business_type,
	asl_status_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	manufacturer_id,
	vendor_id,
	item_id,
	category_id,
	vendor_site_id,
	primary_vendor_item,
	manufacturer_asl_id,
	review_by_date,
	comments,
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
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	DISABLE_FLAG,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date	
)
    SELECT
     	asl_id,
	using_organization_id,
	owning_organization_id,
	vendor_business_type,
	asl_status_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	manufacturer_id,
	vendor_id,
	item_id,
	category_id,
	vendor_site_id,
	primary_vendor_item,
	manufacturer_asl_id,
	review_by_date,
	comments,
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
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	DISABLE_FLAG,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.PO_APPROVED_SUPPLIER_LIST;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'po_approved_supplier_list';
	
commit;