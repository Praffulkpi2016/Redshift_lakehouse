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

DROP TABLE if exists bec_ods.jtf_rs_salesreps;

CREATE TABLE IF NOT EXISTS bec_ods.jtf_rs_salesreps
(
	salesrep_id NUMERIC(15,0)   ENCODE az64
	,resource_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,sales_credit_type_id NUMERIC(15,0)   ENCODE az64
	,name VARCHAR(240)   ENCODE lzo
	,status VARCHAR(30)   ENCODE lzo
	,start_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,gl_id_rev NUMERIC(15,0)   ENCODE az64
	,gl_id_freight NUMERIC(15,0)   ENCODE az64
	,gl_id_rec NUMERIC(15,0)   ENCODE az64
	,set_of_books_id NUMERIC(15,0)   ENCODE az64
	,salesrep_number VARCHAR(30)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,email_address VARCHAR(240)   ENCODE lzo
	,wh_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,person_id NUMERIC(15,0)   ENCODE az64
	,sales_tax_geocode VARCHAR(30)   ENCODE lzo
	,sales_tax_inside_city_limits VARCHAR(1)   ENCODE lzo
	,object_version_number NUMERIC(15,0)   ENCODE az64
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
	,security_group_id NUMERIC(15,0)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
	,is_deleted_flg VARCHAR(2)   ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;
insert
	into
	bec_ods.jtf_rs_salesreps
(	salesrep_id,
	resource_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	sales_credit_type_id,
	"name",
	status,
	start_date_active,
	end_date_active,
	gl_id_rev,
	gl_id_freight,
	gl_id_rec,
	set_of_books_id,
	salesrep_number,
	org_id,
	email_address,
	wh_update_date,
	person_id,
	sales_tax_geocode,
	sales_tax_inside_city_limits,
	object_version_number,
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
	security_group_id,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date)
	SELECT
	salesrep_id,
	resource_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	sales_credit_type_id,
	"name",
	status,
	start_date_active,
	end_date_active,
	gl_id_rev,
	gl_id_freight,
	gl_id_rec,
	set_of_books_id,
	salesrep_number,
	org_id,
	email_address,
	wh_update_date,
	person_id,
	sales_tax_geocode,
	sales_tax_inside_city_limits,
	object_version_number,
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
	security_group_id,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.jtf_rs_salesreps;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'jtf_rs_salesreps';
	
commit;
	
