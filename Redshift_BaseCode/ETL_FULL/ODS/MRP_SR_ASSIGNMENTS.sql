/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach FOR ODS.
# File Version: KPI v1.0
*/

begin;

DROP TABLE if exists bec_ods.MRP_SR_ASSIGNMENTS;

CREATE TABLE IF NOT EXISTS bec_ods.MRP_SR_ASSIGNMENTS
(

     assignment_id NUMERIC(15,0)   ENCODE az64
	,assignment_type NUMERIC(15,0)   ENCODE az64
	,sourcing_rule_id NUMERIC(15,0)   ENCODE az64
	,sourcing_rule_type NUMERIC(15,0)   ENCODE az64
	,assignment_set_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,customer_id NUMERIC(15,0)   ENCODE az64
	,ship_to_site_id NUMERIC(15,0)   ENCODE az64
	,category_id NUMERIC(15,0)   ENCODE az64
	,category_set_id NUMERIC(15,0)   ENCODE az64
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,secondary_inventory VARCHAR(150)   ENCODE lzo
	,attribute_category VARCHAR(150)   ENCODE lzo
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
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MRP_SR_ASSIGNMENTS (
     assignment_id
    ,assignment_type
    ,sourcing_rule_id
    ,sourcing_rule_type
    ,assignment_set_id
    ,last_update_date
    ,last_updated_by
    ,creation_date
    ,created_by
    ,last_update_login
    ,request_id
    ,program_application_id
    ,program_id
    ,program_update_date
    ,organization_id
    ,customer_id
    ,ship_to_site_id
    ,category_id
    ,category_set_id
    ,inventory_item_id
    ,secondary_inventory
    ,attribute_category
    ,attribute1
    ,attribute2
    ,attribute3
    ,attribute4
    ,attribute5
    ,attribute6
    ,attribute7
    ,attribute8
    ,attribute9
    ,attribute10
    ,attribute11
    ,attribute12
    ,attribute13
    ,attribute14
    ,attribute15,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
    assignment_id
    ,assignment_type
    ,sourcing_rule_id
    ,sourcing_rule_type
    ,assignment_set_id
    ,last_update_date
    ,last_updated_by
    ,creation_date
    ,created_by
    ,last_update_login
    ,request_id
    ,program_application_id
    ,program_id
    ,program_update_date
    ,organization_id
    ,customer_id
    ,ship_to_site_id
    ,category_id
    ,category_set_id
    ,inventory_item_id
    ,secondary_inventory
    ,attribute_category
    ,attribute1
    ,attribute2
    ,attribute3
    ,attribute4
    ,attribute5
    ,attribute6
    ,attribute7
    ,attribute8
    ,attribute9
    ,attribute10
    ,attribute11
    ,attribute12
    ,attribute13
    ,attribute14
    ,attribute15,
    KCA_OPERATION,
    'N' as IS_DELETED_FLG,
    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.MRP_SR_ASSIGNMENTS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'mrp_sr_assignments';
	
COMMIT;