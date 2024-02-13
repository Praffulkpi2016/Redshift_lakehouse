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

DROP TABLE if exists bec_ods.PO_ASL_ATTRIBUTES;

CREATE TABLE IF NOT EXISTS bec_ods.PO_ASL_ATTRIBUTES
(
	asl_id NUMERIC(15,0)   ENCODE az64
	,using_organization_id NUMERIC(15,0)    ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)    ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)    ENCODE az64
	,document_sourcing_method VARCHAR(25)   ENCODE lzo
	,release_generation_method VARCHAR(25)   ENCODE lzo
	,purchasing_unit_of_measure VARCHAR(25)   ENCODE lzo
	,enable_plan_schedule_flag VARCHAR(1)   ENCODE lzo
	,enable_ship_schedule_flag VARCHAR(1)   ENCODE lzo
	,plan_schedule_type VARCHAR(25)   ENCODE lzo
	,ship_schedule_type VARCHAR(25)   ENCODE lzo
	,plan_bucket_pattern_id NUMERIC(15,0)    ENCODE az64
	,ship_bucket_pattern_id NUMERIC(15,0)    ENCODE az64
	,enable_autoschedule_flag VARCHAR(1)   ENCODE lzo
	,scheduler_id NUMERIC(15,0)    ENCODE az64
	,enable_authorizations_flag VARCHAR(1)   ENCODE lzo
	,vendor_id NUMERIC(15,0)   ENCODE az64
	,vendor_site_id NUMERIC(15,0)    ENCODE az64
	,item_id NUMERIC(15,0)   ENCODE az64
	,category_id NUMERIC(15,0)   ENCODE az64
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
	,price_update_tolerance NUMERIC(28,10)   ENCODE az64
	,processing_lead_time NUMERIC(15,0)   ENCODE az64
	,min_order_qty NUMERIC(28,10)   ENCODE az64
	,fixed_lot_multiple NUMERIC(28,10)   ENCODE az64
	,delivery_calendar VARCHAR(10)   ENCODE lzo
	,country_of_origin_code VARCHAR(2)   ENCODE lzo
	,enable_vmi_flag VARCHAR(1)   ENCODE lzo
	,vmi_min_qty NUMERIC(28,10)   ENCODE az64
	,vmi_max_qty NUMERIC(28,10)   ENCODE az64
	,enable_vmi_auto_replenish_flag VARCHAR(1)   ENCODE lzo
	,vmi_replenishment_approval VARCHAR(30)   ENCODE lzo
	,consigned_from_supplier_flag VARCHAR(1)   ENCODE lzo
	,last_billing_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,consigned_billing_cycle NUMERIC(15,0)   ENCODE az64
	,consume_on_aging_flag VARCHAR(1)   ENCODE lzo
	,aging_period NUMERIC(15,0)   ENCODE az64
	,replenishment_method NUMERIC(28,10)   ENCODE az64
	,vmi_min_days NUMERIC(15,0)   ENCODE az64
	,vmi_max_days NUMERIC(15,0)   ENCODE az64
	,fixed_order_quantity NUMERIC(28,10)   ENCODE az64
	,forecast_horizon NUMERIC(28,10)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;
 
INSERT INTO bec_ods.PO_ASL_ATTRIBUTES (
	asl_id,
	using_organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	document_sourcing_method,
	release_generation_method,
	purchasing_unit_of_measure,
	enable_plan_schedule_flag,
	enable_ship_schedule_flag,
	plan_schedule_type,
	ship_schedule_type,
	plan_bucket_pattern_id,
	ship_bucket_pattern_id,
	enable_autoschedule_flag,
	scheduler_id,
	enable_authorizations_flag,
	vendor_id,
	vendor_site_id,
	item_id,
	category_id,
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
	price_update_tolerance,
	processing_lead_time,
	min_order_qty,
	fixed_lot_multiple,
	delivery_calendar,
	country_of_origin_code,
	enable_vmi_flag,
	vmi_min_qty,
	vmi_max_qty,
	enable_vmi_auto_replenish_flag,
	vmi_replenishment_approval,
	consigned_from_supplier_flag,
	last_billing_date,
	consigned_billing_cycle,
	consume_on_aging_flag,
	aging_period,
	replenishment_method,
	vmi_min_days,
	vmi_max_days,
	fixed_order_quantity,
	forecast_horizon,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
	asl_id,
	using_organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	document_sourcing_method,
	release_generation_method,
	purchasing_unit_of_measure,
	enable_plan_schedule_flag,
	enable_ship_schedule_flag,
	plan_schedule_type,
	ship_schedule_type,
	plan_bucket_pattern_id,
	ship_bucket_pattern_id,
	enable_autoschedule_flag,
	scheduler_id,
	enable_authorizations_flag,
	vendor_id,
	vendor_site_id,
	item_id,
	category_id,
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
	price_update_tolerance,
	processing_lead_time,
	min_order_qty,
	fixed_lot_multiple,
	delivery_calendar,
	country_of_origin_code,
	enable_vmi_flag,
	vmi_min_qty,
	vmi_max_qty,
	enable_vmi_auto_replenish_flag,
	vmi_replenishment_approval,
	consigned_from_supplier_flag,
	last_billing_date,
	consigned_billing_cycle,
	consume_on_aging_flag,
	aging_period,
	replenishment_method,
	vmi_min_days,
	vmi_max_days,
	fixed_order_quantity,
	forecast_horizon,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.PO_ASL_ATTRIBUTES;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'po_asl_attributes';
	
Commit;