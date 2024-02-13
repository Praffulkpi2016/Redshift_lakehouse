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

DROP TABLE if exists bec_ods.MSC_TRADING_PARTNERS ;

CREATE TABLE IF NOT EXISTS bec_ods.MSC_TRADING_PARTNERS 
(

     partner_id NUMERIC(15,0)   ENCODE az64
	,sr_tp_id NUMERIC(15,0)   ENCODE az64
	,sr_instance_id NUMERIC(15,0)   ENCODE az64
	,organization_code VARCHAR(7)  ENCODE lzo
	,disable_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,status VARCHAR(1)   ENCODE lzo
	,master_organization NUMERIC(15,0)   ENCODE az64
	,weight_uom VARCHAR(3)   ENCODE lzo
	,maximum_weight NUMERIC(28,10)   ENCODE az64
	,volume_uom VARCHAR(3)   ENCODE lzo
	,maximum_volume NUMERIC(28,10)   ENCODE az64
	,partner_type NUMERIC(15,0)   ENCODE az64
	,partner_name VARCHAR(255)   ENCODE lzo
	,partner_number VARCHAR(154)   ENCODE lzo
	,calendar_code VARCHAR(14)   ENCODE lzo
	,calendar_exception_set_id NUMERIC(15,0)   ENCODE az64
	,operating_unit NUMERIC(15,0)   ENCODE az64
	,project_reference_enabled NUMERIC(15,0)   ENCODE az64
	,project_control_level NUMERIC(15,0)   ENCODE az64
	,source_org_id NUMERIC(15,0)   ENCODE az64
	,demand_lateness_cost NUMERIC(28,10)   ENCODE az64
	,supplier_cap_overutil_cost NUMERIC(28,10)   ENCODE az64
	,resource_cap_overutil_cost NUMERIC(28,10)   ENCODE az64
	,default_demand_class VARCHAR(34)   ENCODE lzo
	,modeled_customer_id NUMERIC(15,0)   ENCODE az64
	,modeled_customer_site_id NUMERIC(15,0)   ENCODE az64
	,modeled_supplier_id NUMERIC(15,0)   ENCODE az64
	,modeled_supplier_site_id NUMERIC(15,0)   ENCODE az64
	,transport_cap_over_util_cost NUMERIC(28,10)   ENCODE az64
	,refresh_number NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
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
	,use_phantom_routings NUMERIC(15,0)   ENCODE az64
	,inherit_phantom_op_seq NUMERIC(15,0)   ENCODE az64
	,default_atp_rule_id NUMERIC(15,0)   ENCODE az64
	,material_account NUMERIC(15,0)   ENCODE az64
	,expense_account NUMERIC(15,0)   ENCODE az64
	,organization_type NUMERIC(15,0)   ENCODE az64
	,service_level NUMERIC(15,0)   ENCODE az64
	,customer_class_code VARCHAR(30)   ENCODE lzo
	,company_id NUMERIC(15,0)   ENCODE az64
	,org_supplier_mapped VARCHAR(1)   ENCODE lzo
	,accept_demands_from_unmet_po NUMERIC(15,0)   ENCODE az64
	,inherit_oc_op_seq_num NUMERIC(15,0)   ENCODE az64
	,sr_business_group_id NUMERIC(15,0)   ENCODE az64
	,sr_legal_entity NUMERIC(15,0)   ENCODE az64
	,sr_set_of_books_id NUMERIC(15,0)   ENCODE az64
	,sr_chart_of_accounts_id NUMERIC(15,0)   ENCODE az64
	,customer_type VARCHAR(30)   ENCODE lzo
	,business_group_name VARCHAR(240)   ENCODE lzo
	,legal_entity_name VARCHAR(240)   ENCODE lzo
	,operating_unit_name VARCHAR(240)   ENCODE lzo
	,currency_code VARCHAR(14)   ENCODE lzo
	,SUBCONTRACTING_SOURCE_ORG NUMERIC(15,0) ENCODE az64
    ,TRADING_PARTNER_ORG_FLAG VARCHAR(1) ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MSC_TRADING_PARTNERS  (
    partner_id,
	sr_tp_id,
	sr_instance_id,
	organization_code,
	disable_date,
	status,
	master_organization,
	weight_uom,
	maximum_weight,
	volume_uom,
	maximum_volume,
	partner_type,
	partner_name,
	partner_number,
	calendar_code,
	calendar_exception_set_id,
	operating_unit,
	project_reference_enabled,
	project_control_level,
	source_org_id,
	demand_lateness_cost,
	supplier_cap_overutil_cost,
	resource_cap_overutil_cost,
	default_demand_class,
	modeled_customer_id,
	modeled_customer_site_id,
	modeled_supplier_id,
	modeled_supplier_site_id,
	transport_cap_over_util_cost,
	refresh_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
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
	use_phantom_routings,
	inherit_phantom_op_seq,
	default_atp_rule_id,
	material_account,
	expense_account,
	organization_type,
	service_level,
	customer_class_code,
	company_id,
	org_supplier_mapped,
	accept_demands_from_unmet_po,
	inherit_oc_op_seq_num,
	sr_business_group_id,
	sr_legal_entity,
	sr_set_of_books_id,
	sr_chart_of_accounts_id,
	customer_type,
	business_group_name,
	legal_entity_name,
	operating_unit_name,
	currency_code,
	SUBCONTRACTING_SOURCE_ORG,
    TRADING_PARTNER_ORG_FLAG,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
    SELECT
    partner_id,
	sr_tp_id,
	sr_instance_id,
	organization_code,
	disable_date,
	status,
	master_organization,
	weight_uom,
	maximum_weight,
	volume_uom,
	maximum_volume,
	partner_type,
	partner_name,
	partner_number,
	calendar_code,
	calendar_exception_set_id,
	operating_unit,
	project_reference_enabled,
	project_control_level,
	source_org_id,
	demand_lateness_cost,
	supplier_cap_overutil_cost,
	resource_cap_overutil_cost,
	default_demand_class,
	modeled_customer_id,
	modeled_customer_site_id,
	modeled_supplier_id,
	modeled_supplier_site_id,
	transport_cap_over_util_cost,
	refresh_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
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
	use_phantom_routings,
	inherit_phantom_op_seq,
	default_atp_rule_id,
	material_account,
	expense_account,
	organization_type,
	service_level,
	customer_class_code,
	company_id,
	org_supplier_mapped,
	accept_demands_from_unmet_po,
	inherit_oc_op_seq_num,
	sr_business_group_id,
	sr_legal_entity,
	sr_set_of_books_id,
	sr_chart_of_accounts_id,
	customer_type,
	business_group_name,
	legal_entity_name,
	operating_unit_name,
	currency_code,
	SUBCONTRACTING_SOURCE_ORG,
    TRADING_PARTNER_ORG_FLAG,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.MSC_TRADING_PARTNERS ;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'msc_trading_partners';
	
commit;