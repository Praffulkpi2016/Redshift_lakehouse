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

DROP TABLE if exists bec_ods.FA_ASSET_INVOICES;

CREATE TABLE IF NOT EXISTS bec_ods.FA_ASSET_INVOICES
(

     asset_id NUMERIC(15,0)   ENCODE az64
	,po_vendor_id NUMERIC(15,0)   ENCODE az64
	,asset_invoice_id NUMERIC(15,0)   ENCODE az64
	,fixed_assets_cost NUMERIC(28,10)   ENCODE az64
	,date_effective TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,date_ineffective TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,invoice_transaction_id_in NUMERIC(15,0)   ENCODE az64
	,invoice_transaction_id_out NUMERIC(15,0)   ENCODE az64
	,deleted_flag VARCHAR(3)   ENCODE lzo
	,po_number VARCHAR(20)   ENCODE lzo
	,invoice_number VARCHAR(50)   ENCODE lzo
	,payables_batch_name VARCHAR(50)   ENCODE lzo
	,payables_code_combination_id NUMERIC(15,0)   ENCODE az64
	,feeder_system_name VARCHAR(40)   ENCODE lzo
	,create_batch_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,create_batch_id NUMERIC(15,0)   ENCODE az64
	,invoice_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,payables_cost NUMERIC(28,10)   ENCODE az64
	,post_batch_id NUMERIC(15,0)   ENCODE az64
	,invoice_id NUMERIC(15,0)   ENCODE az64
	,ap_distribution_line_number NUMERIC(15,0)   ENCODE az64
	,payables_units NUMERIC(15,0)   ENCODE az64
	,split_merged_code VARCHAR(3)   ENCODE lzo
	,description VARCHAR(80)   ENCODE lzo
	,parent_mass_addition_id VARCHAR(15)   ENCODE lzo
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
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
	,attribute_category_code VARCHAR(30)   ENCODE lzo
	,unrevalued_cost NUMERIC(28,10)   ENCODE az64
	,merged_code VARCHAR(3)   ENCODE lzo
	,split_code VARCHAR(3)   ENCODE lzo
	,merge_parent_mass_additions_id NUMERIC(15,0)   ENCODE az64
	,split_parent_mass_additions_id NUMERIC(15,0)   ENCODE az64
	,project_asset_line_id NUMERIC(15,0)   ENCODE az64
	,project_id NUMERIC(15,0)   ENCODE az64
	,task_id NUMERIC(15,0)   ENCODE az64
	,source_line_id NUMERIC(15,0)   ENCODE az64
	,depreciate_in_group_flag VARCHAR(1)   ENCODE lzo
	,material_indicator_flag VARCHAR(1)   ENCODE lzo
	,prior_source_line_id NUMERIC(15,0)   ENCODE az64
	,invoice_distribution_id NUMERIC(15,0)   ENCODE az64
	,invoice_line_number NUMERIC(15,0)   ENCODE az64
	,po_distribution_id NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.FA_ASSET_INVOICES (
    asset_id,
	po_vendor_id,
	asset_invoice_id,
	fixed_assets_cost,
	date_effective,
	date_ineffective,
	invoice_transaction_id_in,
	invoice_transaction_id_out,
	deleted_flag,
	po_number,
	invoice_number,
	payables_batch_name,
	payables_code_combination_id,
	feeder_system_name,
	create_batch_date,
	create_batch_id,
	invoice_date,
	payables_cost,
	post_batch_id,
	invoice_id,
	ap_distribution_line_number,
	payables_units,
	split_merged_code,
	description,
	parent_mass_addition_id,
	last_update_date,
	last_updated_by,
	created_by,
	creation_date,
	last_update_login,
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
	attribute_category_code,
	unrevalued_cost,
	merged_code,
	split_code,
	merge_parent_mass_additions_id,
	split_parent_mass_additions_id,
	project_asset_line_id,
	project_id,
	task_id,
	source_line_id,
	depreciate_in_group_flag,
	material_indicator_flag,
	prior_source_line_id,
	invoice_distribution_id,
	invoice_line_number,
	po_distribution_id,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
    asset_id,
	po_vendor_id,
	asset_invoice_id,
	fixed_assets_cost,
	date_effective,
	date_ineffective,
	invoice_transaction_id_in,
	invoice_transaction_id_out,
	deleted_flag,
	po_number,
	invoice_number,
	payables_batch_name,
	payables_code_combination_id,
	feeder_system_name,
	create_batch_date,
	create_batch_id,
	invoice_date,
	payables_cost,
	post_batch_id,
	invoice_id,
	ap_distribution_line_number,
	payables_units,
	split_merged_code,
	description,
	parent_mass_addition_id,
	last_update_date,
	last_updated_by,
	created_by,
	creation_date,
	last_update_login,
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
	attribute_category_code,
	unrevalued_cost,
	merged_code,
	split_code,
	merge_parent_mass_additions_id,
	split_parent_mass_additions_id,
	project_asset_line_id,
	project_id,
	task_id,
	source_line_id,
	depreciate_in_group_flag,
	material_indicator_flag,
	prior_source_line_id,
	invoice_distribution_id,
	invoice_line_number,
	po_distribution_id,
    KCA_OPERATION,
    'N' as IS_DELETED_FLG,
    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.FA_ASSET_INVOICES;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'fa_asset_invoices';
	
COMMIT;