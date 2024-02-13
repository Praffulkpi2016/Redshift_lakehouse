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

DROP TABLE if exists bec_ods.QP_LIST_LINES;

CREATE TABLE IF NOT EXISTS bec_ods.QP_LIST_LINES
(

    list_line_id NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,list_header_id NUMERIC(15,0)   ENCODE az64
	,list_line_type_code VARCHAR(30)   ENCODE lzo
	,start_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,automatic_flag VARCHAR(1)   ENCODE lzo
	,modifier_level_code VARCHAR(30)   ENCODE lzo
	,price_by_formula_id NUMERIC(15,0)   ENCODE az64
	,list_price NUMERIC(28,10)   ENCODE az64
	,list_price_uom_code VARCHAR(3)   ENCODE lzo
	,primary_uom_flag VARCHAR(1)   ENCODE lzo
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,related_item_id NUMERIC(15,0)   ENCODE az64
	,relationship_type_id NUMERIC(15,0)   ENCODE az64
	,substitution_context VARCHAR(30)   ENCODE lzo
	,substitution_attribute VARCHAR(30)   ENCODE lzo
	,substitution_value VARCHAR(240)   ENCODE lzo
	,revision VARCHAR(50)   ENCODE lzo
	,revision_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,revision_reason_code VARCHAR(30)   ENCODE lzo
	,price_break_type_code VARCHAR(30)   ENCODE lzo
	,percent_price NUMERIC(28,10)   ENCODE az64
	,number_effective_periods NUMERIC(15,0)   ENCODE az64
	,effective_period_uom VARCHAR(3)   ENCODE lzo
	,arithmetic_operator VARCHAR(30)   ENCODE lzo
	,operand NUMERIC(28,10)   ENCODE az64
	,override_flag VARCHAR(1)   ENCODE lzo
	,print_on_invoice_flag VARCHAR(1)   ENCODE lzo
	,rebate_transaction_type_code VARCHAR(30)   ENCODE lzo
	,base_qty NUMERIC(28,10)   ENCODE az64
	,base_uom_code VARCHAR(3)   ENCODE lzo
	,accrual_qty NUMERIC(28,10)   ENCODE az64
	,accrual_uom_code VARCHAR(3)   ENCODE lzo
	,estim_accrual_rate NUMERIC(28,10)   ENCODE az64
	,comments VARCHAR(2000)   ENCODE lzo
	,generate_using_formula_id NUMERIC(15,0)   ENCODE az64
	,reprice_flag VARCHAR(1)   ENCODE lzo
	,list_line_no VARCHAR(30)   ENCODE lzo
	,estim_gl_value NUMERIC(28,10)   ENCODE az64
	,benefit_price_list_line_id NUMERIC(15,0)   ENCODE az64
	,expiration_period_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,number_expiration_periods NUMERIC(28,10)   ENCODE az64
	,expiration_period_uom VARCHAR(3)   ENCODE lzo
	,expiration_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,accrual_flag VARCHAR(1)   ENCODE lzo
	,pricing_phase_id NUMERIC(15,0)   ENCODE az64
	,pricing_group_sequence NUMERIC(28,10)   ENCODE az64
	,incompatibility_grp_code VARCHAR(30)   ENCODE lzo
	,product_precedence NUMERIC(15,0)   ENCODE az64
	,proration_type_code VARCHAR(30)   ENCODE lzo
	,accrual_conversion_rate NUMERIC(28,10)   ENCODE az64
	,benefit_qty NUMERIC(28,10)   ENCODE az64
	,benefit_uom_code VARCHAR(3)   ENCODE lzo
	,recurring_flag VARCHAR(1)   ENCODE lzo
	,benefit_limit NUMERIC(28,10)   ENCODE az64
	,charge_type_code VARCHAR(30)   ENCODE lzo
	,charge_subtype_code VARCHAR(30)   ENCODE lzo
	,context VARCHAR(30)   ENCODE lzo
	,attribute1 VARCHAR(240)   ENCODE lzo
	,attribute2 VARCHAR(240)   ENCODE lzo
	,attribute3 VARCHAR(240)   ENCODE lzo
	,attribute4 VARCHAR(240)   ENCODE lzo
	,attribute5 VARCHAR(240)   ENCODE lzo
	,attribute6 VARCHAR(240)   ENCODE lzo
	,attribute7 VARCHAR(240)   ENCODE lzo
	,attribute8 VARCHAR(240)   ENCODE lzo
	,attribute9 VARCHAR(240)   ENCODE lzo
	,attribute10 VARCHAR(240)   ENCODE lzo
	,attribute11 VARCHAR(240)   ENCODE lzo
	,attribute12 VARCHAR(240)   ENCODE lzo
	,attribute13 VARCHAR(240)   ENCODE lzo
	,attribute14 VARCHAR(240)   ENCODE lzo
	,attribute15 VARCHAR(240)   ENCODE lzo
	,include_on_returns_flag VARCHAR(1)   ENCODE lzo
	,qualification_ind NUMERIC(15,0)   ENCODE az64
	,limit_exists_flag VARCHAR(1)   ENCODE lzo
	,group_count NUMERIC(28,10)   ENCODE az64
	,net_amount_flag VARCHAR(1)   ENCODE lzo
	,recurring_value NUMERIC(28,10)   ENCODE az64
	,accum_context VARCHAR(30)   ENCODE lzo
	,accum_attribute VARCHAR(240)   ENCODE lzo
	,accum_attr_run_src_flag VARCHAR(1)   ENCODE lzo
	,customer_item_id NUMERIC(15,0)   ENCODE az64
	,break_uom_code VARCHAR(3)   ENCODE lzo
	,break_uom_context VARCHAR(30)   ENCODE lzo
	,break_uom_attribute VARCHAR(30)   ENCODE lzo
	,pattern_id NUMERIC(15,0)   ENCODE az64
	,product_uom_code VARCHAR(3)   ENCODE lzo
	,pricing_attribute_count NUMERIC(28,10)   ENCODE az64
	,hash_key VARCHAR(2000)   ENCODE lzo
	,cache_key VARCHAR(240)   ENCODE lzo
	,orig_sys_line_ref VARCHAR(50)   ENCODE lzo
	,orig_sys_header_ref VARCHAR(50)   ENCODE lzo
	,continuous_price_break_flag VARCHAR(1)   ENCODE lzo
	,eq_flag VARCHAR(1)   ENCODE lzo
	,currency_header_id NUMERIC(15,0)   ENCODE az64
	,null_other_oprt_count NUMERIC(28,10)   ENCODE az64
	,pte_code VARCHAR(30)   ENCODE lzo
	,source_system_code VARCHAR(30)   ENCODE lzo
	,service_duration NUMERIC(28,10)   ENCODE az64
	,service_period VARCHAR(3)   ENCODE lzo
	,subscription_service_flag VARCHAR(1)   ENCODE lzo
	,subscription_template_id NUMERIC(15,0) 
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

insert
	into
	bec_ods.QP_LIST_LINES (
    list_line_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	list_header_id,
	list_line_type_code,
	start_date_active,
	end_date_active,
	automatic_flag,
	modifier_level_code,
	price_by_formula_id,
	list_price,
	list_price_uom_code,
	primary_uom_flag,
	inventory_item_id,
	organization_id,
	related_item_id,
	relationship_type_id,
	substitution_context,
	substitution_attribute,
	substitution_value,
	revision,
	revision_date,
	revision_reason_code,
	price_break_type_code,
	percent_price,
	number_effective_periods,
	effective_period_uom,
	arithmetic_operator,
	operand,
	override_flag,
	print_on_invoice_flag,
	rebate_transaction_type_code,
	base_qty,
	base_uom_code,
	accrual_qty,
	accrual_uom_code,
	estim_accrual_rate,
	comments,
	generate_using_formula_id,
	reprice_flag,
	list_line_no,
	estim_gl_value,
	benefit_price_list_line_id,
	expiration_period_start_date,
	number_expiration_periods,
	expiration_period_uom,
	expiration_date,
	accrual_flag,
	pricing_phase_id,
	pricing_group_sequence,
	incompatibility_grp_code,
	product_precedence,
	proration_type_code,
	accrual_conversion_rate,
	benefit_qty,
	benefit_uom_code,
	recurring_flag,
	benefit_limit,
	charge_type_code,
	charge_subtype_code,
	context,
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
	include_on_returns_flag,
	qualification_ind,
	limit_exists_flag,
	group_count,
	net_amount_flag,
	recurring_value,
	accum_context,
	accum_attribute,
	accum_attr_run_src_flag,
	customer_item_id,
	break_uom_code,
	break_uom_context,
	break_uom_attribute,
	pattern_id,
	product_uom_code,
	pricing_attribute_count,
	hash_key,
	cache_key,
	orig_sys_line_ref,
	orig_sys_header_ref,
	continuous_price_break_flag,
	eq_flag,
	currency_header_id,
	null_other_oprt_count,
	pte_code,
	source_system_code,
	service_duration,
	service_period,
	subscription_service_flag,
	subscription_template_id,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    select
	list_line_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	list_header_id,
	list_line_type_code,
	start_date_active,
	end_date_active,
	automatic_flag,
	modifier_level_code,
	price_by_formula_id,
	list_price,
	list_price_uom_code,
	primary_uom_flag,
	inventory_item_id,
	organization_id,
	related_item_id,
	relationship_type_id,
	substitution_context,
	substitution_attribute,
	substitution_value,
	revision,
	revision_date,
	revision_reason_code,
	price_break_type_code,
	percent_price,
	number_effective_periods,
	effective_period_uom,
	arithmetic_operator,
	operand,
	override_flag,
	print_on_invoice_flag,
	rebate_transaction_type_code,
	base_qty,
	base_uom_code,
	accrual_qty,
	accrual_uom_code,
	estim_accrual_rate,
	comments,
	generate_using_formula_id,
	reprice_flag,
	list_line_no,
	estim_gl_value,
	benefit_price_list_line_id,
	expiration_period_start_date,
	number_expiration_periods,
	expiration_period_uom,
	expiration_date,
	accrual_flag,
	pricing_phase_id,
	pricing_group_sequence,
	incompatibility_grp_code,
	product_precedence,
	proration_type_code,
	accrual_conversion_rate,
	benefit_qty,
	benefit_uom_code,
	recurring_flag,
	benefit_limit,
	charge_type_code,
	charge_subtype_code,
	context,
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
	include_on_returns_flag,
	qualification_ind,
	limit_exists_flag,
	group_count,
	net_amount_flag,
	recurring_value,
	accum_context,
	accum_attribute,
	accum_attr_run_src_flag,
	customer_item_id,
	break_uom_code,
	break_uom_context,
	break_uom_attribute,
	pattern_id,
	product_uom_code,
	pricing_attribute_count,
	hash_key,
	cache_key,
	orig_sys_line_ref,
	orig_sys_header_ref,
	continuous_price_break_flag,
	eq_flag,
	currency_header_id,
	null_other_oprt_count,
	pte_code,
	source_system_code,
	service_duration,
	service_period,
	subscription_service_flag,
	subscription_template_id,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
	,kca_seq_date
from
	bec_ods_stg.QP_LIST_LINES;
end;

update
	bec_etl_ctrl.batch_ods_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'qp_list_lines';

commit;