/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
 
begin;
-- Delete Records

delete
from
	bec_ods.QP_LIST_LINES
where
	(
	nvl(LIST_LINE_ID, 0) 
	) in 
	(
	select
		NVL(stg.LIST_LINE_ID, 0) as LIST_LINE_ID 
	from
		bec_ods.QP_LIST_LINES ods,
		bec_ods_stg.QP_LIST_LINES stg
	where
		    NVL(ods.LIST_LINE_ID, 0) = NVL(stg.LIST_LINE_ID, 0) 
					and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

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
	,kca_seq_date)
(
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
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
		,kca_seq_date
	from
		bec_ods_stg.QP_LIST_LINES
	where
		kca_operation in ('INSERT','UPDATE')
		and (
		nvl(LIST_LINE_ID, 0) ,
		KCA_SEQ_ID
		) in 
	(
		select
			nvl(LIST_LINE_ID, 0) as LIST_LINE_ID ,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.QP_LIST_LINES
		where
			kca_operation in ('INSERT','UPDATE')
		group by
			LIST_LINE_ID 
			)	
	);

commit;

-- Soft delete
update bec_ods.QP_LIST_LINES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.QP_LIST_LINES set IS_DELETED_FLG = 'Y'
where (LIST_LINE_ID)  in
(
select LIST_LINE_ID from bec_raw_dl_ext.QP_LIST_LINES
where (LIST_LINE_ID,KCA_SEQ_ID)
in 
(
select LIST_LINE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.QP_LIST_LINES
group by LIST_LINE_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'qp_list_lines';

commit;