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

delete from bec_ods.AR_DISTRIBUTIONS_ALL
where (source_id,source_table,source_type,line_id) in (
select stg.source_id,stg.source_table,stg.source_type,stg.line_id
from bec_ods.AR_DISTRIBUTIONS_ALL ods,
bec_ods_stg.AR_DISTRIBUTIONS_ALL stg
where ods.source_id = stg.source_id
and ods.source_table = stg.source_table
and ods.source_type = stg.source_type
and ods.line_id = stg.line_id 
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.AR_DISTRIBUTIONS_ALL
       (
    line_id,
	source_id,
	source_table,
	source_type,
	code_combination_id,
	amount_dr,
	amount_cr,
	acctd_amount_dr,
	acctd_amount_cr,
	creation_date,
	created_by,
	last_updated_by,
	last_update_date,
	last_update_login,
	org_id,
	source_table_secondary,
	source_id_secondary,
	currency_code,
	currency_conversion_rate,
	currency_conversion_type,
	currency_conversion_date,
	taxable_entered_dr,
	taxable_entered_cr,
	taxable_accounted_dr,
	taxable_accounted_cr,
	tax_link_id,
	third_party_id,
	third_party_sub_id,
	reversed_source_id,
	tax_code_id,
	location_segment_id,
	source_type_secondary,
	tax_group_code_id,
	ref_customer_trx_line_id,
	ref_cust_trx_line_gl_dist_id,
	ref_account_class,
	activity_bucket,
	ref_line_id,
	from_amount_dr,
	from_amount_cr,
	from_acctd_amount_dr,
	from_acctd_amount_cr,
	ref_mf_dist_flag,
	ref_dist_ccid, 
	ref_prev_cust_trx_line_id,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
(
	SELECT
	line_id,
	source_id,
	source_table,
	source_type,
	code_combination_id,
	amount_dr,
	amount_cr,
	acctd_amount_dr,
	acctd_amount_cr,
	creation_date,
	created_by,
	last_updated_by,
	last_update_date,
	last_update_login,
	org_id,
	source_table_secondary,
	source_id_secondary,
	currency_code,
	currency_conversion_rate,
	currency_conversion_type,
	currency_conversion_date,
	taxable_entered_dr,
	taxable_entered_cr,
	taxable_accounted_dr,
	taxable_accounted_cr,
	tax_link_id,
	third_party_id,
	third_party_sub_id,
	reversed_source_id,
	tax_code_id,
	location_segment_id,
	source_type_secondary,
	tax_group_code_id,
	ref_customer_trx_line_id,
	ref_cust_trx_line_gl_dist_id,
	ref_account_class,
	activity_bucket,
	ref_line_id,
	from_amount_dr,
	from_amount_cr,
	from_acctd_amount_dr,
	from_acctd_amount_cr,
	ref_mf_dist_flag,
	ref_dist_ccid, 
	ref_prev_cust_trx_line_id,
	kca_operation,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.AR_DISTRIBUTIONS_ALL
	where kca_operation IN ('INSERT','UPDATE') 
	and (source_id,source_table,source_type,line_id,kca_seq_id) in 
	(select source_id,source_table,source_type,line_id,max(kca_seq_id) from bec_ods_stg.AR_DISTRIBUTIONS_ALL 
     where kca_operation IN ('INSERT','UPDATE')
     group by source_id,source_table,source_type,line_id)
);

commit;

-- Soft delete
update bec_ods.AR_DISTRIBUTIONS_ALL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.AR_DISTRIBUTIONS_ALL set IS_DELETED_FLG = 'Y'
where (source_id,source_table,source_type,line_id)  in
(
select source_id,source_table,source_type,line_id from bec_raw_dl_ext.AR_DISTRIBUTIONS_ALL
where (source_id,source_table,source_type,line_id,KCA_SEQ_ID)
in 
(
select source_id,source_table,source_type,line_id,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.AR_DISTRIBUTIONS_ALL
group by source_id,source_table,source_type,line_id
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'ar_distributions_all';

commit;