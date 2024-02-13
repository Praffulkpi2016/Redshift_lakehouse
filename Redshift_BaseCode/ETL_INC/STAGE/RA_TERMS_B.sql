/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
begin;

truncate table bec_ods_stg.RA_TERMS_B;

insert into	bec_ods_stg.RA_TERMS_B
   (
	term_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	--"name",
	credit_check_flag,
	due_cutoff_day,
	printing_lead_days,
	--description,
	start_date_active,
	end_date_active,
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
	base_amount,
	calc_discount_on_lines_flag,
	first_installment_code,
	in_use,
	partial_discount_flag,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	prepayment_flag,
	billing_cycle_id,
	zd_edition_name,
	zd_sync,
	kca_operation,
		kca_seq_id
		,KCA_SEQ_DATE)
(
	select	
	term_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	--"name",
	credit_check_flag,
	due_cutoff_day,
	printing_lead_days,
	--description,
	start_date_active,
	end_date_active,
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
	base_amount,
	calc_discount_on_lines_flag,
	first_installment_code,
	in_use,
	partial_discount_flag,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	prepayment_flag,
	billing_cycle_id,
	zd_edition_name,
	zd_sync,
	kca_operation,
		kca_seq_id
,KCA_SEQ_DATE		from bec_raw_dl_ext.RA_TERMS_B
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (TERM_ID,kca_seq_id) in 
	(select TERM_ID,max(kca_seq_id) from bec_raw_dl_ext.RA_TERMS_B 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by TERM_ID)
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'ra_terms_b')

            )
);
end;