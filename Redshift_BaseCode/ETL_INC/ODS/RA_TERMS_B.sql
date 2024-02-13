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

delete from bec_ods.RA_TERMS_B
where (TERM_ID) in (
select stg.TERM_ID 
from bec_ods.RA_TERMS_B ods, bec_ods_stg.RA_TERMS_B stg
where 
ods.TERM_ID = stg.TERM_ID   
 


and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.RA_TERMS_B
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
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date)	
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
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.RA_TERMS_B
	where kca_operation in ('INSERT','UPDATE') 
	and (TERM_ID,kca_seq_id) in 
	(select TERM_ID,max(kca_seq_id) from bec_ods_stg.RA_TERMS_B 
     where kca_operation in ('INSERT','UPDATE')
     group by TERM_ID)
);

commit;

-- Soft delete
update bec_ods.RA_TERMS_B set IS_DELETED_FLG = 'N';
commit;
update bec_ods.RA_TERMS_B set IS_DELETED_FLG = 'Y'
where (TERM_ID)  in
(
select TERM_ID from bec_raw_dl_ext.RA_TERMS_B
where (TERM_ID,KCA_SEQ_ID)
in 
(
select TERM_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.RA_TERMS_B
group by TERM_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'ra_terms_b';

commit;