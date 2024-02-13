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

delete from bec_ods.ap_income_tax_types
where income_tax_type in (
select stg.income_tax_type 
from bec_ods.ap_income_tax_types ods, bec_ods_stg.ap_income_tax_types stg
where ods.income_tax_type = stg.income_tax_type
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.ap_income_tax_types
(income_tax_type,
	description,
	inactive_date,
	last_update_date,
	last_updated_by,
	last_update_login,
	creation_date,
	created_by,
	ZD_EDITION_NAME,
	ZD_SYNC,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(select
	income_tax_type,
	description,
	inactive_date,
	last_update_date,
	last_updated_by,
	last_update_login,
	creation_date,
	created_by,
	ZD_EDITION_NAME,
	ZD_SYNC,
	KCA_OPERATION,
	'N' AS IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
from
	bec_ods_stg.ap_income_tax_types
    where kca_operation IN ('INSERT','UPDATE') 
	and (income_tax_type,kca_seq_id) in 
	(select income_tax_type,max(kca_seq_id) from bec_ods_stg.ap_income_tax_types 
     where kca_operation IN ('INSERT','UPDATE')
     group by income_tax_type)
);

commit;

-- Soft delete
update bec_ods.ap_income_tax_types set IS_DELETED_FLG = 'N';
commit;
update bec_ods.ap_income_tax_types set IS_DELETED_FLG = 'Y'
where (income_tax_type)  in
(
select income_tax_type from bec_raw_dl_ext.ap_income_tax_types
where (income_tax_type,KCA_SEQ_ID)
in 
(
select income_tax_type,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.ap_income_tax_types
group by income_tax_type
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
	ods_table_name = 'ap_income_tax_types';

commit;