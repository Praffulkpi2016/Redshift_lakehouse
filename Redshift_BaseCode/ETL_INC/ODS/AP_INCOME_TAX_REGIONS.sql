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

delete from bec_ods.ap_income_tax_regions
where region_short_name in (
select stg.region_short_name 
from bec_ods.ap_income_tax_regions ods, bec_ods_stg.ap_income_tax_regions stg
where ods.region_short_name = stg.region_short_name
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.ap_income_tax_regions
   (region_short_name,
	region_long_name,
	region_code,
	reporting_limit,
	num_of_payees,
	control_total1,
	control_total2,
	control_total3,
	control_total4,
	control_total5,
	control_total6,
	control_total7,
	control_total8,
	control_total9,
	control_total10,
	active_date,
	inactive_date,
	last_update_date,
	last_updated_by,
	last_update_login,
	creation_date,
	created_by,
	reporting_limit_method_code,
	control_total13,
	control_total13e,
	control_total14,
	control_total15a,
	control_total15b,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(select
	region_short_name,
	region_long_name,
	region_code,
	reporting_limit,
	num_of_payees,
	control_total1,
	control_total2,
	control_total3,
	control_total4,
	control_total5,
	control_total6,
	control_total7,
	control_total8,
	control_total9,
	control_total10,
	active_date,
	inactive_date,
	last_update_date,
	last_updated_by,
	last_update_login,
	creation_date,
	created_by,
	reporting_limit_method_code,
	control_total13,
	control_total13e,
	control_total14,
	control_total15a,
	control_total15b,
	KCA_OPERATION,
	'N' AS IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
from
	bec_ods_stg.ap_income_tax_regions
where kca_operation IN ('INSERT','UPDATE') 
	and (region_short_name,KCA_SEQ_ID) in 
	(select region_short_name,max(KCA_SEQ_ID) from bec_ods_stg.ap_income_tax_regions 
     where kca_operation IN ('INSERT','UPDATE')
     group by region_short_name)
);

commit;

-- Soft delete
update bec_ods.ap_income_tax_regions set IS_DELETED_FLG = 'N';
commit;
update bec_ods.ap_income_tax_regions set IS_DELETED_FLG = 'Y'
where (region_short_name)  in
(
select region_short_name from bec_raw_dl_ext.ap_income_tax_regions
where (region_short_name,KCA_SEQ_ID)
in 
(
select region_short_name,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.ap_income_tax_regions
group by region_short_name
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
	ods_table_name = 'ap_income_tax_regions';

commit;