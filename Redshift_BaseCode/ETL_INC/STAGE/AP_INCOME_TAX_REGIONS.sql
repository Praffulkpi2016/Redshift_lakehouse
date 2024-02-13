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
BEGIN;

TRUNCATE TABLE bec_ods_stg.ap_income_tax_regions;

insert into	bec_ods_stg.ap_income_tax_regions
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
	kca_seq_id,
	kca_seq_date
from
	bec_raw_dl_ext.ap_income_tax_regions 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (REGION_SHORT_NAME,KCA_SEQ_ID) in 
	(select REGION_SHORT_NAME,max(KCA_SEQ_ID) from bec_raw_dl_ext.ap_income_tax_regions 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by REGION_SHORT_NAME)
     and kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info 
	where ods_table_name ='ap_income_tax_regions')
	);
END;