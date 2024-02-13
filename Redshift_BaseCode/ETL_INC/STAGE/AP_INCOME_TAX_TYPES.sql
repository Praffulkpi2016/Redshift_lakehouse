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

TRUNCATE TABLE bec_ods_stg.ap_income_tax_types;

insert into	bec_ods_stg.ap_income_tax_types
(income_tax_type,
	description,
	inactive_date,
	last_update_date,
	last_updated_by,
	last_update_login,
	creation_date,
	created_by,
	zd_edition_name,
	zd_sync,
	KCA_OPERATION,
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
	zd_edition_name,
	zd_sync,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
from
	bec_raw_dl_ext.ap_income_tax_types 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (income_tax_type,KCA_SEQ_ID) in 
	(select income_tax_type,max(KCA_SEQ_ID) from bec_raw_dl_ext.ap_income_tax_types 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by income_tax_type)
     and kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info 
	where ods_table_name ='ap_income_tax_types')
	);
END;