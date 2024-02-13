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

TRUNCATE TABLE bec_ods_stg.ap_distribution_sets_all;

insert into	bec_ods_stg.ap_distribution_sets_all
    (distribution_set_id,
	distribution_set_name,
	last_update_date,
	last_updated_by,
	description,
	total_percent_distribution,
	inactive_date,
	last_update_login,
	creation_date,
	created_by,
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
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	org_id,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(select
	distribution_set_id,
	distribution_set_name,
	last_update_date,
	last_updated_by,
	description,
	total_percent_distribution,
	inactive_date,
	last_update_login,
	creation_date,
	created_by,
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
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	org_id,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
from
	bec_raw_dl_ext.ap_distribution_sets_all 
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (distribution_set_id,KCA_SEQ_ID) in 
	(select distribution_set_id,max(KCA_SEQ_ID) from bec_raw_dl_ext.AP_DISTRIBUTION_SETS_ALL 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by distribution_set_id)
     and kca_seq_date > (select (executebegints-prune_days) 
	from bec_etl_ctrl.batch_ods_info where ods_table_name ='ap_distribution_sets_all')
	);
END;