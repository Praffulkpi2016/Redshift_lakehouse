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

TRUNCATE TABLE bec_ods_stg.GL_BUDGET_VERSIONS;

insert into	bec_ods_stg.GL_BUDGET_VERSIONS
    (budget_version_id,
	last_update_date,
	last_updated_by,
	budget_type,
	budget_name,
	version_num,
	status,
	date_opened,
	creation_date,
	created_by,
	last_update_login,
	description,
	date_active,
	date_archived,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	context,
	control_budget_version_id,
	igi_bud_nyc_flag,
	KCA_OPERATION,
	KCA_SEQ_ID
	,kca_seq_date
	)
(select
	budget_version_id,
	last_update_date,
	last_updated_by,
	budget_type,
	budget_name,
	version_num,
	status,
	date_opened,
	creation_date,
	created_by,
	last_update_login,
	description,
	date_active,
	date_archived,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	context,
	control_budget_version_id,
	igi_bud_nyc_flag,
	KCA_OPERATION,
	KCA_SEQ_ID
	,kca_seq_date
from
	bec_raw_dl_ext.GL_BUDGET_VERSIONS 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (BUDGET_VERSION_ID,KCA_SEQ_ID) in 
	(select BUDGET_VERSION_ID,max(KCA_SEQ_ID) from bec_raw_dl_ext.GL_BUDGET_VERSIONS 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by BUDGET_VERSION_ID)
     and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='gl_budget_versions')
	             )
	 );
END;