/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental approach for ODS.
# File Version: KPI v1.0
*/
begin;
truncate table bec_ods.OE_HOLD_AUTHORIZATIONS;

insert into bec_ods.OE_HOLD_AUTHORIZATIONS
(
	hold_id,
	responsibility_id,
	application_id,
	authorized_action_code,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	start_date_active,
	end_date_active,
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
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
(
select 
	hold_id,
	responsibility_id,
	application_id,
	authorized_action_code,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	start_date_active,
	end_date_active,
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
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	KCA_SEQ_DATE 
from bec_ods_stg.OE_HOLD_AUTHORIZATIONS
);

end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='oe_hold_authorizations'; 

commit;