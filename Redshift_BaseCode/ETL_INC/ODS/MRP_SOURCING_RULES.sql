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

delete from bec_ods.MRP_SOURCING_RULES
where NVL(SOURCING_RULE_ID,0) in (
select NVL(stg.SOURCING_RULE_ID,0) from bec_ods.MRP_SOURCING_RULES ods, bec_ods_stg.MRP_SOURCING_RULES stg
where ods.SOURCING_RULE_ID = stg.SOURCING_RULE_ID and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.MRP_SOURCING_RULES
       (	
	sourcing_rule_id,
	sourcing_rule_name,
	organization_id,
	description,
	status,
	sourcing_rule_type,
	last_update_date,
	last_updated_by,
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
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	planning_active,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
	sourcing_rule_id,
	sourcing_rule_name,
	organization_id,
	description,
	status,
	sourcing_rule_type,
	last_update_date,
	last_updated_by,
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
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	planning_active,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.MRP_SOURCING_RULES
	where kca_operation IN ('INSERT','UPDATE') 
	and (SOURCING_RULE_ID,kca_seq_id) in 
	(select SOURCING_RULE_ID,max(kca_seq_id) from bec_ods_stg.MRP_SOURCING_RULES 
     where kca_operation IN ('INSERT','UPDATE')
     group by SOURCING_RULE_ID)
);

commit;

-- Soft delete
update bec_ods.MRP_SOURCING_RULES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MRP_SOURCING_RULES set IS_DELETED_FLG = 'Y'
where (SOURCING_RULE_ID)  in
(
select SOURCING_RULE_ID from bec_raw_dl_ext.MRP_SOURCING_RULES
where (SOURCING_RULE_ID,KCA_SEQ_ID)
in 
(
select SOURCING_RULE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MRP_SOURCING_RULES
group by SOURCING_RULE_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'mrp_sourcing_rules';

commit;