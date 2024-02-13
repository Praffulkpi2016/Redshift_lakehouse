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

truncate table bec_ods_stg.MRP_SOURCING_RULES;

insert into	bec_ods_stg.MRP_SOURCING_RULES
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
		kca_seq_id,
		kca_seq_date
	from bec_raw_dl_ext.MRP_SOURCING_RULES
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (SOURCING_RULE_ID,kca_seq_id) in 
	(select SOURCING_RULE_ID,max(kca_seq_id) from bec_raw_dl_ext.MRP_SOURCING_RULES 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by SOURCING_RULE_ID)
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'mrp_sourcing_rules')
);
end;