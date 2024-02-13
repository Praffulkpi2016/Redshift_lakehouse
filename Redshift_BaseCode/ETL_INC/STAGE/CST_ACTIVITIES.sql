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

truncate table bec_ods_stg.cst_activities;

insert into	bec_ods_stg.cst_activities
   (activity_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    activity,
    organization_id,
    description,
    default_basis_type,
    disable_date,
    output_uom,
    value_added_activity_flag,
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
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    zd_edition_name,
    zd_sync,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		activity_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        activity,
        organization_id,
        description,
        default_basis_type,
        disable_date,
        output_uom,
        value_added_activity_flag,
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
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        zd_edition_name,
        zd_sync,
	    KCA_OPERATION,
	    kca_seq_id
		,kca_seq_date
	from bec_raw_dl_ext.cst_activities
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (ACTIVITY_ID,kca_seq_id) in 
	(select ACTIVITY_ID,max(kca_seq_id) from bec_raw_dl_ext.cst_activities 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by ACTIVITY_ID)
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'cst_activities')
);
end;