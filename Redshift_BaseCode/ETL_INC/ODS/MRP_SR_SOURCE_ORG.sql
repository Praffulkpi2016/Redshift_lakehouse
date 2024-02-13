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

delete from bec_ods.MRP_SR_SOURCE_ORG
where NVL(SR_SOURCE_ID,0) in (
select NVL(stg.SR_SOURCE_ID,0) from bec_ods.MRP_SR_SOURCE_ORG ods, bec_ods_stg.MRP_SR_SOURCE_ORG stg
where ods.SR_SOURCE_ID = stg.SR_SOURCE_ID and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.MRP_SR_SOURCE_ORG
       (	
	sr_source_id,
	sr_receipt_id,
	source_organization_id,
	vendor_id,
	vendor_site_id,
	secondary_inventory,
	source_type,
	allocation_percent,
	"rank",
	old_rank,
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
	ship_method,
      KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)
( 
SELECT
	sr_source_id,
	sr_receipt_id,
	source_organization_id,
	vendor_id,
	vendor_site_id,
	secondary_inventory,
	source_type,
	allocation_percent,
	"rank",
	old_rank,
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
	ship_method,		
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.MRP_SR_SOURCE_ORG
	where kca_operation IN ('INSERT','UPDATE') 
	and (SR_SOURCE_ID,kca_seq_id) in 
	(select SR_SOURCE_ID,max(kca_seq_id) from bec_ods_stg.MRP_SR_SOURCE_ORG 
     where kca_operation IN ('INSERT','UPDATE')
     group by SR_SOURCE_ID)
);

commit;

-- Soft delete
update bec_ods.MRP_SR_SOURCE_ORG set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MRP_SR_SOURCE_ORG set IS_DELETED_FLG = 'Y'
where (SR_SOURCE_ID)  in
(
select SR_SOURCE_ID from bec_raw_dl_ext.MRP_SR_SOURCE_ORG
where (SR_SOURCE_ID,KCA_SEQ_ID)
in 
(
select SR_SOURCE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MRP_SR_SOURCE_ORG
group by SR_SOURCE_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'mrp_sr_source_org';

commit;