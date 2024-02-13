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

delete from bec_ods.MRP_SR_RECEIPT_ORG
where NVL(SR_RECEIPT_ID,0) in (
select NVL(stg.SR_RECEIPT_ID,0) as SR_RECEIPT_ID
from bec_ods.MRP_SR_RECEIPT_ORG ods, bec_ods_stg.MRP_SR_RECEIPT_ORG stg
where ods.SR_RECEIPT_ID = stg.SR_RECEIPT_ID
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into bec_ods.MRP_SR_RECEIPT_ORG
(	
		SR_RECEIPT_ID
,      SOURCING_RULE_ID
,      RECEIPT_ORGANIZATION_ID
,      EFFECTIVE_DATE
,      DISABLE_DATE
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      ATTRIBUTE_CATEGORY
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date)
(select
		SR_RECEIPT_ID
,      SOURCING_RULE_ID
,      RECEIPT_ORGANIZATION_ID
,      EFFECTIVE_DATE
,      DISABLE_DATE
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      ATTRIBUTE_CATEGORY
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,KCA_OPERATION
,'N' AS IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
  KCA_SEQ_DATE
from bec_ods_stg.MRP_SR_RECEIPT_ORG
where kca_operation in ('INSERT','UPDATE') 
	and (nvl(SR_RECEIPT_ID,0),kca_seq_id) in 
	(select nvl(SR_RECEIPT_ID,0) as SR_RECEIPT_ID,max(kca_seq_id) from bec_ods_stg.MRP_SR_RECEIPT_ORG 
     where kca_operation in ('INSERT','UPDATE')
     group by nvl(SR_RECEIPT_ID,0))
);

commit;


-- Soft delete
update bec_ods.MRP_SR_RECEIPT_ORG set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MRP_SR_RECEIPT_ORG set IS_DELETED_FLG = 'Y'
where (SR_RECEIPT_ID)  in
(
select SR_RECEIPT_ID from bec_raw_dl_ext.MRP_SR_RECEIPT_ORG
where (SR_RECEIPT_ID,KCA_SEQ_ID)
in 
(
select SR_RECEIPT_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MRP_SR_RECEIPT_ORG
group by SR_RECEIPT_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info 
set last_refresh_date = getdate() 
where ods_table_name='mrp_sr_receipt_org';
commit;