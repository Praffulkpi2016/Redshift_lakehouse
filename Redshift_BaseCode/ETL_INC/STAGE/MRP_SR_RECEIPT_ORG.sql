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

truncate table bec_ods_stg.MRP_SR_RECEIPT_ORG;

insert into	bec_ods_stg.MRP_SR_RECEIPT_ORG
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
    ,KCA_OPERATION,
	kca_seq_id
	,KCA_SEQ_DATE)
(
	select
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
		,kca_seq_id
		,KCA_SEQ_DATE
	from bec_raw_dl_ext.MRP_SR_RECEIPT_ORG
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	
	and     (NVL(SR_RECEIPT_ID,0),kca_seq_id)
	         in 
	        (
	         select
         	 NVL(SR_RECEIPT_ID,0) AS SR_RECEIPT_ID , max(kca_seq_id) from bec_raw_dl_ext.MRP_SR_RECEIPT_ORG 
             where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
             group by 
	         NVL(SR_RECEIPT_ID,0)
	         )
			 
    and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'mrp_sr_receipt_org')
            )
);
end;