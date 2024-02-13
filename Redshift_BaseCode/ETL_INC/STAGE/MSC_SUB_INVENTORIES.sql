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

TRUNCATE TABLE bec_ods_stg.MSC_SUB_INVENTORIES;

insert into	bec_ods_stg.MSC_SUB_INVENTORIES
   (
     PLAN_ID
	,ORGANIZATION_ID
	,SUB_INVENTORY_CODE
	,NETTING_TYPE
	,SR_INSTANCE_ID
	,DESCRIPTION
	,REFRESH_NUMBER
	,INVENTORY_ATP_CODE
	,LAST_UPDATE_DATE
	,LAST_UPDATED_BY
	,CREATION_DATE
	,CREATED_BY
	,LAST_UPDATE_LOGIN
	,REQUEST_ID
	,PROGRAM_APPLICATION_ID
	,PROGRAM_ID
	,PROGRAM_UPDATE_DATE
	,ATTRIBUTE_CATEGORY
	,ATTRIBUTE1
	,ATTRIBUTE2
	,ATTRIBUTE3
	,ATTRIBUTE4
	,ATTRIBUTE5
	,ATTRIBUTE6
	,ATTRIBUTE7
	,ATTRIBUTE8
	,ATTRIBUTE9
	,ATTRIBUTE10
	,ATTRIBUTE11
	,ATTRIBUTE12
	,ATTRIBUTE13
	,ATTRIBUTE14
	,ATTRIBUTE15
	,INCLUDE_NON_NETTABLE
	,SR_RESOURCE_NAME
	,SR_CUSTOMER_ACCT_ID
	,CONDITION_TYPE
	,KCA_OPERATION
	,kca_seq_id
	,kca_seq_date)
(select
	PLAN_ID
	,ORGANIZATION_ID
	,SUB_INVENTORY_CODE
	,NETTING_TYPE
	,SR_INSTANCE_ID
	,DESCRIPTION
	,REFRESH_NUMBER
	,INVENTORY_ATP_CODE
	,LAST_UPDATE_DATE
	,LAST_UPDATED_BY
	,CREATION_DATE
	,CREATED_BY
	,LAST_UPDATE_LOGIN
	,REQUEST_ID
	,PROGRAM_APPLICATION_ID
	,PROGRAM_ID
	,PROGRAM_UPDATE_DATE
	,ATTRIBUTE_CATEGORY
	,ATTRIBUTE1
	,ATTRIBUTE2
	,ATTRIBUTE3
	,ATTRIBUTE4
	,ATTRIBUTE5
	,ATTRIBUTE6
	,ATTRIBUTE7
	,ATTRIBUTE8
	,ATTRIBUTE9
	,ATTRIBUTE10
	,ATTRIBUTE11
	,ATTRIBUTE12
	,ATTRIBUTE13
	,ATTRIBUTE14
	,ATTRIBUTE15
	,INCLUDE_NON_NETTABLE
	,SR_RESOURCE_NAME
	,SR_CUSTOMER_ACCT_ID
	,CONDITION_TYPE
	,KCA_OPERATION
	,kca_seq_id
	,kca_seq_date
from 
  bec_raw_dl_ext.MSC_SUB_INVENTORIES 
where 
  kca_operation != 'DELETE' 
  and nvl(kca_seq_id, '')!= '' 
  and (
    nvl(PLAN_ID, 0), 
    nvl(SR_INSTANCE_ID, 0), 
    nvl(ORGANIZATION_ID, 0), 
    nvl(SUB_INVENTORY_CODE, 'NA'), 
    KCA_SEQ_ID
  ) in (
    select 
      nvl(PLAN_ID, 0) as PLAN_ID, 
      nvl(SR_INSTANCE_ID, 0) as SR_INSTANCE_ID, 
      nvl(ORGANIZATION_ID, 0) as ORGANIZATION_ID, 
      nvl(SUB_INVENTORY_CODE, 'NA') as SUB_INVENTORY_CODE, 
      max(KCA_SEQ_ID) 
    from 
      bec_raw_dl_ext.MSC_SUB_INVENTORIES 
    where 
      kca_operation != 'DELETE' 
      and nvl(kca_seq_id, '')!= '' 
    group by 
      nvl(PLAN_ID, 0), 
      nvl(SR_INSTANCE_ID, 0), 
      nvl(ORGANIZATION_ID, 0), 
      nvl(SUB_INVENTORY_CODE, 'NA')
  ) 
  and kca_seq_date > (
    select 
      (executebegints - prune_days) 
    from 
      bec_etl_ctrl.batch_ods_info 
    where 
      ods_table_name = 'msc_sub_inventories'
  )
);

END;

