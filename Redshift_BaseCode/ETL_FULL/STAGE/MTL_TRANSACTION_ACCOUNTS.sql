/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for stage.
# File Version: KPI v1.0
*/
BEGIN; 

DROP TABLE IF EXISTS bec_ods_stg.MTL_TRANSACTION_ACCOUNTS;

CREATE TABLE bec_ods_stg.MTL_TRANSACTION_ACCOUNTS 
DISTKEY (TRANSACTION_ID)
SORTKEY (TRANSACTION_ID, REFERENCE_ACCOUNT, COST_ELEMENT_ID, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.MTL_TRANSACTION_ACCOUNTS
where kca_operation != 'DELETE' 
and (nvl(TRANSACTION_ID,0),nvl(REFERENCE_ACCOUNT,0),nvl(COST_ELEMENT_ID,0),nvl(PRIMARY_QUANTITY,0)
,nvl(RESOURCE_ID,0)
,nvl(ACCOUNTING_LINE_TYPE,0)
,nvl(BASE_TRANSACTION_VALUE,0),
last_update_date) in 
(select nvl(TRANSACTION_ID,0),nvl(REFERENCE_ACCOUNT,0),nvl(COST_ELEMENT_ID,0),nvl(PRIMARY_QUANTITY,0)
,nvl(RESOURCE_ID,0)
,nvl(ACCOUNTING_LINE_TYPE,0)
,nvl(BASE_TRANSACTION_VALUE,0),max(last_update_date) from bec_raw_dl_ext.MTL_TRANSACTION_ACCOUNTS 
where kca_operation != 'DELETE' --and nvl(kca_seq_id,'') = ''
group by nvl(TRANSACTION_ID,0),nvl(REFERENCE_ACCOUNT,0),nvl(COST_ELEMENT_ID,0),nvl(PRIMARY_QUANTITY,0)
,nvl(RESOURCE_ID,0)
,nvl(ACCOUNTING_LINE_TYPE,0)
,nvl(BASE_TRANSACTION_VALUE,0));

END;
