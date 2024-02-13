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

DROP TABLE IF EXISTS bec_ods_stg.AR_RECEIVABLES_TRX_ALL;

CREATE TABLE bec_ods_stg.AR_RECEIVABLES_TRX_ALL 
DISTKEY (RECEIVABLES_TRX_ID)
SORTKEY (RECEIVABLES_TRX_ID, LAST_UPDATE_DATE)
AS 
SELECT * FROM bec_raw_dl_ext.AR_RECEIVABLES_TRX_ALL
where kca_operation != 'DELETE' 
and (RECEIVABLES_TRX_ID,nvl(org_id,0),last_update_date) in 
(select RECEIVABLES_TRX_ID,nvl(org_id,0),max(last_update_date) from bec_raw_dl_ext.AR_RECEIVABLES_TRX_ALL 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by RECEIVABLES_TRX_ID,nvl(org_id,0));

END;