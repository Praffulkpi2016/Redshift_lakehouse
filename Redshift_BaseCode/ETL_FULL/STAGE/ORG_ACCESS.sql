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

DROP TABLE IF EXISTS bec_ods_stg.ORG_ACCESS;

CREATE TABLE bec_ods_stg.ORG_ACCESS 
DISTKEY (RESP_APPLICATION_ID)
SORTKEY (RESP_APPLICATION_ID, RESPONSIBILITY_ID, ORGANIZATION_ID, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.ORG_ACCESS
where kca_operation != 'DELETE' 
and (nvl(RESP_APPLICATION_ID,0),nvl(RESPONSIBILITY_ID, 0 ),nvl(ORGANIZATION_ID, 0 ),last_update_date) in 
(select nvl(RESP_APPLICATION_ID,0) as RESP_APPLICATION_ID,nvl(RESPONSIBILITY_ID, 0 ) as RESPONSIBILITY_ID,nvl(ORGANIZATION_ID, 0 ) AS ORGANIZATION_ID,max(last_update_date) from bec_raw_dl_ext.ORG_ACCESS 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by nvl(RESP_APPLICATION_ID,0),nvl(RESPONSIBILITY_ID, 0 ),nvl(ORGANIZATION_ID, 0 ));

END;