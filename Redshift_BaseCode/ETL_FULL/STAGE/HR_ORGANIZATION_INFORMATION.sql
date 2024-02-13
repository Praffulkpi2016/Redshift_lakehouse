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
DROP TABLE IF EXISTS bec_ods_stg.HR_ORGANIZATION_INFORMATION;

CREATE TABLE bec_ods_stg.HR_ORGANIZATION_INFORMATION 
DISTKEY (ORG_INFORMATION_ID)
SORTKEY (ORG_INFORMATION_ID, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.HR_ORGANIZATION_INFORMATION
WHERE kca_operation != 'DELETE' and (ORG_INFORMATION_ID,last_update_date)
in  (select ORG_INFORMATION_ID,max(last_update_date) from bec_raw_dl_ext.HR_ORGANIZATION_INFORMATION 
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by ORG_INFORMATION_ID);
END;