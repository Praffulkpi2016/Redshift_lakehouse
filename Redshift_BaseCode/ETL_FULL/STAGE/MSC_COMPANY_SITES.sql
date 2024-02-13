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
DROP TABLE IF EXISTS bec_ods_stg.MSC_COMPANY_SITES; 

CREATE TABLE bec_ods_stg.MSC_COMPANY_SITES 
	DISTKEY (COMPANY_SITE_ID)
SORTKEY (COMPANY_ID, COMPANY_SITE_ID, last_update_date)
	AS (SELECT * FROM bec_raw_dl_ext.MSC_COMPANY_SITES
WHERE kca_operation != 'DELETE' and 
(nvl(COMPANY_ID,0),nvl(COMPANY_SITE_ID,0)  ,last_update_date)
in (select nvl(COMPANY_ID,0) as COMPANY_ID ,nvl(COMPANY_SITE_ID,0) AS COMPANY_SITE_ID ,max(last_update_date) from bec_raw_dl_ext.MSC_COMPANY_SITES 
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by nvl(COMPANY_ID,0),nvl(COMPANY_SITE_ID,0) 
)
);

END;


