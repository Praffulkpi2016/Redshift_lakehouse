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
DROP TABLE IF EXISTS bec_ods_stg.HR_ALL_ORGANIZATION_UNITS_TL;

CREATE TABLE bec_ods_stg.HR_ALL_ORGANIZATION_UNITS_TL 
DISTKEY (ORGANIZATION_ID)
SORTKEY ( ORGANIZATION_ID, LANGUAGE, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.HR_ALL_ORGANIZATION_UNITS_TL
WHERE kca_operation != 'DELETE' and (ORGANIZATION_ID,LANGUAGE,last_update_date)
in  (select ORGANIZATION_ID,LANGUAGE,max(last_update_date) from bec_raw_dl_ext.HR_ALL_ORGANIZATION_UNITS_TL 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by ORGANIZATION_ID,LANGUAGE);
END;