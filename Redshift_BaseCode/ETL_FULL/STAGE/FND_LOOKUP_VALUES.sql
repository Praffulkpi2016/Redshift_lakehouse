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
DROP TABLE IF EXISTS bec_ods_stg.FND_LOOKUP_VALUES;

CREATE TABLE bec_ods_stg.FND_LOOKUP_VALUES 
DISTKEY (LOOKUP_TYPE)
SORTKEY (LOOKUP_TYPE, LANGUAGE, LOOKUP_CODE, SECURITY_GROUP_ID, VIEW_APPLICATION_ID, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.FND_LOOKUP_VALUES
where kca_operation != 'DELETE' and (LOOKUP_TYPE,LANGUAGE,LOOKUP_CODE,SECURITY_GROUP_ID,VIEW_APPLICATION_ID,last_update_date) in (select LOOKUP_TYPE,LANGUAGE,LOOKUP_CODE,SECURITY_GROUP_ID,VIEW_APPLICATION_ID,max(last_update_date)
from bec_raw_dl_ext.FND_LOOKUP_VALUES 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by LOOKUP_TYPE,LANGUAGE,LOOKUP_CODE,SECURITY_GROUP_ID,VIEW_APPLICATION_ID);
END;
