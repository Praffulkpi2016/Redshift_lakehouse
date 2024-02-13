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

DROP TABLE IF EXISTS bec_ods_stg.FND_RESPONSIBILITY;

CREATE TABLE bec_ods_stg.FND_RESPONSIBILITY 
DISTKEY (APPLICATION_ID)
SORTKEY ( APPLICATION_ID, RESPONSIBILITY_ID, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.FND_RESPONSIBILITY
where kca_operation != 'DELETE' 
and (nvl(APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0),last_update_date) in 
(select nvl(APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0),max(last_update_date) from bec_raw_dl_ext.FND_RESPONSIBILITY 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by nvl(APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0));

END;