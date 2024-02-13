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

DROP TABLE IF EXISTS bec_ods_stg.PO_AGENTS;

CREATE TABLE bec_ods_stg.PO_AGENTS 
DISTKEY (AGENT_ID)
SORTKEY (AGENT_ID, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.PO_AGENTS
where kca_operation != 'DELETE' 
and (AGENT_ID,last_update_date) in 
(select AGENT_ID,max(last_update_date) from bec_raw_dl_ext.PO_AGENTS
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by AGENT_ID);

END;
