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

DROP TABLE IF EXISTS bec_ods_stg.CSF_DEBRIEF_LINES;

CREATE TABLE bec_ods_stg.CSF_DEBRIEF_LINES 
DISTKEY (DEBRIEF_LINE_ID)
SORTKEY (kca_operation, DEBRIEF_LINE_ID, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.CSF_DEBRIEF_LINES 
where 
  kca_operation != 'DELETE' 
  and (
    nvl(DEBRIEF_LINE_ID, 0), 
    last_update_date
  ) in (
    select 
      nvl(DEBRIEF_LINE_ID, 0) as DEBRIEF_LINE_ID, 
      max(last_update_date) as last_update_date
    from 
      bec_raw_dl_ext.CSF_DEBRIEF_LINES 
    where 
      kca_operation != 'DELETE' 
      and nvl(kca_seq_id, '') = '' 
    group by 
      nvl(DEBRIEF_LINE_ID, 0)
  );
END;
