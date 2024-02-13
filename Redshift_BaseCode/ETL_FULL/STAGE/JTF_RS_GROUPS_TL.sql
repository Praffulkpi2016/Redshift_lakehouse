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

DROP TABLE IF EXISTS bec_ods_stg.JTF_RS_GROUPS_TL;

COMMIT;

CREATE TABLE bec_ods_stg.JTF_RS_GROUPS_TL 
DISTKEY (GROUP_ID)
SORTKEY (GROUP_ID, LANGUAGE, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.JTF_RS_GROUPS_TL 
where 
kca_operation != 'DELETE' 
  and (
    nvl(GROUP_ID, 0), 
	nvl(LANGUAGE, 'NA'),
    last_update_date
  ) in (
    select 
      nvl(GROUP_ID, 0) as GROUP_ID, 
	  nvl(LANGUAGE, 'NA') as LANGUAGE,
      max(last_update_date) as last_update_date
    from 
      bec_raw_dl_ext.JTF_RS_GROUPS_TL 
    where 
      kca_operation != 'DELETE' 
      and nvl(kca_seq_id, '') = '' 
    group by 
      nvl(GROUP_ID, 0),
	  nvl(LANGUAGE, 'NA')
  );
END;
