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

DROP TABLE IF EXISTS bec_ods_stg.MTL_PLANNERS;

CREATE TABLE bec_ods_stg.MTL_PLANNERS 
DISTKEY (ORGANIZATION_ID)
SORTKEY (  ORGANIZATION_ID, PLANNER_CODE, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.MTL_PLANNERS 
where 
  kca_operation != 'DELETE' 
  and (
    nvl(ORGANIZATION_ID, 0), 
	nvl(PLANNER_CODE, 'NA'),
    last_update_date
  ) in (
    select 
      nvl(ORGANIZATION_ID, 0) as ORGANIZATION_ID, 
	  nvl(PLANNER_CODE, 'NA') as PLANNER_CODE,
      max(last_update_date) as last_update_date
    from 
      bec_raw_dl_ext.MTL_PLANNERS 
    where 
      kca_operation != 'DELETE' 
      and nvl(kca_seq_id, '') = '' 
    group by 
      nvl(ORGANIZATION_ID, 0),
	  nvl(PLANNER_CODE, 'NA')
  );
END;
