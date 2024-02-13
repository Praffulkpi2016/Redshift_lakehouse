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

DROP TABLE IF EXISTS bec_ods_stg.BOM_STANDARD_OPERATIONS;

COMMIT;

CREATE TABLE bec_ods_stg.bom_standard_operations 
DISTKEY (STANDARD_OPERATION_ID)
SORTKEY (last_update_date, LINE_ID, OPERATION_TYPE, STANDARD_OPERATION_ID)
AS 
SELECT * FROM bec_raw_dl_ext.bom_standard_operations 
where 
  kca_operation != 'DELETE' 
  and (
    nvl(LINE_ID, 0), 
	nvl(OPERATION_TYPE, 0), 
	nvl(STANDARD_OPERATION_ID, 0),
    last_update_date
  ) in (
    select 
      nvl(LINE_ID, 0) as LINE_ID, 
	  nvl(OPERATION_TYPE, 0) as OPERATION_TYPE, 
	  nvl(STANDARD_OPERATION_ID, 0) as STANDARD_OPERATION_ID,
      max(last_update_date) as last_update_date
    from 
      bec_raw_dl_ext.bom_standard_operations 
    where 
      kca_operation != 'DELETE' 
      and nvl(kca_seq_id, '') = '' 
    group by 
      nvl(LINE_ID, 0),
	  nvl(OPERATION_TYPE, 0),
	  nvl(STANDARD_OPERATION_ID, 0)
  );
END;
