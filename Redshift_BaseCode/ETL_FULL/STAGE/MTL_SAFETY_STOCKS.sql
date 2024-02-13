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

DROP TABLE IF EXISTS bec_ods_stg.MTL_SAFETY_STOCKS;

CREATE TABLE bec_ods_stg.mtl_safety_stocks 
DISTKEY (INVENTORY_ITEM_ID)
SORTKEY (INVENTORY_ITEM_ID, ORGANIZATION_ID, EFFECTIVITY_DATE, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.mtl_safety_stocks 
where 
  kca_operation != 'DELETE' 
  and (
    nvl(INVENTORY_ITEM_ID, 0), 
	nvl(ORGANIZATION_ID, 0), 
	nvl(EFFECTIVITY_DATE, '1900-01-01 12:00:00'),
    last_update_date
  ) in (
    select 
      nvl(INVENTORY_ITEM_ID, 0) as INVENTORY_ITEM_ID, 
	  nvl(ORGANIZATION_ID, 0) as ORGANIZATION_ID, 
	  nvl(EFFECTIVITY_DATE, '1900-01-01 12:00:00') as EFFECTIVITY_DATE,
      max(last_update_date) as last_update_date
    from 
      bec_raw_dl_ext.mtl_safety_stocks 
    where 
      kca_operation != 'DELETE' 
      and nvl(kca_seq_id, '') = '' 
    group by 
      nvl(INVENTORY_ITEM_ID, 0),
	  nvl(ORGANIZATION_ID, 0),
	  nvl(EFFECTIVITY_DATE, '1900-01-01 12:00:00')
  );
END;
