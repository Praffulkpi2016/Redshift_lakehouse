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

DROP TABLE IF EXISTS bec_ods_stg.WSH_SERIAL_NUMBERS;

CREATE TABLE bec_ods_stg.WSH_SERIAL_NUMBERS 
DISTSTYLE AUTO
SORTKEY (DELIVERY_DETAIL_ID, FM_SERIAL_NUMBER, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.wsh_serial_numbers 
where 
  kca_operation != 'DELETE' 
  and (
    nvl(DELIVERY_DETAIL_ID, 0), 
	nvl(FM_SERIAL_NUMBER, 'NA'),
    last_update_date
  ) in (
    select 
      nvl(DELIVERY_DETAIL_ID, 0) as DELIVERY_DETAIL_ID, 
	  nvl(FM_SERIAL_NUMBER, 'NA') as FM_SERIAL_NUMBER,
      max(last_update_date) as last_update_date
    from 
      bec_raw_dl_ext.wsh_serial_numbers 
    where 
      kca_operation != 'DELETE' 
      and nvl(kca_seq_id, '') = '' 
    group by 
      nvl(DELIVERY_DETAIL_ID, 0),
	  nvl(FM_SERIAL_NUMBER, 'NA')
  );
END;
