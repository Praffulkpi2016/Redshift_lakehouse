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

DROP TABLE IF EXISTS bec_ods_stg.MTL_INTERORG_SHIP_METHODS;

CREATE TABLE bec_ods_stg.MTL_INTERORG_SHIP_METHODS 
DISTSTYLE ALL
SORTKEY ( FROM_ORGANIZATION_ID, TO_ORGANIZATION_ID, FROM_LOCATION_ID, TO_LOCATION_ID, SHIP_METHOD, last_update_date)

AS 
SELECT * FROM bec_raw_dl_ext.MTL_INTERORG_SHIP_METHODS
where kca_operation != 'DELETE' 
and (nvl(FROM_ORGANIZATION_ID,0),nvl(TO_ORGANIZATION_ID,0),
nvl(FROM_LOCATION_ID,0),nvl(TO_LOCATION_ID,0),nvl(SHIP_METHOD,'0'),last_update_date) in 
(select nvl(FROM_ORGANIZATION_ID,0),nvl(TO_ORGANIZATION_ID,0),nvl(FROM_LOCATION_ID,0),nvl(TO_LOCATION_ID,0),
nvl(SHIP_METHOD,'0'),max(last_update_date) 
from bec_raw_dl_ext.MTL_INTERORG_SHIP_METHODS 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by nvl(FROM_ORGANIZATION_ID,0),nvl(TO_ORGANIZATION_ID,0),nvl(FROM_LOCATION_ID,0),nvl(TO_LOCATION_ID,0),nvl(SHIP_METHOD,'0'));

END;