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
DROP TABLE IF EXISTS bec_ods_stg.RCV_SHIPMENT_HEADERS;

CREATE TABLE bec_ods_stg.RCV_SHIPMENT_HEADERS 
DISTKEY (SHIPMENT_HEADER_ID)
SORTKEY (SHIPMENT_HEADER_ID, last_update_date)
AS SELECT * FROM bec_raw_dl_ext.RCV_SHIPMENT_HEADERS
where kca_operation != 'DELETE' and (SHIPMENT_HEADER_ID,last_update_date) in (select SHIPMENT_HEADER_ID,max(last_update_date) 
from bec_raw_dl_ext.RCV_SHIPMENT_HEADERS 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by SHIPMENT_HEADER_ID);

END;
