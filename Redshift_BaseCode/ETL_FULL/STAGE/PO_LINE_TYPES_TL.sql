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

DROP TABLE IF EXISTS bec_ods_stg.PO_LINE_TYPES_TL;

CREATE TABLE bec_ods_stg.PO_LINE_TYPES_TL 
DISTKEY (LINE_TYPE_ID)
SORTKEY (LINE_TYPE_ID, LANGUAGE, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.PO_LINE_TYPES_TL
where kca_operation != 'DELETE' 
and (LINE_TYPE_ID,LANGUAGE,last_update_date) in 
(select LINE_TYPE_ID,LANGUAGE,max(last_update_date) from bec_raw_dl_ext.PO_LINE_TYPES_TL
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by LINE_TYPE_ID,LANGUAGE);
END;
