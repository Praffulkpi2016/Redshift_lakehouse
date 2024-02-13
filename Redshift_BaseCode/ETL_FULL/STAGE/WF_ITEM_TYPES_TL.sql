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

DROP TABLE IF EXISTS bec_ods_stg.WF_ITEM_TYPES_TL;

CREATE TABLE bec_ods_stg.WF_ITEM_TYPES_TL 
DISTKEY (NAME)
SORTKEY (NAME, LANGUAGE)
AS 
SELECT * FROM bec_raw_dl_ext.WF_ITEM_TYPES_TL
where kca_operation != 'DELETE' 
and (nvl(NAME,'NA'),nvl(LANGUAGE,'NA') ) in 
(select nvl(NAME,'NA') AS NAME ,nvl(LANGUAGE,'NA') AS LANGUAGE from bec_raw_dl_ext.WF_ITEM_TYPES_TL 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by nvl(NAME,'NA'),nvl(LANGUAGE,'NA'));

END;