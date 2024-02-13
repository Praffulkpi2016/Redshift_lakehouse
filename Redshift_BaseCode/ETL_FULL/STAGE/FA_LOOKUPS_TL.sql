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

DROP TABLE IF EXISTS bec_ods_stg.FA_LOOKUPS_TL;

CREATE TABLE bec_ods_stg.FA_LOOKUPS_TL 
DISTKEY (LOOKUP_TYPE)
SORTKEY ( LOOKUP_TYPE, LOOKUP_CODE, language, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.FA_LOOKUPS_TL
where kca_operation != 'DELETE' 
and (nvl(LOOKUP_TYPE,'NA'),nvl(LOOKUP_CODE,'NA'),nvl(language,'NA'),last_update_date) in 
(select nvl(LOOKUP_TYPE,'NA') as LOOKUP_TYPE,nvl(LOOKUP_CODE,'NA') as LOOKUP_CODE,nvl(language,'NA') as language,max(last_update_date) as last_update_date from bec_raw_dl_ext.FA_LOOKUPS_TL 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by nvl(LOOKUP_TYPE,'NA'),nvl(LOOKUP_CODE,'NA'),nvl(language,'NA'));

END;