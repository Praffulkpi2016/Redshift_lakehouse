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
DROP TABLE IF EXISTS bec_ods_stg.AP_TERMS;

CREATE TABLE bec_ods_stg.AP_TERMS 
DISTKEY (TERM_ID)
SORTKEY (TERM_ID, LANGUAGE, LAST_UPDATE_DATE)
 as  
(select
	B.TERM_ID,
	B.LANGUAGE,
	B.LAST_UPDATE_DATE,
	B.LAST_UPDATED_BY,
	B.CREATION_DATE,
	B.CREATED_BY,
	B.LAST_UPDATE_LOGIN,
	B.ENABLED_FLAG,
	B.DUE_CUTOFF_DAY,
	B.TYPE,
	B.START_DATE_ACTIVE,
	B.END_DATE_ACTIVE,
	B.RANK,
	B.ATTRIBUTE_CATEGORY,
	B.ATTRIBUTE1,
	B.ATTRIBUTE2,
	B.ATTRIBUTE3,
	B.ATTRIBUTE4,
	B.ATTRIBUTE5,
	B.ATTRIBUTE6,
	B.ATTRIBUTE7,
	B.ATTRIBUTE8,
	B.ATTRIBUTE9,
	B.ATTRIBUTE10,
	B.ATTRIBUTE11,
	B.ATTRIBUTE12,
	B.ATTRIBUTE13,
	B.ATTRIBUTE14,
	B.ATTRIBUTE15,
	B.NAME,
	B.DESCRIPTION,
	B.kca_operation,
	B.kca_seq_id,
      B.kca_seq_date
from
	bec_raw_dl_ext.AP_TERMS_TL B
where B.LANGUAGE = 'US'
and B.kca_operation != 'DELETE' and (B.term_id,B.LANGUAGE,B.last_update_date) in 
(select term_id,LANGUAGE,max(last_update_date) from bec_raw_dl_ext.AP_TERMS_TL 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by term_id,LANGUAGE));

END;