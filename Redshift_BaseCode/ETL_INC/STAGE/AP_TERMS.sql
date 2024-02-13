/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
begin;

truncate
	table bec_ods_stg.ap_terms;

insert
	into
	bec_ods_stg.ap_terms
(
TERM_ID,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_LOGIN,
ENABLED_FLAG,
DUE_CUTOFF_DAY,
TYPE,
START_DATE_ACTIVE,
END_DATE_ACTIVE,
RANK,
ATTRIBUTE_CATEGORY,
ATTRIBUTE1,
ATTRIBUTE2,
ATTRIBUTE3,
ATTRIBUTE4,
ATTRIBUTE5,
ATTRIBUTE6,
ATTRIBUTE7,
ATTRIBUTE8,
ATTRIBUTE9,
ATTRIBUTE10,
ATTRIBUTE11,
ATTRIBUTE12,
ATTRIBUTE13,
ATTRIBUTE14,
ATTRIBUTE15,
NAME,
DESCRIPTION,
KCA_OPERATION
,kca_seq_id
,kca_seq_date)
(
	select
	B.TERM_ID,
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
	B.KCA_OPERATION,
	B.kca_seq_id
	,B.kca_seq_date
from
	bec_raw_dl_ext.AP_TERMS_TL B
where B.LANGUAGE = 'US'
AND B.kca_operation != 'DELETE' and nvl(B.kca_seq_id,'')!= ''
and (B.TERM_ID,B.LANGUAGE,B.kca_seq_id) in (select TERM_ID,LANGUAGE,max(kca_seq_id) from bec_raw_dl_ext.AP_TERMS_TL 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
group by TERM_ID,LANGUAGE)
AND B.kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'ap_terms')
);
end;
