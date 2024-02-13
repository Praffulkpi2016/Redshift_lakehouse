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
	table bec_ods_stg.gl_daily_rates;

insert
	into
	bec_ods_stg.gl_daily_rates
(FROM_CURRENCY,
TO_CURRENCY,
CONVERSION_DATE,
CONVERSION_TYPE,
CONVERSION_RATE,
STATUS_CODE,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
LAST_UPDATE_LOGIN,
CONTEXT,
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
RATE_SOURCE_CODE
,kca_operation
,kca_seq_id,
	kca_seq_date)
(
	select
	FROM_CURRENCY,
TO_CURRENCY,
CONVERSION_DATE,
CONVERSION_TYPE,
CONVERSION_RATE,
STATUS_CODE,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
LAST_UPDATE_LOGIN,
CONTEXT,
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
RATE_SOURCE_CODE
,kca_operation
,kca_seq_id,
	kca_seq_date
	from
		bec_raw_dl_ext.gl_daily_rates
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' and (FROM_CURRENCY,TO_CURRENCY,CONVERSION_DATE,CONVERSION_TYPE,kca_seq_id) in  (select FROM_CURRENCY,TO_CURRENCY,CONVERSION_DATE,CONVERSION_TYPE,max(kca_seq_id) from bec_raw_dl_ext.GL_DAILY_RATES 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
group by FROM_CURRENCY,TO_CURRENCY,CONVERSION_DATE,CONVERSION_TYPE) and
		( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'gl_daily_rates')
		 
            )
);
end;
