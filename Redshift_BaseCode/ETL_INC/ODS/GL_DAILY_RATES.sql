/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
begin;

-- Delete Records

delete from bec_ods.gl_daily_rates
where (FROM_CURRENCY, TO_CURRENCY, CONVERSION_DATE, CONVERSION_TYPE) in (
select stg.FROM_CURRENCY,stg.TO_CURRENCY,stg.CONVERSION_DATE,stg.CONVERSION_TYPE from bec_ods.gl_daily_rates ods, bec_ods_stg.gl_daily_rates stg
where ods.FROM_CURRENCY = stg.FROM_CURRENCY and ods.TO_CURRENCY = stg.TO_CURRENCY and ods.CONVERSION_DATE = stg.CONVERSION_DATE 
and ods.CONVERSION_TYPE = stg.CONVERSION_TYPE  and stg.kca_operation IN ('INSERT','UPDATE') );

commit;
 -- Insert records

insert into bec_ods.gl_daily_rates
(
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
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID,
	kca_seq_date
)
(
select FROM_CURRENCY,
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
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from bec_ods_stg.gl_daily_rates
where kca_operation IN ('INSERT','UPDATE') and (FROM_CURRENCY, TO_CURRENCY, CONVERSION_DATE, CONVERSION_TYPE,kca_seq_id) in (select FROM_CURRENCY, TO_CURRENCY, CONVERSION_DATE, CONVERSION_TYPE,max(kca_seq_id) from bec_ods_stg.gl_daily_rates 
where kca_operation IN ('INSERT','UPDATE')
group by FROM_CURRENCY, TO_CURRENCY, CONVERSION_DATE, CONVERSION_TYPE)
);

commit;
 
-- Soft delete
update bec_ods.gl_daily_rates set IS_DELETED_FLG = 'N';
commit;
update bec_ods.gl_daily_rates set IS_DELETED_FLG = 'Y'
where (FROM_CURRENCY, TO_CURRENCY, CONVERSION_DATE, CONVERSION_TYPE)  in
(
select FROM_CURRENCY, TO_CURRENCY, CONVERSION_DATE, CONVERSION_TYPE from bec_raw_dl_ext.gl_daily_rates
where (FROM_CURRENCY, TO_CURRENCY, CONVERSION_DATE, CONVERSION_TYPE,KCA_SEQ_ID)
in 
(
select FROM_CURRENCY, TO_CURRENCY, CONVERSION_DATE, CONVERSION_TYPE,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.gl_daily_rates
group by FROM_CURRENCY, TO_CURRENCY, CONVERSION_DATE, CONVERSION_TYPE
) 
and kca_operation= 'DELETE'
);
commit;
end; 

update bec_etl_ctrl.batch_ods_info set last_refresh_date = getdate() where ods_table_name='gl_daily_rates';
commit;

