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

delete from bec_ods.MSC_COMPANIES
where (COMPANY_ID,COMPANY_NAME) in (
select stg.COMPANY_ID,stg.COMPANY_NAME from bec_ods.MSC_COMPANIES ods, bec_ods_stg.MSC_COMPANIES stg
where ods.COMPANY_ID = stg.COMPANY_ID and ods.COMPANY_NAME = stg.COMPANY_NAME and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.MSC_COMPANIES
       (COMPANY_ID,
		COMPANY_NAME,
		DISABLE_DATE,
		REFRESH_NUMBER,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_LOGIN,
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
		KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
	kca_seq_date)	
(
	select
		COMPANY_ID,
		COMPANY_NAME,
		DISABLE_DATE,
		REFRESH_NUMBER,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_LOGIN,
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
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
	from bec_ods_stg.MSC_COMPANIES
	where kca_operation IN ('INSERT','UPDATE') 
	and (COMPANY_ID,COMPANY_NAME,kca_seq_id) in 
	(select COMPANY_ID,COMPANY_NAME,max(kca_seq_id) from bec_ods_stg.MSC_COMPANIES 
     where kca_operation IN ('INSERT','UPDATE')
     group by COMPANY_ID,COMPANY_NAME)
);

commit;
 
-- Soft delete
update bec_ods.MSC_COMPANIES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MSC_COMPANIES set IS_DELETED_FLG = 'Y'
where (COMPANY_ID,COMPANY_NAME)  in
(
select COMPANY_ID,COMPANY_NAME from bec_raw_dl_ext.MSC_COMPANIES
where (COMPANY_ID,COMPANY_NAME,KCA_SEQ_ID)
in 
(
select COMPANY_ID,COMPANY_NAME,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MSC_COMPANIES
group by COMPANY_ID,COMPANY_NAME
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'msc_companies';

commit;