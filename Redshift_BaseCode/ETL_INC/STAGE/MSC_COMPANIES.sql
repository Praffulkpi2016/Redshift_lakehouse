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

truncate table bec_ods_stg.MSC_COMPANIES;

insert into	bec_ods_stg.MSC_COMPANIES
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
		kca_seq_id,
	kca_seq_date
	from bec_raw_dl_ext.MSC_COMPANIES
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (COMPANY_ID,COMPANY_NAME,kca_seq_id) in 
	(select COMPANY_ID,COMPANY_NAME,max(kca_seq_id) from bec_raw_dl_ext.MSC_COMPANIES 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by COMPANY_ID,COMPANY_NAME)
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'msc_companies')
			 
            )
);
end;