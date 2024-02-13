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

delete from bec_ods.fa_transaction_headers
where nvl(TRANSACTION_HEADER_ID,0)  in (
select nvl(stg.TRANSACTION_HEADER_ID,0) as TRANSACTION_HEADER_ID from bec_ods.fa_transaction_headers ods, bec_ods_stg.fa_transaction_headers stg
where nvl(ods.TRANSACTION_HEADER_ID,0) = nvl(stg.TRANSACTION_HEADER_ID,0) 
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.fa_transaction_headers
       (TRANSACTION_HEADER_ID,
		BOOK_TYPE_CODE,
		ASSET_ID,
		TRANSACTION_TYPE_CODE,
		TRANSACTION_DATE_ENTERED,
		DATE_EFFECTIVE,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		TRANSACTION_NAME,
		INVOICE_TRANSACTION_ID,
		SOURCE_TRANSACTION_HEADER_ID,
		MASS_REFERENCE_ID,
		LAST_UPDATE_LOGIN,
		TRANSACTION_SUBTYPE,
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
		ATTRIBUTE_CATEGORY_CODE,
		TRANSACTION_KEY,
		AMORTIZATION_START_DATE,
		CALLING_INTERFACE,
		MASS_TRANSACTION_ID,
		MEMBER_TRANSACTION_HEADER_ID,
		TRX_REFERENCE_ID,
		EVENT_ID,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
		TRANSACTION_HEADER_ID,
		BOOK_TYPE_CODE,
		ASSET_ID,
		TRANSACTION_TYPE_CODE,
		TRANSACTION_DATE_ENTERED,
		DATE_EFFECTIVE,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		TRANSACTION_NAME,
		INVOICE_TRANSACTION_ID,
		SOURCE_TRANSACTION_HEADER_ID,
		MASS_REFERENCE_ID,
		LAST_UPDATE_LOGIN,
		TRANSACTION_SUBTYPE,
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
		ATTRIBUTE_CATEGORY_CODE,
		TRANSACTION_KEY,
		AMORTIZATION_START_DATE,
		CALLING_INTERFACE,
		MASS_TRANSACTION_ID,
		MEMBER_TRANSACTION_HEADER_ID,
		TRX_REFERENCE_ID,
		EVENT_ID,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.fa_transaction_headers
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(TRANSACTION_HEADER_ID,0),kca_seq_id) in 
	(select nvl(TRANSACTION_HEADER_ID,0) as  TRANSACTION_HEADER_ID,max(kca_seq_id) from bec_ods_stg.fa_transaction_headers 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(TRANSACTION_HEADER_ID,0))
);

commit;

-- Soft delete
update bec_ods.fa_transaction_headers set IS_DELETED_FLG = 'N';
commit;
update bec_ods.fa_transaction_headers set IS_DELETED_FLG = 'Y'
where (TRANSACTION_HEADER_ID)  in
(
select TRANSACTION_HEADER_ID from bec_raw_dl_ext.fa_transaction_headers
where (TRANSACTION_HEADER_ID,KCA_SEQ_ID)
in 
(
select TRANSACTION_HEADER_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.fa_transaction_headers
group by TRANSACTION_HEADER_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'fa_transaction_headers';

commit;