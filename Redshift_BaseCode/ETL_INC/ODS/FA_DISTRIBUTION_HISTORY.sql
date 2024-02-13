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

delete from bec_ods.fa_distribution_history
where (nvl(DISTRIBUTION_ID,0))  in (
select nvl(stg.DISTRIBUTION_ID,0) as DISTRIBUTION_ID from bec_ods.fa_distribution_history ods, bec_ods_stg.fa_distribution_history stg
where nvl(ods.DISTRIBUTION_ID,0) = nvl(stg.DISTRIBUTION_ID,0)
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.fa_distribution_history
       (DISTRIBUTION_ID,
		BOOK_TYPE_CODE,
		ASSET_ID,
		UNITS_ASSIGNED,
		DATE_EFFECTIVE,
		CODE_COMBINATION_ID,
		LOCATION_ID,
		TRANSACTION_HEADER_ID_IN,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		DATE_INEFFECTIVE,
		ASSIGNED_TO,
		TRANSACTION_HEADER_ID_OUT,
		TRANSACTION_UNITS,
		RETIREMENT_ID,
		LAST_UPDATE_LOGIN,
		CAPITAL_ADJ_ACCOUNT_CCID,
		GENERAL_FUND_ACCOUNT_CCID,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
		DISTRIBUTION_ID,
		BOOK_TYPE_CODE,
		ASSET_ID,
		UNITS_ASSIGNED,
		DATE_EFFECTIVE,
		CODE_COMBINATION_ID,
		LOCATION_ID,
		TRANSACTION_HEADER_ID_IN,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		DATE_INEFFECTIVE,
		ASSIGNED_TO,
		TRANSACTION_HEADER_ID_OUT,
		TRANSACTION_UNITS,
		RETIREMENT_ID,
		LAST_UPDATE_LOGIN,
		CAPITAL_ADJ_ACCOUNT_CCID,
		GENERAL_FUND_ACCOUNT_CCID,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.fa_distribution_history
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(DISTRIBUTION_ID,0),kca_seq_id) in 
	(select nvl(DISTRIBUTION_ID,0) as DISTRIBUTION_ID,max(kca_seq_id) from bec_ods_stg.fa_distribution_history 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(DISTRIBUTION_ID,0))
);

commit;

-- Soft delete
update bec_ods.fa_distribution_history set IS_DELETED_FLG = 'N';
commit;
update bec_ods.fa_distribution_history set IS_DELETED_FLG = 'Y'
where (DISTRIBUTION_ID)  in
(
select DISTRIBUTION_ID from bec_raw_dl_ext.fa_distribution_history
where (DISTRIBUTION_ID,KCA_SEQ_ID)
in 
(
select DISTRIBUTION_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.fa_distribution_history
group by DISTRIBUTION_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'fa_distribution_history';

commit;