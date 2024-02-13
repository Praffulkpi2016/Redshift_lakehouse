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

truncate table bec_ods_stg.fa_distribution_history;

insert into	bec_ods_stg.fa_distribution_history
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
		kca_seq_id,
		kca_seq_date
	from bec_raw_dl_ext.fa_distribution_history
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (nvl(DISTRIBUTION_ID,0),kca_seq_id) in 
	(select nvl(DISTRIBUTION_ID,0) as DISTRIBUTION_ID,max(kca_seq_id) from bec_raw_dl_ext.fa_distribution_history 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by nvl(DISTRIBUTION_ID,0))
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'fa_distribution_history')
);
end;