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
	table bec_ods_stg.fa_category_books;

insert
	into
	bec_ods_stg.fa_category_books
(	CATEGORY_ID,
	BOOK_TYPE_CODE,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	CREATED_BY,
	CREATION_DATE,
	LAST_UPDATE_LOGIN,
	ASSET_CLEARING_ACCT,
	ASSET_COST_ACCT,
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
	CIP_CLEARING_ACCT,
	CIP_COST_ACCT,
	DEPRN_EXPENSE_ACCT,
	DEPRN_RESERVE_ACCT,
	REVAL_AMORTIZATION_ACCT,
	REVAL_RESERVE_ACCT,
	ASSET_COST_ACCOUNT_CCID,
	ASSET_CLEARING_ACCOUNT_CCID,
	WIP_COST_ACCOUNT_CCID,
	WIP_CLEARING_ACCOUNT_CCID,
	RESERVE_ACCOUNT_CCID,
	REVAL_AMORT_ACCOUNT_CCID,
	REVAL_RESERVE_ACCOUNT_CCID,
	LIFE_EXTENSION_CEILING,
	LIFE_EXTENSION_FACTOR,
	PERCENT_SALVAGE_VALUE,
	GLOBAL_ATTRIBUTE1,
	GLOBAL_ATTRIBUTE2,
	GLOBAL_ATTRIBUTE3,
	GLOBAL_ATTRIBUTE4,
	GLOBAL_ATTRIBUTE5,
	GLOBAL_ATTRIBUTE6,
	GLOBAL_ATTRIBUTE7,
	GLOBAL_ATTRIBUTE8,
	GLOBAL_ATTRIBUTE9,
	GLOBAL_ATTRIBUTE10,
	GLOBAL_ATTRIBUTE11,
	GLOBAL_ATTRIBUTE12,
	GLOBAL_ATTRIBUTE13,
	GLOBAL_ATTRIBUTE14,
	GLOBAL_ATTRIBUTE15,
	GLOBAL_ATTRIBUTE16,
	GLOBAL_ATTRIBUTE17,
	GLOBAL_ATTRIBUTE18,
	GLOBAL_ATTRIBUTE19,
	GLOBAL_ATTRIBUTE20,
	GLOBAL_ATTRIBUTE_CATEGORY,
	DEFAULT_GROUP_ASSET_ID,
	BONUS_DEPRN_EXPENSE_ACCT,
	BONUS_DEPRN_RESERVE_ACCT,
	BONUS_RESERVE_ACCT_CCID,
	UNPLAN_EXPENSE_ACCOUNT_CCID,
	UNPLAN_EXPENSE_ACCT,
	DEPRN_EXPENSE_ACCOUNT_CCID,
	BONUS_EXPENSE_ACCOUNT_CCID,
	ALT_COST_ACCOUNT_CCID,
	ALT_COST_ACCT,
	WRITE_OFF_ACCOUNT_CCID,
	WRITE_OFF_ACCT,
	IMPAIR_EXPENSE_ACCOUNT_CCID,
	IMPAIR_EXPENSE_ACCT,
	IMPAIR_RESERVE_ACCOUNT_CCID,
	IMPAIR_RESERVE_ACCT,
	CAPITAL_ADJ_ACCT,
	CAPITAL_ADJ_ACCOUNT_CCID,
	GENERAL_FUND_ACCT,
	GENERAL_FUND_ACCOUNT_CCID,
	REVAL_LOSS_ACCT,
	REVAL_LOSS_ACCOUNT_CCID,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
)
(
	select
		CATEGORY_ID,
		BOOK_TYPE_CODE,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		CREATED_BY,
		CREATION_DATE,
		LAST_UPDATE_LOGIN,
		ASSET_CLEARING_ACCT,
		ASSET_COST_ACCT,
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
		CIP_CLEARING_ACCT,
		CIP_COST_ACCT,
		DEPRN_EXPENSE_ACCT,
		DEPRN_RESERVE_ACCT,
		REVAL_AMORTIZATION_ACCT,
		REVAL_RESERVE_ACCT,
		ASSET_COST_ACCOUNT_CCID,
		ASSET_CLEARING_ACCOUNT_CCID,
		WIP_COST_ACCOUNT_CCID,
		WIP_CLEARING_ACCOUNT_CCID,
		RESERVE_ACCOUNT_CCID,
		REVAL_AMORT_ACCOUNT_CCID,
		REVAL_RESERVE_ACCOUNT_CCID,
		LIFE_EXTENSION_CEILING,
		LIFE_EXTENSION_FACTOR,
		PERCENT_SALVAGE_VALUE,
		GLOBAL_ATTRIBUTE1,
		GLOBAL_ATTRIBUTE2,
		GLOBAL_ATTRIBUTE3,
		GLOBAL_ATTRIBUTE4,
		GLOBAL_ATTRIBUTE5,
		GLOBAL_ATTRIBUTE6,
		GLOBAL_ATTRIBUTE7,
		GLOBAL_ATTRIBUTE8,
		GLOBAL_ATTRIBUTE9,
		GLOBAL_ATTRIBUTE10,
		GLOBAL_ATTRIBUTE11,
		GLOBAL_ATTRIBUTE12,
		GLOBAL_ATTRIBUTE13,
		GLOBAL_ATTRIBUTE14,
		GLOBAL_ATTRIBUTE15,
		GLOBAL_ATTRIBUTE16,
		GLOBAL_ATTRIBUTE17,
		GLOBAL_ATTRIBUTE18,
		GLOBAL_ATTRIBUTE19,
		GLOBAL_ATTRIBUTE20,
		GLOBAL_ATTRIBUTE_CATEGORY,
		DEFAULT_GROUP_ASSET_ID,
		BONUS_DEPRN_EXPENSE_ACCT,
		BONUS_DEPRN_RESERVE_ACCT,
		BONUS_RESERVE_ACCT_CCID,
		UNPLAN_EXPENSE_ACCOUNT_CCID,
		UNPLAN_EXPENSE_ACCT,
		DEPRN_EXPENSE_ACCOUNT_CCID,
		BONUS_EXPENSE_ACCOUNT_CCID,
		ALT_COST_ACCOUNT_CCID,
		ALT_COST_ACCT,
		WRITE_OFF_ACCOUNT_CCID,
		WRITE_OFF_ACCT,
		IMPAIR_EXPENSE_ACCOUNT_CCID,
		IMPAIR_EXPENSE_ACCT,
		IMPAIR_RESERVE_ACCOUNT_CCID,
		IMPAIR_RESERVE_ACCT,
		CAPITAL_ADJ_ACCT,
		CAPITAL_ADJ_ACCOUNT_CCID,
		GENERAL_FUND_ACCT,
		GENERAL_FUND_ACCOUNT_CCID,
		REVAL_LOSS_ACCT,
		REVAL_LOSS_ACCOUNT_CCID,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.fa_category_books
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (nvl(CATEGORY_ID,0), nvl(BOOK_TYPE_CODE,'NA'),
		kca_seq_id) in 
(
		select
			nvl(CATEGORY_ID,0) as CATEGORY_ID,nvl(BOOK_TYPE_CODE,'NA') as BOOK_TYPE_CODE,
			max(kca_seq_id)
		from
			bec_raw_dl_ext.fa_category_books
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(CATEGORY_ID,0), nvl(BOOK_TYPE_CODE,'NA'))
		and 
kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'fa_category_books')
);
end;