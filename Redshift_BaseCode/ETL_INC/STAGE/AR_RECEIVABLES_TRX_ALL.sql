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

truncate table bec_ods_stg.AR_RECEIVABLES_TRX_ALL;

insert into	bec_ods_stg.AR_RECEIVABLES_TRX_ALL
   (
   ACCOUNTING_AFFECT_FLAG
,ASSET_TAX_CODE
,ATTRIBUTE1
,ATTRIBUTE10
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,ATTRIBUTE9
,ATTRIBUTE_CATEGORY
,CODE_COMBINATION_ID
,CREATED_BY
,CREATION_DATE
,DEFAULT_ACCTG_DISTRIBUTION_SET
,DESCRIPTION
,END_DATE_ACTIVE
,GLOBAL_ATTRIBUTE1
,GLOBAL_ATTRIBUTE10
,GLOBAL_ATTRIBUTE11
,GLOBAL_ATTRIBUTE12
,GLOBAL_ATTRIBUTE13
,GLOBAL_ATTRIBUTE14
,GLOBAL_ATTRIBUTE15
,GLOBAL_ATTRIBUTE16
,GLOBAL_ATTRIBUTE17
,GLOBAL_ATTRIBUTE18
,GLOBAL_ATTRIBUTE19
,GLOBAL_ATTRIBUTE2
,GLOBAL_ATTRIBUTE20
,GLOBAL_ATTRIBUTE3
,GLOBAL_ATTRIBUTE4
,GLOBAL_ATTRIBUTE5
,GLOBAL_ATTRIBUTE6
,GLOBAL_ATTRIBUTE7
,GLOBAL_ATTRIBUTE8
,GLOBAL_ATTRIBUTE9
,GLOBAL_ATTRIBUTE_CATEGORY
,GL_ACCOUNT_SOURCE
,INACTIVE_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_DATE
,LAST_UPDATE_LOGIN
,LIABILITY_TAX_CODE
,NAME
,ORG_ID
,RECEIVABLES_TRX_ID
,RISK_ELIMINATION_DAYS
,SET_OF_BOOKS_ID
,START_DATE_ACTIVE
,STATUS
,TAX_CODE_SOURCE
,TAX_RECOVERABLE_FLAG
,TYPE
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION
,kca_seq_id
,kca_seq_date
	)
(
	select
ACCOUNTING_AFFECT_FLAG
,ASSET_TAX_CODE
,ATTRIBUTE1
,ATTRIBUTE10
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,ATTRIBUTE9
,ATTRIBUTE_CATEGORY
,CODE_COMBINATION_ID
,CREATED_BY
,CREATION_DATE
,DEFAULT_ACCTG_DISTRIBUTION_SET
,DESCRIPTION
,END_DATE_ACTIVE
,GLOBAL_ATTRIBUTE1
,GLOBAL_ATTRIBUTE10
,GLOBAL_ATTRIBUTE11
,GLOBAL_ATTRIBUTE12
,GLOBAL_ATTRIBUTE13
,GLOBAL_ATTRIBUTE14
,GLOBAL_ATTRIBUTE15
,GLOBAL_ATTRIBUTE16
,GLOBAL_ATTRIBUTE17
,GLOBAL_ATTRIBUTE18
,GLOBAL_ATTRIBUTE19
,GLOBAL_ATTRIBUTE2
,GLOBAL_ATTRIBUTE20
,GLOBAL_ATTRIBUTE3
,GLOBAL_ATTRIBUTE4
,GLOBAL_ATTRIBUTE5
,GLOBAL_ATTRIBUTE6
,GLOBAL_ATTRIBUTE7
,GLOBAL_ATTRIBUTE8
,GLOBAL_ATTRIBUTE9
,GLOBAL_ATTRIBUTE_CATEGORY
,GL_ACCOUNT_SOURCE
,INACTIVE_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_DATE
,LAST_UPDATE_LOGIN
,LIABILITY_TAX_CODE
,NAME
,ORG_ID
,RECEIVABLES_TRX_ID
,RISK_ELIMINATION_DAYS
,SET_OF_BOOKS_ID
,START_DATE_ACTIVE
,STATUS
,TAX_CODE_SOURCE
,TAX_RECOVERABLE_FLAG
,TYPE
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION
,kca_seq_id
,kca_seq_date
	from bec_raw_dl_ext.AR_RECEIVABLES_TRX_ALL
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (RECEIVABLES_TRX_ID,org_id,kca_seq_id) in 
	(select RECEIVABLES_TRX_ID,org_id,max(kca_seq_id) from bec_raw_dl_ext.AR_RECEIVABLES_TRX_ALL 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by RECEIVABLES_TRX_ID,org_id)
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'ar_receivables_trx_all')
);
end;