/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;

-- Delete Records

delete from bec_dwh.DIM_AR_BANK_ACC
where (NVL(CASH_RECEIPT_ID,0)) in (
select NVL(ods.CASH_RECEIPT_ID,0)  from bec_dwh.DIM_AR_BANK_ACC dw,
 (SELECT acra.CASH_RECEIPT_ID,
    acra.last_update_date
  FROM BEC_ODS.AR_CASH_RECEIPTS_ALL            acra,
       BEC_ODS.CE_BANK_ACCOUNTS                CBA,
       BEC_ODS.CE_BANK_ACCT_USES_ALL           REMIT_BANK
WHERE   1=1
       AND acra.REMIT_BANK_ACCT_USE_ID = REMIT_BANK.BANK_ACCT_USE_ID(+)
       AND acra.ORG_ID = REMIT_BANK.ORG_ID(+)
       AND remit_bank.bank_account_id = CBA.bank_account_id(+)
       AND acra.TYPE <> 'MISC'
	   and (acra.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ar_bank_acc' and batch_name = 'ar')
 )
	) ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.CASH_RECEIPT_ID,0)		 
);

commit;

-- Insert records

insert into bec_dwh.DIM_AR_BANK_ACC
(
	cash_receipt_id,
	org_id,
	receipt_number,
	doc_sequence_value,
	receipt_date,
	comments,
	masked_account_num
	,is_deleted_flg
	,source_app_id
	,dw_load_id
	,dw_insert_date
	,dw_update_date
)
(
SELECT acra.CASH_RECEIPT_ID,
       acra.ORG_ID,
       acra.RECEIPT_NUMBER,
       acra.DOC_SEQUENCE_VALUE,
       ACRA.RECEIPT_DATE,
       ACRA.COMMENTS,
       CBA.MASKED_ACCOUNT_NUM,
	     'N' as is_deleted_flg,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )                   AS source_app_id,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-' || nvl(acra.CASH_RECEIPT_ID, 0) AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
  FROM BEC_ODS.AR_CASH_RECEIPTS_ALL            acra,
       BEC_ODS.CE_BANK_ACCOUNTS                CBA,
       BEC_ODS.CE_BANK_ACCT_USES_ALL           REMIT_BANK
WHERE   1=1
       AND acra.REMIT_BANK_ACCT_USE_ID = REMIT_BANK.BANK_ACCT_USE_ID(+)
       AND acra.ORG_ID = REMIT_BANK.ORG_ID(+)
       AND remit_bank.bank_account_id = CBA.bank_account_id(+)
       AND acra.TYPE <> 'MISC'
	   and (acra.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ar_bank_acc' and batch_name = 'ar')
 )
	   );

-- Soft delete

update bec_dwh.DIM_AR_BANK_ACC set is_deleted_flg = 'Y'
where (NVL(CASH_RECEIPT_ID,0)) not in (
select NVL(ods.CASH_RECEIPT_ID,0) from bec_dwh.DIM_AR_BANK_ACC dw,
 (SELECT acra.CASH_RECEIPT_ID,
    acra.last_update_date,
	acra.kca_operation
  FROM (select * from bec_ods.AR_CASH_RECEIPTS_ALL  where is_deleted_flg <> 'Y')         acra,
       (select * from bec_ods.CE_BANK_ACCOUNTS      where is_deleted_flg <> 'Y')         CBA,
       (select * from bec_ods.CE_BANK_ACCT_USES_ALL where is_deleted_flg <> 'Y')         REMIT_BANK
WHERE   1=1
       AND acra.REMIT_BANK_ACCT_USE_ID = REMIT_BANK.BANK_ACCT_USE_ID(+)
       AND acra.ORG_ID = REMIT_BANK.ORG_ID(+)
       AND remit_bank.bank_account_id = CBA.bank_account_id(+)
       AND acra.TYPE <> 'MISC'
	) ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.CASH_RECEIPT_ID,0)
);

commit;

end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'dim_ar_bank_acc'
	and batch_name = 'ar';

COMMIT;