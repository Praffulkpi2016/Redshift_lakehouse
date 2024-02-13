/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for DIMENSIONS.
# File Version: KPI v1.0
*/

begin;

DROP TABLE IF EXISTS BEC_DWH.DIM_AR_BANK_ACC;

CREATE TABLE BEC_DWH.DIM_AR_BANK_ACC DISTSTYLE ALL SORTKEY(CASH_RECEIPT_ID)
AS
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
    || '-'
       || nvl(acra.CASH_RECEIPT_ID, 0) AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
  FROM BEC_ODS.AR_CASH_RECEIPTS_ALL            acra,
       BEC_ODS.CE_BANK_ACCOUNTS                CBA,
       BEC_ODS.CE_BANK_ACCT_USES_ALL           REMIT_BANK
WHERE   1=1
       AND acra.REMIT_BANK_ACCT_USE_ID = REMIT_BANK.BANK_ACCT_USE_ID(+)
       AND acra.ORG_ID = REMIT_BANK.ORG_ID(+)
       AND remit_bank.bank_account_id = CBA.bank_account_id(+)
       AND acra.TYPE <> 'MISC';
	   
end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ar_bank_acc'  and batch_name = 'ar';

COMMIT;
