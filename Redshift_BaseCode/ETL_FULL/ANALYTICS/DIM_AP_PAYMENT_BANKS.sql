/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;
drop table if exists bec_dwh.DIM_AP_PAYMENT_BANKS;

CREATE TABLE bec_dwh.DIM_AP_PAYMENT_BANKS 
diststyle all 
sortkey(PARTY_ID,BANK_ACCOUNT_ID)
AS
( SELECT
    aba.bank_account_id,
	bv.party_id,
    aba.bank_account_name,
    aba.bank_account_num,
    aba.currency_code,
    aba.description        AS bank_acc_desc,
    'BANK CONTACT'         AS bank_acc_contact,
    'BANK PHONE'           AS contact_phone,
    aba.bank_account_type,
    'ACCT TYPE'            AS account_type,
    'BRANCH'               AS bank_branch_name,
    bv.party_name          AS bank_name,
    'BRANCH DESC'          AS description,
	'N' as is_deleted_flg,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )                      AS source_app_id,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-'
       || nvl(aba.bank_account_id, 0)
          || '-'
             || nvl(bv.party_id, 0) AS dw_load_id,
    getdate()              AS dw_insert_date,
    getdate()              AS dw_update_date
FROM
    bec_ods.ce_bank_accounts aba,
    bec_ods.hz_parties       bv
WHERE
    aba.bank_id = bv.party_id
);

end;



UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ap_payment_banks' and batch_name ='ap';

COMMIT;
