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
--delete
WITH ValidAccounts AS (
    SELECT
        TRUNC(aba.bank_account_id) AS bank_account_id,
        bv.party_id AS party_id
    FROM
        bec_ods.ce_bank_accounts aba
    JOIN
        bec_ods.hz_parties bv ON aba.bank_id = bv.party_id
    WHERE
        bv.kca_seq_date > (SELECT (executebegints - prune_days) FROM bec_etl_ctrl.batch_dw_info
                           WHERE dw_table_name = 'dim_ap_bank_accounts' AND batch_name = 'ap')
    UNION
select
	bank_ac.ext_bank_account_id bank_account_id,
	hps.party_id as party_id
from 
	bec_ods.hz_party_sites hps,
	bec_ods.iby_external_payees_all ib_xt_payee,
	bec_ods.iby_pmt_instr_uses_all ib_pmt_instr,
	bec_ods.iby_ext_bank_accounts bank_ac,
	bec_ods.ce_bank_branches_v bank_branch
where
	1 = 1
	and ib_xt_payee.payee_party_id = hps.party_id
	and ib_pmt_instr.ext_pmt_party_id = ib_xt_payee.ext_payee_id
	and ib_pmt_instr.instrument_type = 'BANKACCOUNT'
	and ib_pmt_instr.instrument_id = bank_ac.ext_bank_account_id
	and bank_ac.branch_id = bank_branch.branch_party_id(+)
	and ib_pmt_instr.END_DATE is null
	and (bank_ac.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info
	where dw_table_name ='dim_ap_bank_accounts' and batch_name = 'ap')
or hps.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info
	where dw_table_name ='dim_ap_bank_accounts' and batch_name = 'ap'))
)
DELETE FROM bec_dwh.DIM_AP_BANK_ACCOUNTS
USING ValidAccounts ods
WHERE DIM_AP_BANK_ACCOUNTS.dw_load_id = (SELECT system_id FROM bec_etl_ctrl.etlsourceappid WHERE source_system = 'EBS')
                       || '-' || NVL(ods.bank_account_id, 0) || '-' || NVL(ods.party_id, 0);

commit;

--INSERT
insert into bec_dwh.DIM_AP_BANK_ACCOUNTS
(
select
bank_account_id,
party_id,
bank_account_name,
bank_account_num,
currency_code,
bank_acc_desc,
bank_acc_contact,
contact_phone,
bank_account_type,
account_type,
bank_branch_name,
bank_name,
description,
bank_branch_num,
bank_num,
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
       || nvl(bank_account_id,0) ||'-'||nvl(party_id,0) AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
from
(
select
	trunc(aba.bank_account_id) as bank_account_id,
	bv.party_id as party_id,
	aba.bank_account_name as bank_account_name,
	aba.bank_account_num as bank_account_num,
	aba.currency_code as currency_code,
	aba.description as bank_acc_desc,
	'BANK CONTACT' as bank_acc_contact,
	'BANK PHONE' as contact_phone,
	aba.bank_account_type as bank_account_type,
	'ACCT TYPE' as account_type,
	'BRANCH' as bank_branch_name,
		bv.party_name as bank_name,
	'BRANCH DESC' as description,
	'BRANCH NUM' as bank_branch_num,
	'BANK BRANCH NUM' as bank_num
from
bec_ods.ce_bank_accounts aba,
bec_ods.hz_parties bv
where aba.bank_id = bv.party_id
and (bv.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info
	where dw_table_name ='dim_ap_bank_accounts' and batch_name = 'ap')
or aba.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info
	where dw_table_name ='dim_ap_bank_accounts' and batch_name = 'ap'))
union
select
	bank_ac.ext_bank_account_id bank_account_id,
	hps.party_id as party_id,
	bank_ac.bank_account_name as bank_account_name,
	bank_ac.bank_account_num as bank_account_num,
	bank_ac.currency_code as currency_code,
	bank_ac.description as bank_acc_desc,
	'BANK CONTACT' as bank_acc_contact,
	'BANK PHONE' as contact_phone,
		bank_ac.bank_account_type as bank_account_type,
	'ACCT TYPE' as account_type,
	bank_branch.bank_branch_name,
	bank_branch.BANK_NAME bank_name,
	bank_branch.description,
	bank_branch.branch_number bank_branch_num,
	bank_branch.bank_number bank_num
from 
	bec_ods.hz_party_sites hps,
	bec_ods.iby_external_payees_all ib_xt_payee,
	bec_ods.iby_pmt_instr_uses_all ib_pmt_instr,
	bec_ods.iby_ext_bank_accounts bank_ac,
	bec_ods.ce_bank_branches_v bank_branch
where
	1 = 1
	and ib_xt_payee.payee_party_id = hps.party_id
	and ib_pmt_instr.ext_pmt_party_id = ib_xt_payee.ext_payee_id
	and ib_pmt_instr.instrument_type = 'BANKACCOUNT'
	and ib_pmt_instr.instrument_id = bank_ac.ext_bank_account_id
	and bank_ac.branch_id = bank_branch.branch_party_id(+)
	and ib_pmt_instr.END_DATE is null
	and (bank_ac.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info
	where dw_table_name ='dim_ap_bank_accounts' and batch_name = 'ap')
or hps.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info
	where dw_table_name ='dim_ap_bank_accounts' and batch_name = 'ap'))
));
commit;
--soft delete
WITH ValidAccounts AS (
    SELECT
        TRUNC(aba.bank_account_id) AS bank_account_id,
        bv.party_id AS party_id
    FROM
        bec_ods.ce_bank_accounts aba
    JOIN
        bec_ods.hz_parties bv ON aba.bank_id = bv.party_id
    WHERE aba.is_deleted_flg <> 'Y'
    UNION
select
	bank_ac.ext_bank_account_id bank_account_id,
	hps.party_id as party_id
from 
	bec_ods.hz_party_sites hps,
	bec_ods.iby_external_payees_all ib_xt_payee,
	bec_ods.iby_pmt_instr_uses_all ib_pmt_instr,
	bec_ods.iby_ext_bank_accounts bank_ac,
	bec_ods.ce_bank_branches_v bank_branch
where
	1 = 1 and bank_ac.is_deleted_flg <> 'Y'
	and ib_xt_payee.payee_party_id = hps.party_id
	and ib_pmt_instr.ext_pmt_party_id = ib_xt_payee.ext_payee_id
	and ib_pmt_instr.instrument_type = 'BANKACCOUNT'
	and ib_pmt_instr.instrument_id = bank_ac.ext_bank_account_id
	and bank_ac.branch_id = bank_branch.branch_party_id(+)
	and ib_pmt_instr.END_DATE is null
)
UPDATE 
    bec_dwh.DIM_AP_BANK_ACCOUNTS
SET 
    is_deleted_flg = 'Y'
WHERE 
    NOT EXISTS (
        SELECT 1
from ValidAccounts ods
WHERE DIM_AP_BANK_ACCOUNTS.dw_load_id = (SELECT system_id FROM bec_etl_ctrl.etlsourceappid WHERE source_system = 'EBS')
                       || '-' || NVL(ods.bank_account_id, 0) || '-' || NVL(ods.party_id, 0)
    );
commit;

end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ap_bank_accounts'
and batch_name = 'ap';

COMMIT;