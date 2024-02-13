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

delete from bec_dwh.dim_ap_payment_banks
where (nvl(bank_account_id,0),nvl(party_id,0)) in (
select nvl(ods.bank_account_id,0),nvl(ods.party_id,0) from bec_dwh.dim_ap_payment_banks dw,
 (select 
	trunc(aba.bank_account_id) as bank_account_id,bv.party_id
	from bec_ods.ce_bank_accounts aba,
 bec_ods.hz_parties bv
where
	aba.bank_id = bv.party_id
	and (aba.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ap_payment_banks' and batch_name = 'ap')
	or bv.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ap_payment_banks' and batch_name = 'ap')
	 )
	) ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.bank_account_id,0) ||'-'||nvl(ods.party_id,0)
);

commit;

-- Insert records

insert into bec_dwh.DIM_AP_PAYMENT_BANKS
(
bank_account_id
,party_id
,bank_account_name
,bank_account_num
,currency_code
,bank_acc_desc
,bank_acc_contact
,contact_phone
,bank_account_type
,account_type
,bank_branch_name
,bank_name
,description
,is_deleted_flg
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
)
(
SELECT
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
    || '-' || nvl(aba.bank_account_id, 0)
    || '-' || nvl(bv.party_id, 0) AS dw_load_id,
    getdate()              AS dw_insert_date,
    getdate()              AS dw_update_date
FROM
    bec_ods.ce_bank_accounts aba,
    bec_ods.hz_parties       bv
WHERE
    aba.bank_id = bv.party_id
	and (aba.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ap_payment_banks' and batch_name = 'ap')
	or bv.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ap_payment_banks' and batch_name = 'ap')
	 )
);

-- Soft delete

update bec_dwh.dim_ap_payment_banks set is_deleted_flg = 'Y'
where (nvl(bank_account_id,0),nvl(party_id,0)) not in (
select nvl(ods.bank_account_id,0),nvl(ods.party_id,0) from bec_dwh.dim_ap_payment_banks dw, 
(select 
	trunc(aba.bank_account_id) as bank_account_id,bv.party_id
	from (select * from bec_ods.ce_bank_accounts where is_deleted_flg <> 'Y') aba,
(select * from bec_ods.hz_parties where is_deleted_flg <> 'Y') bv
where
	aba.bank_id = bv.party_id
	) ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.bank_account_id,0) ||'-'||nvl(ods.party_id,0) 
);

commit;

END;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ap_payment_banks' and batch_name = 'ap';

COMMIT;