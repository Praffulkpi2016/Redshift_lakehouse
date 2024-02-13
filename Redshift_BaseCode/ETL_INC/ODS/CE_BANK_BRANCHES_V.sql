/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for ODS.
# File Version: KPI v1.0
*/

begin;

truncate table bec_ods.CE_BANK_BRANCHES_V;

INSERT INTO bec_ods.CE_BANK_BRANCHES_V (
 BANK_HOME_COUNTRY
,      BANK_PARTY_ID
,      BANK_NAME
,      BANK_NAME_ALT
,      SHORT_BANK_NAME
,      BANK_NUMBER
,      BRANCH_PARTY_ID
,      BANK_BRANCH_NAME
,      BANK_BRANCH_NAME_ALT
,      BRANCH_NUMBER
,      START_DATE
,      END_DATE
,      ADDRESS_LINE1
,      ADDRESS_LINE2
,      ADDRESS_LINE3
,      ADDRESS_LINE4
,      CITY
,      STATE
,      PROVINCE
,      ZIP
,      COUNTRY
,      BANK_INSTITUTION_TYPE
,      BANK_BRANCH_TYPE
,      DESCRIPTION
,      EFT_SWIFT_CODE
,      EFT_USER_NUMBER
,      EDI_ID_NUMBER
,      PK_ID
	,kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
kca_seq_date
)
    SELECT
  BANK_HOME_COUNTRY
,      BANK_PARTY_ID
,      BANK_NAME
,      BANK_NAME_ALT
,      SHORT_BANK_NAME
,      BANK_NUMBER
,      BRANCH_PARTY_ID
,      BANK_BRANCH_NAME
,      BANK_BRANCH_NAME_ALT
,      BRANCH_NUMBER
,      START_DATE
,      END_DATE
,      ADDRESS_LINE1
,      ADDRESS_LINE2
,      ADDRESS_LINE3
,      ADDRESS_LINE4
,      CITY
,      STATE
,      PROVINCE
,      ZIP
,      COUNTRY
,      BANK_INSTITUTION_TYPE
,      BANK_BRANCH_TYPE
,      DESCRIPTION
,      EFT_SWIFT_CODE
,      EFT_USER_NUMBER
,      EDI_ID_NUMBER
,      PK_ID
   , KCA_OPERATION,
    'N' as IS_DELETED_FLG,
    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
kca_seq_date
    FROM
        bec_ods_stg.CE_BANK_BRANCHES_V;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ce_bank_branches_v';
	
COMMIT;