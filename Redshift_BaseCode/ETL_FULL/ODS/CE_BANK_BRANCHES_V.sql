/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS.
# File Version: KPI v1.0
*/

begin;

DROP TABLE if exists bec_ods.CE_BANK_BRANCHES_V;

CREATE TABLE IF NOT EXISTS bec_ods.CE_BANK_BRANCHES_V
(

     bank_home_country VARCHAR(2)   ENCODE lzo
	,bank_party_id NUMERIC(15,0)   ENCODE az64
	,bank_name VARCHAR(360)   ENCODE lzo
	,bank_name_alt VARCHAR(320)   ENCODE lzo
	,short_bank_name VARCHAR(240)   ENCODE lzo
	,bank_number VARCHAR(30)   ENCODE lzo
	,branch_party_id NUMERIC(15,0)   ENCODE az64
	,bank_branch_name VARCHAR(360)   ENCODE lzo
	,bank_branch_name_alt VARCHAR(320)   ENCODE lzo
	,branch_number VARCHAR(30)   ENCODE lzo
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,address_line1 VARCHAR(240)   ENCODE lzo
	,address_line2 VARCHAR(240)   ENCODE lzo
	,address_line3 VARCHAR(240)   ENCODE lzo
	,address_line4 VARCHAR(240)   ENCODE lzo
	,city VARCHAR(60)   ENCODE lzo
	,state VARCHAR(60)   ENCODE lzo
	,province VARCHAR(60)   ENCODE lzo
	,zip VARCHAR(60)   ENCODE lzo
	,country VARCHAR(60)   ENCODE lzo
	,bank_institution_type VARCHAR(30)   ENCODE lzo
	,bank_branch_type VARCHAR(30)   ENCODE lzo
	,description VARCHAR(2000)   ENCODE lzo
	,eft_swift_code VARCHAR(30)   ENCODE lzo
	,eft_user_number VARCHAR(30)   ENCODE lzo
	,edi_id_number VARCHAR(30)   ENCODE lzo
	,pk_id NUMERIC(15,0)   ENCODE az64 
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

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