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

DROP TABLE if exists bec_ods.ap_bank_branches;

CREATE TABLE IF NOT EXISTS bec_ods.ap_bank_branches
(
	bank_branch_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,bank_name VARCHAR(60)   ENCODE lzo
	,bank_branch_name VARCHAR(60)   ENCODE lzo
	,description VARCHAR(240)   ENCODE lzo
	,address_line1 VARCHAR(35)   ENCODE lzo
	,address_line2 VARCHAR(35)   ENCODE lzo
	,address_line3 VARCHAR(35)   ENCODE lzo
	,city VARCHAR(25)   ENCODE lzo
	,state VARCHAR(25)   ENCODE lzo
	,zip VARCHAR(20)   ENCODE lzo
	,province VARCHAR(25)   ENCODE lzo
	,country VARCHAR(25)   ENCODE lzo
	,area_code VARCHAR(10)   ENCODE lzo
	,phone VARCHAR(15)   ENCODE lzo
	,contact_first_name VARCHAR(15)   ENCODE lzo
	,contact_middle_name VARCHAR(15)   ENCODE lzo
	,contact_last_name VARCHAR(20)   ENCODE lzo
	,contact_prefix VARCHAR(5)   ENCODE lzo
	,contact_title VARCHAR(30)   ENCODE lzo
	,bank_num VARCHAR(25)   ENCODE lzo
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,institution_type VARCHAR(25)   ENCODE lzo
	,clearing_house_id NUMERIC(15,0)   ENCODE az64
	,transmission_program_id NUMERIC(15,0)   ENCODE az64
	,printing_program_id NUMERIC(15,0)   ENCODE az64
	,attribute_category VARCHAR(150)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,attribute6 VARCHAR(150)   ENCODE lzo
	,attribute7 VARCHAR(150)   ENCODE lzo
	,attribute8 VARCHAR(150)   ENCODE lzo
	,attribute9 VARCHAR(150)   ENCODE lzo
	,attribute10 VARCHAR(150)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,address_style VARCHAR(30)   ENCODE lzo
	,bank_number VARCHAR(30)   ENCODE lzo
	,address_line4 VARCHAR(35)   ENCODE lzo
	,county VARCHAR(25)   ENCODE lzo
	,eft_user_number VARCHAR(30)   ENCODE lzo
	,eft_swift_code VARCHAR(12)   ENCODE lzo
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,edi_id_number VARCHAR(30)   ENCODE lzo
	,bank_branch_type VARCHAR(25)   ENCODE lzo
	,global_attribute_category VARCHAR(150)   ENCODE lzo
	,global_attribute1 VARCHAR(150)   ENCODE lzo
	,global_attribute2 VARCHAR(150)   ENCODE lzo
	,global_attribute3 VARCHAR(150)   ENCODE lzo
	,global_attribute4 VARCHAR(150)   ENCODE lzo
	,global_attribute5 VARCHAR(150)   ENCODE lzo
	,global_attribute6 VARCHAR(150)   ENCODE lzo
	,global_attribute7 VARCHAR(150)   ENCODE lzo
	,global_attribute8 VARCHAR(150)   ENCODE lzo
	,global_attribute9 VARCHAR(150)   ENCODE lzo
	,global_attribute10 VARCHAR(150)   ENCODE lzo
	,global_attribute11 VARCHAR(150)   ENCODE lzo
	,global_attribute12 VARCHAR(150)   ENCODE lzo
	,global_attribute13 VARCHAR(150)   ENCODE lzo
	,global_attribute14 VARCHAR(150)   ENCODE lzo
	,global_attribute15 VARCHAR(150)   ENCODE lzo
	,global_attribute16 VARCHAR(150)   ENCODE lzo
	,global_attribute17 VARCHAR(150)   ENCODE lzo
	,global_attribute18 VARCHAR(150)   ENCODE lzo
	,global_attribute19 VARCHAR(150)   ENCODE lzo
	,global_attribute20 VARCHAR(150)   ENCODE lzo
	,bank_name_alt VARCHAR(320)   ENCODE lzo
	,bank_branch_name_alt VARCHAR(320)   ENCODE lzo
	,address_lines_alt VARCHAR(560)   ENCODE lzo
	,active_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,tp_header_id NUMERIC(15,0)   ENCODE az64
	,ece_tp_location_code VARCHAR(60)   ENCODE lzo
	,payroll_bank_account_id NUMERIC(15,0)   ENCODE az64
	,rfc_identifier VARCHAR(15)   ENCODE lzo
	,bank_admin_email VARCHAR(255)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.ap_bank_branches (
    bank_branch_id,
    last_update_date,
    last_updated_by,
    bank_name,
    bank_branch_name,
    description,
    address_line1,
    address_line2,
    address_line3,
    city,
    state,
    zip,
    province,
    country,
    area_code,
    phone,
    contact_first_name,
    contact_middle_name,
    contact_last_name,
    contact_prefix,
    contact_title,
    bank_num,
    last_update_login,
    creation_date,
    created_by,
    institution_type,
    clearing_house_id,
    transmission_program_id,
    printing_program_id,
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    address_style,
    bank_number,
    address_line4,
    county,
    eft_user_number,
    eft_swift_code,
    end_date,
    edi_id_number,
    bank_branch_type,
    global_attribute_category,
    global_attribute1,
    global_attribute2,
    global_attribute3,
    global_attribute4,
    global_attribute5,
    global_attribute6,
    global_attribute7,
    global_attribute8,
    global_attribute9,
    global_attribute10,
    global_attribute11,
    global_attribute12,
    global_attribute13,
    global_attribute14,
    global_attribute15,
    global_attribute16,
    global_attribute17,
    global_attribute18,
    global_attribute19,
    global_attribute20,
    bank_name_alt,
    bank_branch_name_alt,
    address_lines_alt,
    active_date,
    tp_header_id,
    ece_tp_location_code,
    payroll_bank_account_id,
    rfc_identifier,
    bank_admin_email,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
        bank_branch_id,
        last_update_date,
        last_updated_by,
        bank_name,
        bank_branch_name,
        description,
        address_line1,
        address_line2,
        address_line3,
        city,
        state,
        zip,
        province,
        country,
        area_code,
        phone,
        contact_first_name,
        contact_middle_name,
        contact_last_name,
        contact_prefix,
        contact_title,
        bank_num,
        last_update_login,
        creation_date,
        created_by,
        institution_type,
        clearing_house_id,
        transmission_program_id,
        printing_program_id,
        attribute_category,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        address_style,
        bank_number,
        address_line4,
        county,
        eft_user_number,
        eft_swift_code,
        end_date,
        edi_id_number,
        bank_branch_type,
        global_attribute_category,
        global_attribute1,
        global_attribute2,
        global_attribute3,
        global_attribute4,
        global_attribute5,
        global_attribute6,
        global_attribute7,
        global_attribute8,
        global_attribute9,
        global_attribute10,
        global_attribute11,
        global_attribute12,
        global_attribute13,
        global_attribute14,
        global_attribute15,
        global_attribute16,
        global_attribute17,
        global_attribute18,
        global_attribute19,
        global_attribute20,
        bank_name_alt,
        bank_branch_name_alt,
        address_lines_alt,
        active_date,
        tp_header_id,
        ece_tp_location_code,
        payroll_bank_account_id,
        rfc_identifier,
        bank_admin_email,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.ap_bank_branches;


end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ap_bank_branches';
	
commit;