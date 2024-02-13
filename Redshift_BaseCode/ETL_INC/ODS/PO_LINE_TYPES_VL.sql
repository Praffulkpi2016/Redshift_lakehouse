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

TRUNCATE TABLE bec_ods.PO_LINE_TYPES_VL; 
INSERT INTO bec_ods.PO_LINE_TYPES_VL (
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
	outside_operation_flag,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	receive_close_tolerance,
	line_type_id,
	last_update_date,
	last_updated_by,
	order_type_lookup_code,
	last_update_login,
	creation_date,
	created_by,
	category_id,
	unit_of_measure,
	unit_price,
	receiving_flag,
	inactive_date,
	attribute_category,
	attribute1,
	attribute2,
	line_type,
	description,
	purchase_basis,
	matching_basis,
	CLM_SEVERABLE_FLAG,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
    SELECT
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
	outside_operation_flag,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	receive_close_tolerance,
	line_type_id,
	last_update_date,
	last_updated_by,
	order_type_lookup_code,
	last_update_login,
	creation_date,
	created_by,
	category_id,
	unit_of_measure,
	unit_price,
	receiving_flag,
	inactive_date,
	attribute_category,
	attribute1,
	attribute2,
	line_type,
	description,
	purchase_basis,
	matching_basis,
	CLM_SEVERABLE_FLAG,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	KCA_SEQ_DATE
    FROM
        bec_ods_stg.PO_LINE_TYPES_VL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'po_line_types_vl';
	
COMMIT;
	
