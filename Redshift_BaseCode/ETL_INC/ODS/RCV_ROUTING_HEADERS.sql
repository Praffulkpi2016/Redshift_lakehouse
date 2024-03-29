/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/

begin;


TRUNCATE TABLE bec_ods.RCV_ROUTING_HEADERS; 

INSERT INTO bec_ods.RCV_ROUTING_HEADERS (
    routing_header_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_updated_login,
	routing_name,
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
	description,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    SELECT
    routing_header_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_updated_login,
	routing_name,
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
	description,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
    FROM
        bec_ods_stg.RCV_ROUTING_HEADERS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'rcv_routing_headers';