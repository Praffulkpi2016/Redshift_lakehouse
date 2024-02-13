/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach to ODS.
# File Version: KPI v1.0
*/


begin;

DROP TABLE if exists bec_ods.OE_LINES_IFACE_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.OE_LINES_IFACE_ALL
(
	 REQUEST_ID NUMERIC(15,0)   ENCODE az64
    ,ORIG_SHIP_ADDRESS_REF  VARCHAR(50)   ENCODE lzo
    ,ORIG_BILL_ADDRESS_REF  VARCHAR(240)   ENCODE lzo
    ,ORIG_DELIVER_ADDRESS_REF VARCHAR(240)   ENCODE lzo
    ,SHIP_TO_CONTACT_REF VARCHAR(50)   ENCODE lzo
    ,BILL_TO_CONTACT_REF VARCHAR(50)   ENCODE lzo
    ,DELIVER_TO_CONTACT_REF VARCHAR(50)   ENCODE lzo
    ,ORDER_SOURCE_ID NUMERIC(15,0)   ENCODE az64
    ,ORIG_SYS_DOCUMENT_REF VARCHAR(50)   ENCODE lzo
    ,ORIG_SYS_LINE_REF VARCHAR(50)   ENCODE lzo
    ,ORIG_SYS_SHIPMENT_REF VARCHAR(50)   ENCODE lzo
    ,SOLD_TO_ORG_ID NUMERIC(15,0)   ENCODE az64
    ,SOLD_TO_ORG VARCHAR(360)   ENCODE lzo
    ,CHANGE_SEQUENCE VARCHAR(50)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.OE_LINES_IFACE_ALL (
	 REQUEST_ID
    ,ORIG_SHIP_ADDRESS_REF
    ,ORIG_BILL_ADDRESS_REF
    ,ORIG_DELIVER_ADDRESS_REF
    ,SHIP_TO_CONTACT_REF
    ,BILL_TO_CONTACT_REF
    ,DELIVER_TO_CONTACT_REF
    ,ORDER_SOURCE_ID
    ,ORIG_SYS_DOCUMENT_REF
    ,ORIG_SYS_LINE_REF
    ,ORIG_SYS_SHIPMENT_REF
    ,SOLD_TO_ORG_ID
    ,SOLD_TO_ORG
    ,CHANGE_SEQUENCE
	,kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
    SELECT
     REQUEST_ID
    ,ORIG_SHIP_ADDRESS_REF
    ,ORIG_BILL_ADDRESS_REF
    ,ORIG_DELIVER_ADDRESS_REF
    ,SHIP_TO_CONTACT_REF
    ,BILL_TO_CONTACT_REF
    ,DELIVER_TO_CONTACT_REF
    ,ORDER_SOURCE_ID
    ,ORIG_SYS_DOCUMENT_REF
    ,ORIG_SYS_LINE_REF
    ,ORIG_SYS_SHIPMENT_REF
    ,SOLD_TO_ORG_ID
    ,SOLD_TO_ORG
    ,CHANGE_SEQUENCE
	,kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.OE_LINES_IFACE_ALL;

end;


UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'oe_lines_iface_all';
	
commit;