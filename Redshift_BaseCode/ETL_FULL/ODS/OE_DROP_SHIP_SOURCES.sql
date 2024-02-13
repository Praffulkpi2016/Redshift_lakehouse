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
DROP TABLE if exists bec_ods.OE_DROP_SHIP_SOURCES;
CREATE TABLE IF NOT EXISTS bec_ods.OE_DROP_SHIP_SOURCES
(
	drop_ship_source_id NUMERIC(15,0)   ENCODE az64
	,header_id NUMERIC(15,0)   ENCODE az64
	,line_id NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)  ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,org_id NUMERIC(15,0)   ENCODE az64
	,destination_organization_id NUMERIC(15,0)   ENCODE az64
	,requisition_header_id NUMERIC(15,0)   ENCODE az64
	,requisition_line_id NUMERIC(15,0)   ENCODE az64
	,po_header_id NUMERIC(15,0)   ENCODE az64
	,po_line_id NUMERIC(15,0)   ENCODE az64
	,line_location_id NUMERIC(15,0)   ENCODE az64
	,po_release_id NUMERIC(15,0)   ENCODE az64
	,inst_id NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64	
)
DISTSTYLE
auto;

INSERT INTO bec_ods.OE_DROP_SHIP_SOURCES (
    drop_ship_source_id,
	header_id,
	line_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	org_id,
	destination_organization_id,
	requisition_header_id,
	requisition_line_id,
	po_header_id,
	po_line_id,
	line_location_id,
	po_release_id,
	inst_id,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    SELECT
       drop_ship_source_id,
	header_id,
	line_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	org_id,
	destination_organization_id,
	requisition_header_id,
	requisition_line_id,
	po_header_id,
	po_line_id,
	line_location_id,
	po_release_id,
	inst_id,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
    FROM
        bec_ods_stg.OE_DROP_SHIP_SOURCES;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'oe_drop_ship_sources';
	
commit;