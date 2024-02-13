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

truncate table bec_ods.MTL_ONHAND_LOCATOR_V;

INSERT INTO bec_ods.MTL_ONHAND_LOCATOR_V (
	organization_id,
	inventory_item_id,
	padded_concatenated_segments,
	revision,
	total_qoh,
	subinventory_code,
	locator_id,
	item_description,
	primary_uom_code,
	organization_code,
	organization_name,
	net,
	rsv,
	atp,
	locator_type,
	item_lot_control,
	item_locator_control,
	item_serial_control,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
    SELECT
    organization_id,
	inventory_item_id,
	padded_concatenated_segments,
	revision,
	total_qoh,
	subinventory_code,
	locator_id,
	item_description,
	primary_uom_code,
	organization_code,
	organization_name,
	net,
	rsv,
	atp,
	locator_type,
	item_lot_control,
	item_locator_control,
	item_serial_control,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.MTL_ONHAND_LOCATOR_V;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'mtl_onhand_locator_v';