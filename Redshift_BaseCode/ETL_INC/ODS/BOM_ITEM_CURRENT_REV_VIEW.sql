/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach to ODS.
# File Version: KPI v1.0
*/
begin;

TRUNCATE TABLE bec_ods.bom_item_current_rev_view;

INSERT INTO bec_ods.bom_item_current_rev_view (
	ORGANIZATION_ID,
	INVENTORY_ITEM_ID,
	CURRENT_REVISION,
	EFFECTIVITY_DATE,
	REVISION_LABEL,
	REVISION_ID,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
	ORGANIZATION_ID,
	INVENTORY_ITEM_ID,
	CURRENT_REVISION,
	EFFECTIVITY_DATE,
	REVISION_LABEL,
	REVISION_ID,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.bom_item_current_rev_view;

end;


UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'bom_item_current_rev_view';
commit;