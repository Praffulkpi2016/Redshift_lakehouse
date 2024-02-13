/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/
begin;

DROP TABLE if exists bec_dwh.dim_abc_classes;

create table bec_dwh.dim_abc_classes 
	DISTKEY (ASSIGNMENT_GROUP_ID)
	SORTKEY (ASSIGNMENT_GROUP_ID, ABC_CLASS_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID)
as
(
SELECT
    maag.assignment_group_id,
    maa.inventory_item_id,
    maag.organization_id,
    mac.abc_class_id,
    mac.abc_class_name,
    mac.description,
    mac.disable_date,
    maag.assignment_group_name,
    maag.compile_id,
    maag.secondary_inventory,
    maag.item_scope_type,
    maag.classification_method_type,
	--audit columns
    'N' is_deleted_flg,
    (SELECT system_id FROM bec_etl_ctrl.etlsourceappid WHERE source_system = 'EBS') source_app_id,
    (SELECT system_id FROM bec_etl_ctrl.etlsourceappid WHERE source_system = 'EBS') 
        || '-' || NVL(maag.assignment_group_id, 0)
        || '-' || NVL(maa.abc_class_id, 0)
        || '-' || NVL(maa.inventory_item_id, 0)
        || '-' || NVL(maag.organization_id, 0) dw_load_id,
    GETDATE() dw_insert_date,
    GETDATE() dw_update_date
FROM
        bec_ods.mtl_abc_assignment_groups maag,
        bec_ods.mtl_abc_assignments       maa,
        bec_ods.mtl_abc_classes           mac
    WHERE
            1 = 1
        AND maag.assignment_group_id = maa.assignment_group_id
            AND maa.abc_class_id = mac.abc_class_id
);

end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
        dw_table_name = 'dim_abc_classes'
    AND batch_name = 'inv';

COMMIT;