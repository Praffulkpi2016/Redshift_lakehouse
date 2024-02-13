/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;

-- Delete Records

delete from bec_dwh.DIM_PO_RELEASES
where (nvl(PO_RELEASE_ID, 0)) in (
select nvl(ods.PO_RELEASE_ID, 0) as PO_RELEASE_ID from bec_dwh.DIM_PO_RELEASES dw, bec_ods.PO_RELEASES_ALL ods
where dw.dw_load_id = 
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    ||'-'|| nvl(ods.PO_RELEASE_ID, 0)
and (ods.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_po_releases' and batch_name = 'ap')
 )
);

commit;

-- Insert records

insert into bec_dwh.DIM_PO_RELEASES
(
    PO_RELEASE_ID,
    PO_HEADER_ID,
    RELEASE_NUM,
    AGENT_ID,
    RELEASE_DATE,
    REVISION_NUM,
    REVISED_DATE,
    APPROVED_FLAG,
    APPROVED_DATE,
    is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
SELECT
    PO_RELEASE_ID,
    PO_HEADER_ID,
    RELEASE_NUM,
    AGENT_ID,
    RELEASE_DATE,
    REVISION_NUM,
    REVISED_DATE,
    APPROVED_FLAG,
    APPROVED_DATE,
	'N' as is_deleted_flg,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )                   AS source_app_id,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    ||'-'|| nvl(PO_RELEASE_ID, 0) AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
FROM  bec_ods.PO_RELEASES_ALL
WHERE 1=1
and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_po_releases' and batch_name = 'ap')
 )
	);	
 
-- Soft delete

update bec_dwh.DIM_PO_RELEASES set is_deleted_flg = 'Y'
where (nvl(PO_RELEASE_ID, 0)) not in (
select nvl(ods.PO_RELEASE_ID, 0) as PO_RELEASE_ID from bec_dwh.DIM_PO_RELEASES dw, bec_ods.PO_RELEASES_ALL ods
where dw.dw_load_id = 
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    ||'-'|| nvl(ods.PO_RELEASE_ID, 0)
AND ods.is_deleted_flg <> 'Y'
);

commit;

end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_po_releases'
and batch_name = 'ap';

COMMIT;