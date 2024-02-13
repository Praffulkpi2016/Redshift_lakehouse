/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;

-- Delete Records

delete from bec_dwh.dim_ap_terms
where nvl(term_id,0) in (
select nvl(ods.term_id,0) from bec_dwh.dim_ap_terms dw, bec_ods.ap_terms_tl ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.term_id,0) 
and (ods.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ap_terms' and batch_name = 'ap')
 )
);

commit;

-- Insert records

INSERT INTO bec_dwh.DIM_AP_TERMS 
(
term_id
,ap_term_type
,start_date_active
,end_date_active
,enabled_flag
,ap_term
,ap_term_desc
,is_deleted_flg
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
)
    ( SELECT
        term_id,
        type               "AP_TERM_TYPE",
        start_date_active,
        end_date_active,
        enabled_flag,
        name               "AP_TERM",
        description        "AP_TERM_DESC",
		'N' as is_deleted_flg,
        (
            SELECT
                system_id
            FROM
                bec_etl_ctrl.etlsourceappid
            WHERE
                source_system = 'EBS'
        )                  AS source_app_id,
        (
            SELECT
                system_id
            FROM
                bec_etl_ctrl.etlsourceappid
            WHERE
                source_system = 'EBS'
        )
        || '-'
        || nvl(term_id, 0) AS dw_load_id,
        getdate()          AS dw_insert_date,
        getdate()          AS dw_update_date
    FROM
        bec_ods.ap_terms_tl
    WHERE
        1=1 and language = 'US'
		and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ap_terms' and batch_name = 'ap')
 )
        )
;

-- Soft delete

update bec_dwh.dim_ap_terms set is_deleted_flg = 'Y'
where nvl(term_id,0) not in (
select nvl(ods.term_id,0) from bec_dwh.dim_ap_terms dw, bec_ods.ap_terms_tl ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.term_id,0) 
AND ods.is_deleted_flg <> 'Y');

commit;

end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ap_terms' and batch_name = 'ap';

COMMIT;