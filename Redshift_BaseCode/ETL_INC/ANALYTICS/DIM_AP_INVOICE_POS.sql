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
--delete 
DELETE FROM bec_dwh.dim_ap_invoice_pos
WHERE EXISTS (
    SELECT 1
    FROM bec_ods.ap_invoice_distributions_all aid
    WHERE aid.kca_seq_date > (
            SELECT (executebegints - prune_days) 
            FROM bec_etl_ctrl.batch_dw_info 
            WHERE dw_table_name = 'dim_ap_invoice_pos' 
              AND batch_name = 'ap'
        )
        AND dim_ap_invoice_pos.invoice_id = aid.invoice_id
);
commit;
--INSERT
insert into bec_dwh.dim_ap_invoice_pos
SELECT invoice_id, MAX(POSTED_FLAG) AS POSTED_FLAG 
FROM bec_ods.ap_invoice_distributions_all 
WHERE kca_seq_date > (
SELECT (executebegints - prune_days) 
FROM bec_etl_ctrl.batch_dw_info 
WHERE dw_table_name = 'dim_ap_invoice_pos' 
  AND batch_name = 'ap'
        ) 
GROUP BY invoice_id;
;


end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_ap_invoice_pos'
	and batch_name = 'ap';

commit;