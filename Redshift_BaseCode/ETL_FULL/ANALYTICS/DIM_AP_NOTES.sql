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

drop table if exists bec_dwh.DIM_AP_NOTES;

create table bec_dwh.DIM_AP_NOTES diststyle all sortkey(ORG_ID,REPORT_HEADER_ID,NOTE_ID)
as
(

select
	an.note_id,
	aer.REPORT_HEADER_ID,
    aer.org_id,
    aer.invoice_num,
    an.entered_date,
    an.notes_detail,
	'N' as is_deleted_flg,
		(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    ) as source_app_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    )
    || '-'
       || nvl(an.NOTE_ID, 0)||'-'||nvl(aer.REPORT_HEADER_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM
    bec_ods.ap_notes                      an,
    bec_ods.ap_expense_report_headers_all aer
WHERE
        1 = 1
    AND an.source_object_id = aer.report_header_id
--  AND aer.org_id = 85
    AND an.notes_detail LIKE 'Approver Action: Approve%'
    AND an.entered_date = (
        SELECT
            MAX(entered_date)
        FROM
            bec_ods.ap_notes an1
        WHERE
                an1.source_object_id = an.source_object_id
            AND an1.notes_detail LIKE 'Approver Action: Approve%'
    )
	
);

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_ap_notes'
	and batch_name = 'ap';

commit;