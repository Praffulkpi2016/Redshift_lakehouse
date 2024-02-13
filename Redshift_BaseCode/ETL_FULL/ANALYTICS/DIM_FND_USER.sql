/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for dimension.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh.DIM_FND_USER;

create table bec_dwh.DIM_FND_USER 
DISTKEY (employee_id)
SORTKEY (start_date, end_date, effective_end_date)
as
( SELECT distinct
    fu.user_id,
    ppf.person_id,
    nvl(ppf.full_name, fu.user_name) employee_name,
	fu.user_name,
    ppf.employee_number,
    fu.start_date,
    fu.end_date,
    fu.description,
    fu.employee_id,
    fu.email_address,
    fu.customer_id,
    fu.supplier_id,
    ppf.effective_end_date,
    ppf.full_name,
	0 as employee_count ,
    'N'                              AS is_deleted_flg,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )                                AS source_app_id,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-'
       || nvl(fu.user_id, 0)  ||'-'|| nvl(fu.employee_id, 0) ||'-'||nvl(ppf.person_id,0)        AS dw_load_id,
    getdate()                        AS dw_insert_date,
    getdate()                        AS dw_update_date
FROM
    (
        SELECT
            *
        FROM
            bec_ods.fnd_user
        WHERE
            is_deleted_flg <> 'Y'
			--and user_id in (select max(user_id) from bec_ods.fnd_user group by employee_id)
    ) fu
    LEFT JOIN (
        SELECT
            *
        FROM
            bec_ods.per_all_people_f
        WHERE
            is_deleted_flg <> 'Y'
    ) ppf ON fu.employee_id = ppf.person_id
             AND sysdate BETWEEN ppf.effective_start_date AND ppf.effective_end_date
    LEFT JOIN (
        SELECT
            person_id,
            MAX(object_version_number) AS object_version_number
        FROM
            bec_ods.per_all_people_f
        WHERE
            is_deleted_flg <> 'Y'
        GROUP BY
            person_id
    ) ppf_max ON ppf.person_id = ppf_max.person_id
                 AND ppf.object_version_number = ppf_max.object_version_number
WHERE
    1 = 1
);

commit;

update bec_dwh.dim_fnd_user fnd set employee_count = 
(select count(employee_id) from bec_ods.fnd_user fnd1 where fnd.employee_id=fnd1.employee_id);

commit;

end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
        dw_table_name = 'dim_fnd_user'
    AND batch_name = 'ap';

COMMIT;

