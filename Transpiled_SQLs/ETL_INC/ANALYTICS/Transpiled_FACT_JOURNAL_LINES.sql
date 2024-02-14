/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_JOURNAL_LINES
WHERE
  (COALESCE(ledger_id, 0), COALESCE(je_batch_id, 0), COALESCE(je_header_id, 0), COALESCE(currency_code, 'NA'), COALESCE(je_line_num, 0), COALESCE(gl_period, 'NA'), COALESCE(code_combination_id, 0), COALESCE(actual_flag, 'X')) IN (
    SELECT
      COALESCE(ods.ledger_id, 0) AS ledger_id,
      COALESCE(ods.je_batch_id, 0) AS je_batch_id,
      COALESCE(ods.je_header_id, 0) AS je_header_id,
      COALESCE(ods.currency_code, 'NA') AS currency_code,
      COALESCE(ods.je_line_num, 0) AS je_line_num,
      COALESCE(ods.gl_period, 'NA') AS gl_period,
      COALESCE(ods.code_combination_id, 0) AS code_combination_id,
      COALESCE(ods.actual_flag, 'NA') AS actual_flag
    FROM gold_bec_dwh.FACT_JOURNAL_LINES AS dw, (
      SELECT
        jl.je_header_id AS je_header_id,
        jl.je_line_num AS je_line_num,
        jl.ledger_id AS ledger_id,
        gl.period_set_name AS period_set_name,
        gcc.chart_of_accounts_id AS chart_of_accounts_id,
        jl.period_name AS gl_period,
        gcc.code_combination_id AS code_combination_id,
        jh.je_category AS je_category,
        jh.je_source AS journal_source,
        jh.je_batch_id AS je_batch_id,
        jh.currency_code AS currency_code,
        jh.actual_flag AS actual_flag,
        jb.posted_by,
        jl.last_update_date
      FROM silver_bec_ods.gl_je_lines AS jl
      INNER JOIN silver_bec_ods.gl_je_headers AS jh
        ON jh.je_header_id = jl.je_header_id
      INNER JOIN silver_bec_ods.gl_je_batches AS jb
        ON jh.je_batch_id = jb.je_batch_id
      INNER JOIN silver_bec_ods.gl_code_combinations AS gcc
        ON gcc.code_combination_id = jl.code_combination_id
      INNER JOIN silver_bec_ods.gl_budget_versions AS gbv
        ON jh.budget_version_id = gbv.budget_version_id
      INNER JOIN silver_bec_ods.gl_ledgers AS gl
        ON jl.ledger_id = gl.ledger_id
      WHERE
        1 = 1
        AND jh.actual_flag = 'B'
        AND gcc.summary_flag = 'N'
        AND (
          jl.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_journal_lines' AND batch_name = 'gl'
          )
          OR jl.is_deleted_flg = 'Y'
          OR jh.is_deleted_flg = 'Y'
          OR jb.is_deleted_flg = 'Y'
          OR gcc.is_deleted_flg = 'Y'
          OR gl.is_deleted_flg = 'Y'
          OR gbv.is_deleted_flg = 'Y'
        )
      UNION ALL
      SELECT
        jl.je_header_id AS je_header_id,
        jl.je_line_num AS je_line_num,
        jl.ledger_id AS ledger_id,
        gl.period_set_name AS period_set_name,
        gcc.chart_of_accounts_id AS chart_of_accounts_id,
        jl.period_name AS gl_period,
        gcc.code_combination_id AS code_combination_id,
        jh.je_category AS je_category,
        jh.je_source AS journal_source,
        jh.je_batch_id AS je_batch_id,
        jh.currency_code AS currency_code,
        jh.actual_flag AS actual_flag,
        jb.posted_by,
        jl.last_update_date
      FROM silver_bec_ods.gl_je_lines AS jl
      INNER JOIN silver_bec_ods.gl_je_headers AS jh
        ON jh.je_header_id = jl.je_header_id
      INNER JOIN silver_bec_ods.gl_je_batches AS jb
        ON jh.je_batch_id = jb.je_batch_id
      INNER JOIN silver_bec_ods.gl_code_combinations AS gcc
        ON gcc.code_combination_id = jl.code_combination_id
      INNER JOIN silver_bec_ods.gl_ledgers AS gl
        ON jl.ledger_id = gl.ledger_id
      WHERE
        1 = 1
        AND jh.actual_flag = 'A'
        AND gcc.summary_flag = 'N'
        AND (
          jl.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_journal_lines' AND batch_name = 'gl'
          )
          OR jl.is_deleted_flg = 'Y'
          OR jh.is_deleted_flg = 'Y'
          OR jb.is_deleted_flg = 'Y'
          OR gcc.is_deleted_flg = 'Y'
          OR gl.is_deleted_flg = 'Y'
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.ledger_id, 0) || '-' || COALESCE(ods.je_batch_id, 0) || '-' || COALESCE(ods.je_header_id, 0) || '-' || COALESCE(ods.currency_code, 'NA') || '-' || COALESCE(ods.je_line_num, 0) || '-' || UPPER(COALESCE(ods.gl_period, 'NA')) || '-' || COALESCE(ods.code_combination_id, 0) || '-' || COALESCE(ods.actual_flag, 'X')
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_JOURNAL_LINES (
  je_header_id,
  je_line_num,
  ledger_id,
  ledger_id_key,
  je_header_id_key,
  period_set_name,
  coa_segval1,
  coa_segval2,
  coa_segval3,
  coa_segval4,
  coa_segval5,
  coa_segval6,
  coa_segval7,
  coa_segval8,
  coa_segval9,
  coa_segval10,
  chart_of_accounts_id,
  segval1,
  segval2,
  segval3,
  segval4,
  segval5,
  segval6,
  segval7,
  segval8,
  segval9,
  segval10,
  period_name,
  gl_period,
  code_combination_id,
  code_combination_id_key,
  account_type,
  actual_or_budget,
  journal_line_status,
  journal_header_status,
  journal_batch_status,
  effective_date,
  description,
  entered_dr,
  entered_cr,
  accounted_dr,
  accounted_cr,
  je_category,
  journal_source,
  journal_name,
  currency_code,
  date_created,
  actual_flag,
  batch_name,
  je_batch_id,
  je_batch_id_key,
  posted_date,
  last_updated_by,
  created_by,
  line_last_update,
  line_creation_date,
  header_desc,
  reversal_flag,
  reversal_status,
  reversal_header_id,
  reversal_period,
  reversal_effective_date,
  attribute1,
  attribute2,
  attribute3,
  approval_status_code,
  approver_employee_id,
  CURRENCY_CONVERSION_RATE,
  CURRENCY_CONVERSION_TYPE,
  CURRENCY_CONVERSION_DATE,
  is_deleted_flg,
  posted_by,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    jl.je_header_id AS je_header_id,
    jl.je_line_num AS je_line_num,
    jl.ledger_id AS ledger_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || jl.ledger_id AS ledger_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || jl.je_header_id AS je_header_id_key,
    gl.period_set_name AS period_set_name,
    gcc.chart_of_accounts_id || '-' || gcc.segment1 AS coa_segval1,
    gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment2, '000') AS coa_segval2,
    gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment3, '000') AS coa_segval3,
    gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment4, '000') AS coa_segval4,
    gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment5, '000') AS coa_segval5,
    gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment6, '000') AS coa_segval6,
    gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment7, '000') AS coa_segval7,
    gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment8, '000') AS coa_segval8,
    gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment9, '000') AS coa_segval9,
    gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment10, '000') AS coa_segval10,
    gcc.chart_of_accounts_id AS chart_of_accounts_id,
    gcc.segment1 AS segval1,
    gcc.segment2 AS segval2,
    gcc.segment3 AS segval3,
    gcc.segment4 AS segval4,
    gcc.segment5 AS segval5,
    gcc.segment6 AS segval6,
    gcc.segment7 AS segval7,
    gcc.segment8 AS segval8,
    gcc.segment9 AS segval9,
    gcc.segment10 AS segval10,
    jl.ledger_id || '-' || UPPER(jl.period_name) AS period_name,
    jl.period_name AS gl_period,
    gcc.code_combination_id AS code_combination_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || gcc.code_combination_id AS code_combination_id_key,
    gcc.account_type AS account_type,
    gbv.budget_name AS actual_or_budget,
    jl.status AS journal_line_status,
    jh.status AS journal_header_status,
    jb.status AS journal_batch_status,
    jl.effective_date AS effective_date,
    jl.description AS description,
    jl.entered_dr AS entered_dr,
    jl.entered_cr AS entered_cr,
    jl.accounted_dr AS accounted_dr,
    jl.accounted_cr AS accounted_cr,
    jh.je_category AS je_category,
    jh.je_source AS journal_source,
    jh.name AS journal_name,
    jh.currency_code AS currency_code,
    CAST(jh.date_created AS DATE) AS date_created,
    jh.actual_flag AS actual_flag,
    jb.name AS batch_name,
    jh.je_batch_id AS je_batch_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || jh.je_batch_id AS je_batch_id_key,
    CAST(jh.posted_date AS DATE) AS posted_date,
    jl.last_updated_by AS last_updated_by,
    jl.created_by AS created_by,
    CAST(jl.last_update_date AS DATE) AS line_last_update,
    CAST(jl.creation_date AS DATE) AS line_creation_date,
    jh.description AS header_desc,
    jh.accrual_rev_flag AS Reversal_Flag,
    jh.accrual_rev_status AS Reversal_Status,
    jh.accrual_rev_je_header_id AS Reversal_header_id,
    jh.accrual_rev_period_name AS Reversal_Period,
    jh.accrual_rev_effective_date AS Reversal_effective_date,
    jl.attribute1,
    jl.attribute2,
    jl.attribute3,
    jb.approval_status_code,
    jb.approver_employee_id,
    jh.CURRENCY_CONVERSION_RATE,
    jh.CURRENCY_CONVERSION_TYPE,
    jh.CURRENCY_CONVERSION_DATE,
    'N' AS is_deleted_flg, /* audit columns */
    jb.posted_by,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) AS source_app_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(jl.ledger_id, 0) || '-' || COALESCE(jh.je_batch_id, 0) || '-' || COALESCE(jh.je_header_id, 0) || '-' || COALESCE(jh.currency_code, 'NA') || '-' || COALESCE(jl.je_line_num, 0) || '-' || UPPER(COALESCE(jl.period_name, 'NA')) || '-' || COALESCE(gcc.code_combination_id, 0) || '-' || COALESCE(jh.actual_flag, 'X') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.gl_je_lines
    WHERE
      is_deleted_flg <> 'Y'
  ) AS jl
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.gl_je_headers
    WHERE
      is_deleted_flg <> 'Y'
  ) AS jh
    ON jh.je_header_id = jl.je_header_id
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.gl_je_batches
    WHERE
      is_deleted_flg <> 'Y'
  ) AS jb
    ON jh.je_batch_id = jb.je_batch_id
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.gl_code_combinations
    WHERE
      is_deleted_flg <> 'Y'
  ) AS gcc
    ON gcc.code_combination_id = jl.code_combination_id
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.gl_budget_versions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS gbv
    ON jh.budget_version_id = gbv.budget_version_id
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.gl_ledgers
    WHERE
      is_deleted_flg <> 'Y'
  ) AS gl
    ON jl.ledger_id = gl.ledger_id
  WHERE
    1 = 1
    AND jh.actual_flag = 'B'
    AND gcc.summary_flag = 'N'
    AND jl.kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_dw_info
      WHERE
        dw_table_name = 'fact_journal_lines' AND batch_name = 'gl'
    )
  UNION ALL
  SELECT
    jl.je_header_id AS je_header_id,
    jl.je_line_num AS je_line_num,
    jl.ledger_id AS ledger_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || jl.ledger_id AS ledger_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || jl.je_header_id AS je_header_id_key,
    gl.period_set_name AS period_set_name,
    gcc.chart_of_accounts_id || '-' || gcc.segment1 AS coa_segval1,
    gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment2, '000') AS coa_segval2,
    gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment3, '000') AS coa_segval3,
    gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment4, '000') AS coa_segval4,
    gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment5, '000') AS coa_segval5,
    gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment6, '000') AS coa_segval6,
    gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment7, '000') AS coa_segval7,
    gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment8, '000') AS coa_segval8,
    gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment9, '000') AS coa_segval9,
    gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment10, '000') AS coa_segval10,
    gcc.chart_of_accounts_id AS chart_of_accounts_id,
    gcc.segment1 AS segval1,
    gcc.segment2 AS segval2,
    gcc.segment3 AS segval3,
    gcc.segment4 AS segval4,
    gcc.segment5 AS segval5,
    gcc.segment6 AS segval6,
    gcc.segment7 AS segval7,
    gcc.segment8 AS segval8,
    gcc.segment9 AS segval9,
    gcc.segment10 AS segval10,
    jl.ledger_id || '-' || UPPER(jl.period_name) AS period_name,
    jl.period_name AS gl_period,
    gcc.code_combination_id AS code_combination_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || gcc.code_combination_id AS code_combination_id_key,
    gcc.account_type AS account_type,
    'ACTUAL' AS actual_or_budget,
    jl.status AS journal_line_status,
    jh.status AS journal_header_status,
    jb.status AS journal_batch_status,
    jl.effective_date AS effective_date,
    jl.description AS description,
    jl.entered_dr AS entered_dr,
    jl.entered_cr AS entered_cr,
    jl.accounted_dr AS accounted_dr,
    jl.accounted_cr AS accounted_cr,
    jh.je_category AS je_category,
    jh.je_source AS journal_source,
    jh.name AS journal_name,
    jh.currency_code AS currency_code,
    CAST(jh.date_created AS DATE) AS date_created,
    jh.actual_flag AS actual_flag,
    jb.name AS batch_name,
    jh.je_batch_id AS je_batch_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || jh.je_batch_id AS je_batch_id_key,
    CAST(jh.posted_date AS DATE) AS posted_date,
    jl.last_updated_by AS last_updated_by,
    jl.created_by AS created_by,
    CAST(jl.last_update_date AS DATE) AS line_last_update,
    CAST(jl.creation_date AS DATE) AS line_creation_date,
    jh.description AS header_desc,
    jh.accrual_rev_flag AS Reversal_Flag,
    jh.accrual_rev_status AS Reversal_Status,
    jh.accrual_rev_je_header_id AS Reversal_header_id,
    jh.accrual_rev_period_name AS Reversal_Period,
    jh.accrual_rev_effective_date AS Reversal_effective_date,
    jl.attribute1,
    jl.attribute2,
    jl.attribute3,
    jb.approval_status_code,
    jb.approver_employee_id,
    jh.CURRENCY_CONVERSION_RATE,
    jh.CURRENCY_CONVERSION_TYPE,
    jh.CURRENCY_CONVERSION_DATE,
    'N' AS is_deleted_flg, /* audit columns */
    jb.posted_by,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) AS source_app_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(jl.ledger_id, 0) || '-' || COALESCE(jh.je_batch_id, 0) || '-' || COALESCE(jh.je_header_id, 0) || '-' || COALESCE(jh.currency_code, 'NA') || '-' || COALESCE(jl.je_line_num, 0) || '-' || UPPER(COALESCE(jl.period_name, 'NA')) || '-' || COALESCE(gcc.code_combination_id, 0) || '-' || COALESCE(jh.actual_flag, 'X') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.gl_je_lines
    WHERE
      is_deleted_flg <> 'Y'
  ) AS jl
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.gl_je_headers
    WHERE
      is_deleted_flg <> 'Y'
  ) AS jh
    ON jh.je_header_id = jl.je_header_id
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.gl_je_batches
    WHERE
      is_deleted_flg <> 'Y'
  ) AS jb
    ON jh.je_batch_id = jb.je_batch_id
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.gl_code_combinations
    WHERE
      is_deleted_flg <> 'Y'
  ) AS gcc
    ON gcc.code_combination_id = jl.code_combination_id
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.gl_ledgers
    WHERE
      is_deleted_flg <> 'Y'
  ) AS gl
    ON jl.ledger_id = gl.ledger_id
  WHERE
    1 = 1
    AND jh.actual_flag = 'A'
    AND gcc.summary_flag = 'N'
    AND jl.kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_dw_info
      WHERE
        dw_table_name = 'fact_journal_lines' AND batch_name = 'gl'
    )
);