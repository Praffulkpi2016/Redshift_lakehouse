/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_GL_ACCOUNT_ANALYSIS
WHERE
  (COALESCE(LINE_NUMBER, 0), COALESCE(HEADER_ID, 0), COALESCE(LEDGER_CURRENCY, 'NA'), COALESCE(TRANSLATED_FLAG, 'NA'), COALESCE(TEMP_LINE_NUM, 0)) IN (
    SELECT
      COALESCE(ods.LINE_NUMBER, 0) AS LINE_NUMBER,
      COALESCE(ods.HEADER_ID, 0) AS HEADER_ID,
      COALESCE(ods.LEDGER_CURRENCY, 'NA') AS LEDGER_CURRENCY,
      COALESCE(ods.TRANSLATED_FLAG, 'NA') AS TRANSLATED_FLAG,
      COALESCE(ods.TEMP_LINE_NUM, 0) AS TEMP_LINE_NUM
    FROM gold_bec_dwh.FACT_GL_ACCOUNT_ANALYSIS AS dw, (
      SELECT
        A.GL_DATE,
        A.created_by_id,
        A.creation_date,
        A.last_update_date,
        A.GL_TRANSFER_DATE,
        A.reference_date,
        A.COMPLETED_DATE,
        A.TRANSACTION_NUMBER,
        A.TRANSACTION_DATE,
        A.ACCOUNTING_SEQUENCE_NAME,
        A.ACCOUNTING_SEQUENCE_VERSION,
        A.ACCOUNTING_SEQUENCE_NUMBER,
        A.REPORTING_SEQUENCE_NAME,
        A.REPORTING_SEQUENCE_VERSION,
        A.REPORTING_SEQUENCE_NUMBER,
        A.DOCUMENT_CATEGORY,
        A.DOCUMENT_SEQUENCE_NAME,
        A.DOCUMENT_SEQUENCE_NUMBER,
        A.APPLICATION_ID,
        A.APPLICATION_NAME,
        A.HEADER_ID,
        A.HEADER_DESCRIPTION,
        A.FUND_STATUS,
        A.je_category,
        A.JE_SOURCE_NAME,
        A.EVENT_ID,
        A.EVENT_DATE,
        A.EVENT_NUMBER,
        A.EVENT_CLASS_CODE,
        A.EVENT_CLASS_NAME,
        A.EVENT_TYPE_CODE,
        A.EVENT_TYPE_NAME,
        A.GL_BATCH_NAME,
        A.POSTED_DATE,
        A.je_header_id,
        A.GL_JE_NAME,
        A.REVERSAL_PERIOD,
        A.REVERSAL_STATUS,
        A.EXTERNAL_REFERENCE,
        A.GL_LINE_NUMBER,
        A.effective_date,
        A.LINE_NUMBER,
        A.ORIG_LINE_NUMBER,
        A.ACCOUNTING_CLASS_CODE,
        A.ACCOUNTING_CLASS_NAME,
        A.LINE_DESCRIPTION,
        A.ENTERED_CURRENCY,
        A.CONVERSION_RATE,
        A.CONVERSION_RATE_DATE,
        A.CONVERSION_RATE_TYPE_CODE,
        A.CONVERSION_RATE_TYPE,
        A.ENTERED_DR,
        A.ENTERED_CR,
        A.UNROUNDED_ACCOUNTED_DR,
        A.UNROUNDED_ACCOUNTED_CR,
        A.ACCOUNTED_DR,
        A.ACCOUNTED_CR,
        A.STATISTICAL_AMOUNT,
        A.RECONCILIATION_REFERENCE,
        A.PARTY_TYPE_CODE,
        A.PARTY_TYPE,
        A.LEDGER_ID,
        A.LEDGER_SHORT_NAME,
        A.LEDGER_DESCRIPTION,
        A.LEDGER_NAME,
        A.LEDGER_CURRENCY,
        A.PERIOD_YEAR,
        A.PERIOD_NUMBER,
        A.PERIOD_NAME,
        A.PERIOD_START_DATE,
        A.PERIOD_END_DATE,
        A.BALANCE_TYPE_CODE,
        A.BALANCE_TYPE,
        A.BUDGET_NAME,
        A.ENCUMBRANCE_TYPE,
        A.BEGIN_BALANCE_DR,
        A.BEGIN_BALANCE_CR,
        A.PERIOD_NET_DR,
        A.PERIOD_NET_CR,
        A.CODE_COMBINATION_ID,
        A.ACCOUNTING_CODE_COMBINATION,
        A.CODE_COMBINATION_DESCRIPTION,
        A.CONTROL_ACCOUNT_FLAG,
        A.CONTROL_ACCOUNT,
        A.BALANCING_SEGMENT,
        A.NATURAL_ACCOUNT_SEGMENT,
        A.COST_CENTER_SEGMENT,
        A.MANAGEMENT_SEGMENT,
        A.INTERCOMPANY_SEGMENT,
        A.BALANCING_SEGMENT_DESC,
        A.NATURAL_ACCOUNT_DESC,
        A.COST_CENTER_DESC,
        A.MANAGEMENT_SEGMENT_DESC,
        A.INTERCOMPANY_SEGMENT_DESC,
        A.segment1,
        A.segment2,
        A.segment3,
        A.segment4,
        A.segment5,
        A.segment6,
        A.segment7,
        A.segment8,
        A.segment9,
        A.segment10,
        A.BEGIN_RUNNING_TOTAL_CR,
        A.BEGIN_RUNNING_TOTAL_DR,
        A.END_RUNNING_TOTAL_CR,
        A.END_RUNNING_TOTAL_DR,
        A.LEGAL_ENTITY_ID,
        A.LEGAL_ENTITY_NAME,
        A.LE_ADDRESS_LINE_1,
        A.LE_ADDRESS_LINE_2,
        A.LE_ADDRESS_LINE_3,
        A.LE_CITY,
        A.LE_REGION_1,
        A.LE_REGION_2,
        A.LE_REGION_3,
        A.LE_POSTAL_CODE,
        A.LE_COUNTRY,
        A.LE_REGISTRATION_NUMBER,
        A.LE_REGISTRATION_EFFECTIVE_FROM,
        A.LE_BR_DAILY_INSCRIPTION_NUMBER,
        A.LE_BR_DAILY_INSCRIPTION_DATE,
        A.LE_BR_DAILY_ENTITY,
        A.LE_BR_DAILY_LOCATION,
        A.LE_BR_DIRECTOR_NUMBER,
        A.LE_BR_ACCOUNTANT_NUMBER,
        A.LE_BR_ACCOUNTANT_NAME,
        A.BE_PROJECT_NO,
        A.BE_PROJECT_DESC,
        A.BE_TASK_NO,
        A.BE_TASK_DESC,
        A.approver_name,
        A.PO_NUMBER,
        A.invoice_num,
        A.INVOICE_ID,
        A.vendor_id,
        A.vendor_site_id,
        A.TRANSLATED_FLAG,
        A.TEMP_LINE_NUM,
        (
          SELECT
            system_id
          FROM bec_etl_ctrl.etlsourceappid
          WHERE
            source_system = 'EBS'
        ) || '-' || A.je_category AS je_category_KEY,
        (
          SELECT
            system_id
          FROM bec_etl_ctrl.etlsourceappid
          WHERE
            source_system = 'EBS'
        ) || '-' || A.je_header_id AS je_header_id_KEY,
        (
          SELECT
            system_id
          FROM bec_etl_ctrl.etlsourceappid
          WHERE
            source_system = 'EBS'
        ) || '-' || A.LEDGER_ID AS LEDGER_ID_KEY,
        (
          SELECT
            system_id
          FROM bec_etl_ctrl.etlsourceappid
          WHERE
            source_system = 'EBS'
        ) || '-' || A.CODE_COMBINATION_ID AS CODE_COMBINATION_ID_KEY,
        (
          SELECT
            system_id
          FROM bec_etl_ctrl.etlsourceappid
          WHERE
            source_system = 'EBS'
        ) || '-' || A.LEGAL_ENTITY_ID AS LEGAL_ENTITY_ID_KEY,
        (
          SELECT
            system_id
          FROM bec_etl_ctrl.etlsourceappid
          WHERE
            source_system = 'EBS'
        ) || '-' || A.INVOICE_ID AS INVOICE_ID_KEY,
        (
          SELECT
            system_id
          FROM bec_etl_ctrl.etlsourceappid
          WHERE
            source_system = 'EBS'
        ) || '-' || A.vendor_id AS vendor_id_KEY, /* audit columns */
        'N' AS is_deleted_flg,
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
        ) || '-' || COALESCE(A.LINE_NUMBER, 0) || '-' || COALESCE(A.HEADER_ID, 0) || '-' || COALESCE(A.LEDGER_CURRENCY, 'NA') || '-' || COALESCE(A.TRANSLATED_FLAG, 'NA') || '-' || COALESCE(A.TEMP_LINE_NUM, 0) AS dw_load_id,
        CURRENT_TIMESTAMP() AS dw_insert_date,
        CURRENT_TIMESTAMP() AS dw_update_date
      FROM (
        (
          WITH CTE_PO_AP AS (
            SELECT
              pa1.segment1,
              pa1.name,
              pa1.description AS BE_PROJECT_DESC,
              pt1.task_number,
              pt1.description AS BE_TASK_DESC,
              PHA.SEGMENT1 AS PO_NUMBER,
              AIA.invoice_num,
              AIA.INVOICE_ID,
              AIA.vendor_id,
              AIA.vendor_site_id,
              apd1.invoice_distribution_id
            FROM silver_bec_ods.ap_invoice_distributions_all AS apd1, silver_bec_ods.po_distributions_all AS pda, silver_bec_ods.AP_INVOICES_ALL AS AIA, silver_bec_ods.po_headers_all AS pha, silver_bec_ods.pa_projects_all AS pa1, silver_bec_ods.pa_tasks AS pt1
            WHERE
              1 = 1
              AND pt1.TASK_ID() = apd1.task_id
              AND apd1.INVOICE_ID = AIA.INVOICE_ID
              AND pa1.PROJECT_ID() = apd1.project_id
              AND PDA.PO_DISTRIBUTION_ID() = apd1.PO_DISTRIBUTION_ID
              AND PHA.PO_HEADER_ID() = PDA.PO_HEADER_ID
              AND (
                apd1.kca_seq_date >= (
                  SELECT
                    (
                      executebegints - prune_days
                    )
                  FROM bec_etl_ctrl.batch_dw_info
                  WHERE
                    dw_table_name = 'fact_gl_account_analysis' AND batch_name = 'gl'
                )
                OR AIA.kca_seq_date >= (
                  SELECT
                    (
                      executebegints - prune_days
                    )
                  FROM bec_etl_ctrl.batch_dw_info
                  WHERE
                    dw_table_name = 'fact_gl_account_analysis' AND batch_name = 'gl'
                )
                OR apd1.is_deleted_flg = 'Y'
                OR pda.is_deleted_flg = 'Y'
                OR AIA.is_deleted_flg = 'Y'
                OR pha.is_deleted_flg = 'Y'
                OR pa1.is_deleted_flg = 'Y'
                OR pt1.is_deleted_flg = 'Y'
              )
          )
          SELECT
            XLA.GL_DATE,
            OTT.created_by_id,
            XLA.creation_date,
            XLA.last_update_date,
            XLA.GL_TRANSFER_DATE,
            XLA.reference_date,
            XLA.COMPLETED_DATE,
            XLA.TRANSACTION_NUMBER,
            XLA.TRANSACTION_DATE,
            XLA.ACCOUNTING_SEQUENCE_NAME,
            XLA.ACCOUNTING_SEQUENCE_VERSION,
            XLA.ACCOUNTING_SEQUENCE_NUMBER,
            XLA.REPORTING_SEQUENCE_NAME,
            XLA.REPORTING_SEQUENCE_VERSION,
            XLA.REPORTING_SEQUENCE_NUMBER,
            NULL AS DOCUMENT_CATEGORY,
            XLA.DOCUMENT_SEQUENCE_NAME,
            XLA.DOCUMENT_SEQUENCE_NUMBER,
            XLA.APPLICATION_ID,
            NULL AS APPLICATION_NAME,
            XLA.HEADER_ID,
            XLA.HEADER_DESCRIPTION,
            XLA.FUND_STATUS,
            XLA.je_category_name AS je_category,
            OTT.JE_SOURCE_NAME,
            XLA.EVENT_ID,
            XLA.EVENT_DATE,
            XLA.EVENT_NUMBER,
            XLA.EVENT_CLASS_CODE,
            XLA.EVENT_CLASS_NAME,
            XLA.EVENT_TYPE_CODE,
            XLA.EVENT_TYPE_NAME,
            OTT.GL_BATCH_NAME,
            OTT.POSTED_DATE,
            OTT.je_header_id,
            OTT.GL_JE_NAME,
            OTT.REVERSAL_PERIOD,
            OTT.REVERSAL_STATUS,
            OTT.EXTERNAL_REFERENCE,
            OTT.GL_LINE_NUMBER,
            OTT.effective_date,
            XLA.LINE_NUMBER,
            XLA.ORIG_LINE_NUMBER,
            XLA.ACCOUNTING_CLASS_CODE,
            XLA.ACCOUNTING_CLASS_NAME,
            XLA.LINE_DESCRIPTION,
            XLA.ENTERED_CURRENCY,
            XLA.CONVERSION_RATE,
            XLA.CONVERSION_RATE_DATE,
            XLA.CONVERSION_RATE_TYPE_CODE,
            XLA.CONVERSION_RATE_TYPE,
            XLA.ENTERED_DR,
            XLA.ENTERED_CR,
            XLA.UNROUNDED_ACCOUNTED_DR,
            XLA.UNROUNDED_ACCOUNTED_CR,
            XLA.ACCOUNTED_DR,
            XLA.ACCOUNTED_CR,
            XLA.STATISTICAL_AMOUNT,
            XLA.RECONCILIATION_REFERENCE,
            XLA.PARTY_TYPE_CODE,
            NULL AS PARTY_TYPE,
            OTT.LEDGER_ID,
            OTT.LEDGER_SHORT_NAME,
            OTT.LEDGER_DESCRIPTION,
            OTT.LEDGER_NAME,
            OTT.LEDGER_CURRENCY,
            OTT.PERIOD_YEAR,
            OTT.PERIOD_NUMBER,
            OTT.PERIOD_NAME,
            OTT.PERIOD_START_DATE,
            OTT.PERIOD_END_DATE,
            OTT.BALANCE_TYPE_CODE,
            OTT.BALANCE_TYPE,
            OTT.BUDGET_NAME,
            OTT.ENCUMBRANCE_TYPE,
            OTT.BEGIN_BALANCE_DR,
            OTT.BEGIN_BALANCE_CR,
            OTT.PERIOD_NET_DR,
            OTT.PERIOD_NET_CR,
            OTT.CODE_COMBINATION_ID,
            OTT.ACCOUNTING_CODE_COMBINATION,
            OTT.CODE_COMBINATION_DESCRIPTION,
            OTT.CONTROL_ACCOUNT_FLAG,
            OTT.CONTROL_ACCOUNT,
            OTT.BALANCING_SEGMENT,
            OTT.NATURAL_ACCOUNT_SEGMENT,
            OTT.COST_CENTER_SEGMENT,
            OTT.MANAGEMENT_SEGMENT,
            OTT.INTERCOMPANY_SEGMENT,
            OTT.BALANCING_SEGMENT_DESC,
            OTT.NATURAL_ACCOUNT_DESC,
            OTT.COST_CENTER_DESC,
            OTT.MANAGEMENT_SEGMENT_DESC,
            OTT.INTERCOMPANY_SEGMENT_DESC,
            OTT.segment1 AS SEGMENT1,
            OTT.segment2 AS SEGMENT2,
            OTT.segment3 AS SEGMENT3,
            OTT.segment4 AS SEGMENT4,
            OTT.segment5 AS SEGMENT5,
            OTT.segment6 AS SEGMENT6,
            OTT.segment7 AS SEGMENT7,
            OTT.segment8 AS SEGMENT8,
            OTT.segment9 AS SEGMENT9,
            OTT.segment10 AS SEGMENT10,
            OTT.BEGIN_RUNNING_TOTAL_CR,
            OTT.BEGIN_RUNNING_TOTAL_DR,
            OTT.END_RUNNING_TOTAL_CR,
            OTT.END_RUNNING_TOTAL_DR,
            OTT.LEGAL_ENTITY_ID,
            OTT.LEGAL_ENTITY_NAME,
            OTT.LE_ADDRESS_LINE_1,
            OTT.LE_ADDRESS_LINE_2,
            OTT.LE_ADDRESS_LINE_3,
            OTT.LE_CITY,
            OTT.LE_REGION_1,
            OTT.LE_REGION_2,
            OTT.LE_REGION_3,
            OTT.LE_POSTAL_CODE,
            OTT.LE_COUNTRY,
            OTT.LE_REGISTRATION_NUMBER,
            OTT.LE_REGISTRATION_EFFECTIVE_FROM,
            OTT.LE_BR_DAILY_INSCRIPTION_NUMBER,
            OTT.LE_BR_DAILY_INSCRIPTION_DATE,
            OTT.LE_BR_DAILY_ENTITY,
            OTT.LE_BR_DAILY_LOCATION,
            OTT.LE_BR_DIRECTOR_NUMBER,
            OTT.LE_BR_ACCOUNTANT_NUMBER,
            OTT.LE_BR_ACCOUNTANT_NAME,
            (
              CASE
                WHEN XLA.event_class_code = 'SALES_ORDER'
                AND OTT.user_je_source_name = 'Cost Management'
                AND XLA.ent_entity_code = 'MTL_ACCOUNTING_EVENTS'
                THEN COALESCE(XLA.order_Number, ' ')
                ELSE POAP.segment1 || ' - ' || POAP.name
              END
            ) AS BE_PROJECT_NO,
            POAP.BE_PROJECT_DESC,
            (
              CASE
                WHEN XLA.entity_code = 'TRANSACTIONS' AND OTT.user_je_source_name = 'Receivables'
                THEN COALESCE(XLA.REC_address1, '')
                WHEN XLA.event_class_code = 'ADJUSTMENT' AND OTT.user_je_source_name = 'Receivables'
                THEN COALESCE(XLA.ADJ_address1, '')
                WHEN XLA.event_class_code = 'SALES_ORDER'
                AND OTT.user_je_source_name = 'Cost Management'
                AND XLA.ent_entity_code = 'MTL_ACCOUNTING_EVENTS'
                THEN COALESCE(XLA.address1, ' ')
                ELSE POAP.task_number || ' '
              END
            ) AS BE_TASK_NO,
            POAP.BE_TASK_DESC,
            OTT.approver_name,
            (
              COALESCE(POAP.PO_NUMBER, ' ')
            ) AS PO_NUMBER,
            POAP.invoice_num,
            POAP.INVOICE_ID,
            POAP.vendor_id,
            POAP.vendor_site_id,
            OTT.TRANSLATED_FLAG,
            XLA.TEMP_LINE_NUM
          FROM gold_bec_dwh.FACT_GL_XLA_STG AS XLA, gold_bec_dwh.FACT_GL_JOURNAL_STG AS OTT, CTE_PO_AP AS POAP
          WHERE
            1 = 1
            AND OTT.gl_sl_link_id = XLA.gl_sl_link_id
            AND OTT.gl_sl_link_table = XLA.gl_sl_link_table
            AND XLA.balance_type_code = OTT.balance_type_code
            AND COALESCE(XLA.budget_version_id, -19999) = COALESCE(OTT.budget_version_id, -19999)
            AND COALESCE(XLA.encumbrance_type_id, -19999) = COALESCE(OTT.encumbrance_type_id, -19999)
            AND POAP.INVOICE_DISTRIBUTION_ID() = XLA.alloc_to_dist_id_num_1
        )
        UNION ALL
        (
          /* Main Query Starts Here */ /* ----------------------------------------------------------------------------------------------------------- */
          WITH gjl_gjh AS (
            SELECT
              gjh.default_effective_date,
              gjh.creation_date,
              gjh.last_update_date,
              gjh.reference_date,
              gjh.posting_acct_seq_value,
              gjh.close_acct_seq_value,
              gjh.je_header_id AS header_id,
              gjh.description AS HEADER_DESCRIPTION,
              gjh.je_category,
              gjh.je_header_id,
              gjh.NAME,
              gjh.ACCRUAL_REV_PERIOD_NAME,
              gjh.ACCRUAL_REV_STATUS,
              gjh.external_reference,
              gjh.currency_code,
              gjh.currency_conversion_rate,
              gjh.currency_conversion_date,
              gjh.currency_conversion_type,
              gjh.actual_flag,
              gjh.je_from_sla_flag,
              gjh.budget_version_id,
              gjh.encumbrance_type_id,
              gjh.je_batch_id,
              gjh.posting_acct_seq_version_id,
              gjh.close_acct_seq_version_id,
              gjh.je_source,
              gjl.ledger_id,
              gjl.code_combination_id,
              gjl.period_name,
              gjl.created_by,
              gjl.je_line_num AS GL_LINE_NUMBER,
              gjl.effective_date,
              gjl.je_line_num AS LINE_NUMBER,
              gjl.je_line_num AS ORIG_LINE_NUMBER,
              gjl.description AS LINE_DESCRIPTION,
              gjl.entered_dr,
              gjl.entered_cr,
              gjl.accounted_dr,
              gjl.accounted_cr,
              gjl.stat_amount,
              gjl.jgzz_recon_ref_11i,
              gjl.attribute1,
              gjl.attribute2
            FROM silver_bec_ods.gl_je_lines AS gjl, silver_bec_ods.gl_je_headers AS gjh
            WHERE
              gjh.je_header_id = gjl.je_header_id
              AND gjl.last_update_date >= TO_DATE('2020-01-01 12:00:00', 'YYYY-MM-DD HH:MI:SS')
              AND gjh.last_update_date >= TO_DATE('2020-01-01 12:00:00', 'YYYY-MM-DD HH:MI:SS')
              AND (
                gjl.kca_seq_date >= (
                  SELECT
                    (
                      executebegints - prune_days
                    )
                  FROM bec_etl_ctrl.batch_dw_info
                  WHERE
                    dw_table_name = 'fact_gl_account_analysis' AND batch_name = 'gl'
                )
                OR gjh.kca_seq_date >= (
                  SELECT
                    (
                      executebegints - prune_days
                    )
                  FROM bec_etl_ctrl.batch_dw_info
                  WHERE
                    dw_table_name = 'fact_gl_account_analysis' AND batch_name = 'gl'
                )
                OR gjl.is_deleted_flg = 'Y'
                OR gjh.is_deleted_flg = 'Y'
              )
              AND COALESCE(gjh.je_from_sla_flag, 'N') = 'N'
          ), gjst /* -----------------------------------------------------------------------------------------------------------  */ AS (
            SELECT
              gjst.user_je_source_name AS user_je_source_name,
              gjst.je_source_name AS je_source_name,
              gjst.language AS `language`
            FROM silver_bec_ods.gl_je_sources_tl AS gjst
            WHERE
              gjst.language = 'US'
          ), gdct /* ----------------------------------------------------------------------------------------------------------- */ AS (
            SELECT
              gdct.user_conversion_type,
              gdct.conversion_type
            FROM silver_bec_ods.gl_daily_conversion_types AS gdct
          ), gjb_papf /* ----------------------------------------------------------------------------------------------------------- */ AS (
            SELECT
              gjb.je_batch_id,
              gjb.NAME,
              gjb.status,
              gjb.APPROVER_EMPLOYEE_ID,
              gjb.creation_date,
              gjb.posted_date,
              papf.person_id,
              papf.full_name,
              papf.effective_start_date,
              papf.effective_end_date
            FROM silver_bec_ods.gl_je_batches AS gjb, silver_bec_ods.per_all_people_f AS papf
            WHERE
              gjb.APPROVER_EMPLOYEE_ID = papf.PERSON_ID()
              AND gjb.creation_date BETWEEN papf.EFFECTIVE_START_DATE() AND COALESCE(papf.EFFECTIVE_END_DATE(), CURRENT_TIMESTAMP() + 1)
              AND gjb.status = 'P'
              AND (
                gjb.kca_seq_date >= (
                  SELECT
                    (
                      executebegints - prune_days
                    )
                  FROM bec_etl_ctrl.batch_dw_info
                  WHERE
                    dw_table_name = 'fact_gl_account_analysis' AND batch_name = 'gl'
                )
                OR gjb.is_deleted_flg = 'Y'
                OR papf.is_deleted_flg = 'Y'
              )
          ) /* ----------------------------------------------------------------------------------------------------------- */
          SELECT
            gjl_gjh.default_effective_date AS GL_DATE,
            gjl_gjh.created_by AS created_by_id,
            gjl_gjh.CREATION_DATE,
            gjl_gjh.LAST_UPDATE_DATE,
            CAST(NULL AS TIMESTAMP) AS GL_TRANSFER_DATE,
            gjl_gjh.REFERENCE_DATE,
            CAST(NULL AS TIMESTAMP) AS COMPLETED_DATE,
            NULL AS TRANSACTION_NUMBER,
            CAST(NULL AS TIMESTAMP) AS TRANSACTION_DATE,
            fsv1.header_name AS ACCOUNTING_SEQUENCE_NAME,
            fsv1.version_name AS ACCOUNTING_SEQUENCE_VERSION,
            gjl_gjh.posting_acct_seq_value AS ACCOUNTING_SEQUENCE_NUMBER,
            fsv2.header_name AS REPORTING_SEQUENCE_NAME,
            fsv2.version_name AS REPORTING_SEQUENCE_VERSION,
            gjl_gjh.close_acct_seq_value AS REPORTING_SEQUENCE_NUMBER,
            NULL AS DOCUMENT_CATEGORY,
            NULL AS DOCUMENT_SEQUENCE_NAME,
            NULL AS DOCUMENT_SEQUENCE_NUMBER,
            NULL AS APPLICATION_ID,
            NULL AS APPLICATION_NAME, /* 1 */
            gjl_gjh.HEADER_ID,
            gjl_gjh.HEADER_DESCRIPTION,
            NULL AS FUND_STATUS,
            gjl_gjh.je_category,
            gjst.user_je_source_name AS JE_SOURCE_NAME,
            NULL AS EVENT_ID,
            CAST(NULL AS TIMESTAMP) AS EVENT_DATE,
            NULL AS EVENT_NUMBER,
            NULL AS EVENT_CLASS_CODE,
            NULL AS EVENT_CLASS_NAME,
            NULL AS EVENT_TYPE_CODE,
            NULL AS EVENT_TYPE_NAME,
            gjb_papf.NAME AS GL_BATCH_NAME,
            gjb_papf.POSTED_DATE,
            gjl_gjh.je_header_id,
            gjl_gjh.NAME AS GL_JE_NAME,
            gjl_gjh.ACCRUAL_REV_PERIOD_NAME AS REVERSAL_PERIOD,
            gjl_gjh.ACCRUAL_REV_STATUS AS REVERSAL_STATUS,
            gjl_gjh.external_reference AS EXTERNAL_REFERENCE,
            gjl_gjh.GL_LINE_NUMBER,
            gjl_gjh.effective_date,
            gjl_gjh.LINE_NUMBER,
            gjl_gjh.ORIG_LINE_NUMBER,
            NULL AS ACCOUNTING_CLASS_CODE,
            NULL AS ACCOUNTING_CLASS_NAME,
            gjl_gjh.LINE_DESCRIPTION,
            gjl_gjh.currency_code AS ENTERED_CURRENCY,
            gjl_gjh.currency_conversion_rate AS CONVERSION_RATE,
            gjl_gjh.currency_conversion_date AS CONVERSION_RATE_DATE,
            gjl_gjh.currency_conversion_type AS CONVERSION_RATE_TYPE_CODE,
            gdct.user_conversion_type AS CONVERSION_RATE_TYPE,
            gjl_gjh.entered_dr AS ENTERED_DR,
            gjl_gjh.entered_cr AS ENTERED_CR,
            NULL AS UNROUNDED_ACCOUNTED_DR,
            NULL AS UNROUNDED_ACCOUNTED_CR,
            gjl_gjh.accounted_dr AS ACCOUNTED_DR,
            gjl_gjh.accounted_cr AS ACCOUNTED_CR,
            gjl_gjh.stat_amount AS STATISTICAL_AMOUNT,
            gjl_gjh.jgzz_recon_ref_11i AS RECONCILIATION_REFERENCE,
            NULL AS PARTY_TYPE_CODE,
            NULL AS PARTY_TYPE,
            glbgt.ledger_id AS LEDGER_ID,
            glbgt.ledger_short_name AS LEDGER_SHORT_NAME,
            glbgt.ledger_description AS LEDGER_DESCRIPTION,
            glbgt.ledger_name AS LEDGER_NAME,
            glbgt.ledger_currency AS LEDGER_CURRENCY,
            glbgt.period_year AS PERIOD_YEAR,
            glbgt.period_number AS PERIOD_NUMBER,
            glbgt.period_name AS PERIOD_NAME,
            glbgt.period_start_date,
            glbgt.period_end_date,
            glbgt.balance_type_code AS BALANCE_TYPE_CODE,
            glbgt.balance_type AS BALANCE_TYPE,
            glbgt.budget_name AS BUDGET_NAME,
            glbgt.encumbrance_type AS ENCUMBRANCE_TYPE,
            glbgt.begin_balance_dr AS BEGIN_BALANCE_DR,
            glbgt.begin_balance_cr AS BEGIN_BALANCE_CR,
            glbgt.period_net_dr AS PERIOD_NET_DR,
            glbgt.period_net_cr AS PERIOD_NET_CR,
            glbgt.code_combination_id AS CODE_COMBINATION_ID,
            CAST('NA' AS STRING) AS ACCOUNTING_CODE_COMBINATION,
            CAST('NA' AS STRING) AS CODE_COMBINATION_DESCRIPTION,
            glbgt.control_account_flag AS CONTROL_ACCOUNT_FLAG,
            glbgt.control_account AS CONTROL_ACCOUNT,
            CAST('NA' AS STRING) AS BALANCING_SEGMENT,
            CAST('NA' AS STRING) AS NATURAL_ACCOUNT_SEGMENT,
            CAST('NA' AS STRING) AS COST_CENTER_SEGMENT,
            CAST('NA' AS STRING) AS MANAGEMENT_SEGMENT,
            CAST('NA' AS STRING) AS INTERCOMPANY_SEGMENT,
            CAST('NA' AS STRING) AS BALANCING_SEGMENT_DESC,
            CAST('NA' AS STRING) AS NATURAL_ACCOUNT_DESC,
            CAST('NA' AS STRING) AS COST_CENTER_DESC,
            CAST('NA' AS STRING) AS MANAGEMENT_SEGMENT_DESC,
            CAST('NA' AS STRING) AS INTERCOMPANY_SEGMENT_DESC,
            glbgt.segment1 AS SEGMENT1,
            glbgt.segment2 AS SEGMENT2,
            glbgt.segment3 AS SEGMENT3,
            glbgt.segment4 AS SEGMENT4,
            glbgt.segment5 AS SEGMENT5,
            glbgt.segment6 AS SEGMENT6,
            glbgt.segment7 AS SEGMENT7,
            glbgt.segment8 AS SEGMENT8,
            glbgt.segment9 AS SEGMENT9,
            glbgt.segment10 AS SEGMENT10,
            CAST('NA' AS STRING) AS BEGIN_RUNNING_TOTAL_CR,
            CAST('NA' AS STRING) AS BEGIN_RUNNING_TOTAL_DR,
            CAST('NA' AS STRING) AS END_RUNNING_TOTAL_CR,
            CAST('NA' AS STRING) AS END_RUNNING_TOTAL_DR,
            CAST('NA' AS STRING) AS LEGAL_ENTITY_ID,
            CAST('NA' AS STRING) AS LEGAL_ENTITY_NAME,
            CAST('NA' AS STRING) AS LE_ADDRESS_LINE_1,
            CAST('NA' AS STRING) AS LE_ADDRESS_LINE_2,
            CAST('NA' AS STRING) AS LE_ADDRESS_LINE_3,
            CAST('NA' AS STRING) AS LE_CITY,
            CAST('NA' AS STRING) AS LE_REGION_1,
            CAST('NA' AS STRING) AS LE_REGION_2,
            CAST('NA' AS STRING) AS LE_REGION_3,
            CAST('NA' AS STRING) AS LE_POSTAL_CODE,
            CAST('NA' AS STRING) AS LE_COUNTRY,
            CAST('NA' AS STRING) AS LE_REGISTRATION_NUMBER,
            CAST('NA' AS STRING) AS LE_REGISTRATION_EFFECTIVE_FROM,
            CAST('NA' AS STRING) AS LE_BR_DAILY_INSCRIPTION_NUMBER,
            CAST(NULL AS TIMESTAMP) AS LE_BR_DAILY_INSCRIPTION_DATE,
            CAST('NA' AS STRING) AS LE_BR_DAILY_ENTITY,
            CAST('NA' AS STRING) AS LE_BR_DAILY_LOCATION,
            CAST('NA' AS STRING) AS LE_BR_DIRECTOR_NUMBER,
            CAST('NA' AS STRING) AS LE_BR_ACCOUNTANT_NUMBE,
            CAST('NA' AS STRING) AS LE_BR_ACCOUNTANT_NAME,
            gjl_gjh.attribute1 AS BE_PROJECT_NO,
            NULL AS BE_PROJECT_DESC,
            gjl_gjh.attribute2 AS BE_TASK_NO,
            NULL AS BE_TASK_DESC,
            gjb_papf.full_name AS approver_name,
            NULL AS PO_NUMBER,
            NULL AS invoice_num,
            NULL AS INVOICE_ID,
            NULL AS vendor_id,
            NULL AS vendor_site_id,
            glbgt.TRANSLATED_FLAG,
            0 AS TEMP_LINE_NUM
          FROM gold_bec_dwh.FACT_XLA_REPORT_BALANCES_STG AS glbgt, gjl_gjh, silver_bec_ods.fun_seq_versions AS fsv1, silver_bec_ods.fun_seq_versions AS fsv2, gjst, gdct, gjb_papf
          WHERE
            gjl_gjh.ledger_id = glbgt.ledger_id
            AND gjl_gjh.code_combination_id = glbgt.code_combination_id
            AND (
              gjl_gjh.effective_date BETWEEN glbgt.period_start_date AND glbgt.period_end_date
            )
            AND gjl_gjh.period_name = glbgt.period_name
            AND gjl_gjh.actual_flag = glbgt.balance_type_code
            AND CASE
              WHEN gjl_gjh.currency_code = 'STAT'
              THEN gjl_gjh.currency_code
              ELSE glbgt.ledger_currency
            END = glbgt.ledger_currency
            AND COALESCE(gjl_gjh.budget_version_id, -19999) = COALESCE(glbgt.budget_version_id, -19999)
            AND COALESCE(gjl_gjh.encumbrance_type_id, -19999) = COALESCE(glbgt.encumbrance_type_id, -19999)
            AND gjb_papf.je_batch_id = gjl_gjh.je_batch_id /* AND   fdu.user_id                      = gjl.created_by --change */
            AND fsv1.SEQ_VERSION_ID() = gjl_gjh.posting_acct_seq_version_id
            AND fsv2.SEQ_VERSION_ID() = gjl_gjh.close_acct_seq_version_id /*       AND   --gjct.je_category_name            = gjh.je_category */
            AND gjst.je_source_name = gjl_gjh.je_source
            AND gdct.CONVERSION_TYPE() = gjl_gjh.currency_conversion_type
        )
        UNION ALL
        (
          SELECT
            gjh.default_effective_date AS GL_DATE,
            gjl.created_by AS created_by_id,
            gjh.creation_date AS CREATION_DATE,
            gjh.last_update_date AS LAST_UPDATE_DATE,
            CAST(NULL AS TIMESTAMP) AS GL_TRANSFER_DATE,
            gjh.reference_date AS REFERENCE_DATE,
            CAST(NULL AS TIMESTAMP) AS COMPLETED_DATE,
            NULL AS TRANSACTION_NUMBER,
            CAST(NULL AS TIMESTAMP) AS TRANSACTION_DATE,
            fsv1.header_name AS ACCOUNTING_SEQUENCE_NAME,
            fsv1.version_name AS ACCOUNTING_SEQUENCE_VERSION,
            gjh.posting_acct_seq_value AS ACCOUNTING_SEQUENCE_NUMBER,
            fsv2.header_name AS REPORTING_SEQUENCE_NAME,
            fsv2.version_name AS REPORTING_SEQUENCE_VERSION,
            gjh.close_acct_seq_value AS REPORTING_SEQUENCE_NUMBER,
            NULL AS DOCUMENT_CATEGORY,
            NULL AS DOCUMENT_SEQUENCE_NAME,
            NULL AS DOCUMENT_SEQUENCE_NUMBER,
            NULL AS APPLICATION_ID,
            NULL AS APPLICATION_NAME,
            NULL AS HEADER_ID,
            gjh.description AS HEADER_DESCRIPTION,
            NULL AS FUND_STATUS,
            gjh.je_category,
            gjst.user_je_source_name AS JE_SOURCE_NAME,
            NULL AS EVENT_ID,
            CAST(NULL AS TIMESTAMP) AS EVENT_DATE,
            NULL AS EVENT_NUMBER,
            NULL AS EVENT_CLASS_CODE,
            NULL AS EVENT_CLASS_NAME,
            NULL AS EVENT_TYPE_CODE,
            NULL AS EVENT_TYPE_NAME,
            gjb.NAME AS GL_BATCH_NAME,
            gjb.posted_date,
            gjh.je_header_id,
            gjh.NAME AS GL_JE_NAME,
            gjh.ACCRUAL_REV_PERIOD_NAME AS REVERSAL_PERIOD,
            gjh.ACCRUAL_REV_STATUS AS REVERSAL_STATUS,
            gjh.external_reference AS EXTERNAL_REFERENCE,
            gjl.je_line_num AS GL_LINE_NUMBER,
            gjl.effective_date,
            gjl.je_line_num AS LINE_NUMBER,
            gjl.je_line_num AS ORIG_LINE_NUMBER,
            NULL AS ACCOUNTING_CLASS_CODE,
            NULL AS ACCOUNTING_CLASS_NAME,
            gjl.description AS LINE_DESCRIPTION,
            gjh.currency_code AS ENTERED_CURRENCY,
            gjh.currency_conversion_rate AS CONVERSION_RATE,
            gjh.currency_conversion_date AS CONVERSION_RATE_DATE,
            gjh.currency_conversion_type AS CONVERSION_RATE_TYPE_CODE,
            gdct.user_conversion_type AS CONVERSION_RATE_TYPE,
            gjl.entered_dr AS ENTERED_DR,
            gjl.entered_cr AS ENTERED_CR,
            NULL AS UNROUNDED_ACCOUNTED_DR,
            NULL AS UNROUNDED_ACCOUNTED_CR,
            gjl.accounted_dr AS ACCOUNTED_DR,
            gjl.accounted_cr AS ACCOUNTED_CR,
            gjl.stat_amount AS STATISTICAL_AMOUNT,
            gjl.jgzz_recon_ref_11i AS RECONCILIATION_REFERENCE,
            NULL AS PARTY_TYPE_CODE,
            NULL AS PARTY_TYPE,
            glbgt.ledger_id AS LEDGER_ID,
            glbgt.ledger_short_name AS LEDGER_SHORT_NAME,
            glbgt.ledger_description AS LEDGER_DESCRIPTION,
            glbgt.ledger_name AS LEDGER_NAME,
            glbgt.ledger_currency AS LEDGER_CURRENCY,
            glbgt.period_year AS PERIOD_YEAR,
            glbgt.period_number AS PERIOD_NUMBER,
            glbgt.period_name AS PERIOD_NAME,
            glbgt.period_start_date,
            glbgt.period_end_date,
            glbgt.balance_type_code AS BALANCE_TYPE_CODE,
            glbgt.balance_type AS BALANCE_TYPE,
            glbgt.budget_name AS BUDGET_NAME,
            glbgt.encumbrance_type AS ENCUMBRANCE_TYPE,
            glbgt.begin_balance_dr AS BEGIN_BALANCE_DR,
            glbgt.begin_balance_cr AS BEGIN_BALANCE_CR,
            glbgt.period_net_dr AS PERIOD_NET_DR,
            glbgt.period_net_cr AS PERIOD_NET_CR,
            glbgt.code_combination_id AS CODE_COMBINATION_ID,
            CAST('NA' AS STRING) AS ACCOUNTING_CODE_COMBINATION,
            CAST('NA' AS STRING) AS CODE_COMBINATION_DESCRIPTION,
            glbgt.control_account_flag AS CONTROL_ACCOUNT_FLAG,
            glbgt.control_account AS CONTROL_ACCOUNT,
            CAST('NA' AS STRING) AS BALANCING_SEGMENT,
            CAST('NA' AS STRING) AS NATURAL_ACCOUNT_SEGMENT,
            CAST('NA' AS STRING) AS COST_CENTER_SEGMENT,
            CAST('NA' AS STRING) AS MANAGEMENT_SEGMENT,
            CAST('NA' AS STRING) AS INTERCOMPANY_SEGMENT,
            CAST('NA' AS STRING) AS BALANCING_SEGMENT_DESC,
            CAST('NA' AS STRING) AS NATURAL_ACCOUNT_DESC,
            CAST('NA' AS STRING) AS COST_CENTER_DESC,
            CAST('NA' AS STRING) AS MANAGEMENT_SEGMENT_DESC,
            CAST('NA' AS STRING) AS INTERCOMPANY_SEGMENT_DESC,
            glbgt.segment1 AS SEGMENT1,
            glbgt.segment2 AS SEGMENT2,
            glbgt.segment3 AS SEGMENT3,
            glbgt.segment4 AS SEGMENT4,
            glbgt.segment5 AS SEGMENT5,
            glbgt.segment6 AS SEGMENT6,
            glbgt.segment7 AS SEGMENT7,
            glbgt.segment8 AS SEGMENT8,
            glbgt.segment9 AS SEGMENT9,
            glbgt.segment10 AS SEGMENT10,
            CAST('NA' AS STRING) AS BEGIN_RUNNING_TOTAL_CR,
            CAST('NA' AS STRING) AS BEGIN_RUNNING_TOTAL_DR,
            CAST('NA' AS STRING) AS END_RUNNING_TOTAL_CR,
            CAST('NA' AS STRING) AS END_RUNNING_TOTAL_DR,
            CAST('NA' AS STRING) AS LEGAL_ENTITY_ID,
            CAST('NA' AS STRING) AS LEGAL_ENTITY_NAME,
            CAST('NA' AS STRING) AS LE_ADDRESS_LINE_1,
            CAST('NA' AS STRING) AS LE_ADDRESS_LINE_2,
            CAST('NA' AS STRING) AS LE_ADDRESS_LINE_3,
            CAST('NA' AS STRING) AS LE_CITY,
            CAST('NA' AS STRING) AS LE_REGION_1,
            CAST('NA' AS STRING) AS LE_REGION_2,
            CAST('NA' AS STRING) AS LE_REGION_3,
            CAST('NA' AS STRING) AS LE_POSTAL_CODE,
            CAST('NA' AS STRING) AS LE_COUNTRY,
            CAST('NA' AS STRING) AS LE_REGISTRATION_NUMBER,
            CAST('NA' AS STRING) AS LE_REGISTRATION_EFFECTIVE_FROM,
            CAST('NA' AS STRING) AS LE_BR_DAILY_INSCRIPTION_NUMBER,
            CAST(NULL AS TIMESTAMP) AS LE_BR_DAILY_INSCRIPTION_DATE,
            CAST('NA' AS STRING) AS LE_BR_DAILY_ENTITY,
            CAST('NA' AS STRING) AS LE_BR_DAILY_LOCATION,
            CAST('NA' AS STRING) AS LE_BR_DIRECTOR_NUMBER,
            CAST('NA' AS STRING) AS LE_BR_ACCOUNTANT_NUMBER,
            CAST('NA' AS STRING) AS LE_BR_ACCOUNTANT_NAME,
            gjl.attribute1 AS BE_PROJECT_NO,
            NULL AS BE_PROJECT_DESC,
            gjl.attribute2 AS BE_TASK_NO,
            NULL AS BE_TASK_DESC,
            PAPF.FULL_NAME AS APPROVER_NAME,
            NULL AS PO_NUMBER,
            NULL AS invoice_num,
            NULL AS INVOICE_ID,
            NULL AS vendor_id,
            NULL AS vendor_site_id,
            glbgt.TRANSLATED_FLAG,
            0 AS TEMP_LINE_NUM
          FROM gold_bec_dwh.FACT_XLA_REPORT_BALANCES_STG AS glbgt, silver_bec_ods.fnd_new_messages AS fnm, silver_bec_ods.fun_seq_versions AS fsv1, silver_bec_ods.fun_seq_versions AS fsv2, silver_bec_ods.gl_je_sources_tl AS gjst, silver_bec_ods.gl_daily_conversion_types AS gdct, silver_bec_ods.gl_je_lines AS gjl, silver_bec_ods.gl_je_headers AS gjh, silver_bec_ods.gl_je_batches AS gjb, silver_bec_ods.PER_ALL_PEOPLE_F AS PAPF
          WHERE
            gjl.ledger_id = glbgt.ledger_id
            AND gjl.code_combination_id = glbgt.code_combination_id
            AND gjl.effective_date BETWEEN glbgt.period_start_date AND glbgt.period_end_date
            AND gjl.period_name = glbgt.period_name
            AND gjh.je_header_id = gjl.je_header_id
            AND gjh.actual_flag = glbgt.balance_type_code
            AND CASE
              WHEN gjh.currency_code = 'STAT'
              THEN gjh.currency_code
              ELSE glbgt.ledger_currency
            END = glbgt.ledger_currency /* added bug 6686541 */
            AND COALESCE(gjh.je_from_sla_flag, 'N') = 'U'
            AND fnm.application_id = 101
            AND fnm.language_code = 'US'
            AND fnm.message_name IN ('PPOS0220', 'PPOS0221', 'PPOS0222', 'PPOS0243', 'PPOS0222_G', 'PPOSO275')
            AND gjl.description = fnm.message_text
            AND COALESCE(gjh.budget_version_id, -19999) = COALESCE(glbgt.budget_version_id, -19999)
            AND COALESCE(gjh.encumbrance_type_id, -19999) = COALESCE(glbgt.encumbrance_type_id, -19999)
            AND gjb.je_batch_id = gjh.je_batch_id
            AND gjb.status = 'P'
            AND GJB.APPROVER_EMPLOYEE_ID = PAPF.PERSON_ID()
            AND gjb.creation_date BETWEEN papf.EFFECTIVE_START_DATE() AND COALESCE(papf.EFFECTIVE_END_DATE(), CURRENT_TIMESTAMP() + 1) /*       AND   fdu.user_id                      = gjl.created_by  --change */
            AND fsv1.SEQ_VERSION_ID() = gjh.posting_acct_seq_version_id
            AND fsv2.SEQ_VERSION_ID() = gjh.close_acct_seq_version_id /* AND   gjct.je_category_name            = gjh.je_category */
            AND gjst.je_source_name = gjh.je_source
            AND gjst.language = 'US'
            AND gdct.CONVERSION_TYPE() = gjh.currency_conversion_type
            AND NOT EXISTS(
              SELECT
                'x'
              FROM silver_bec_ods.gl_import_references AS gir
              WHERE
                gir.je_header_id = gjl.je_header_id AND gir.je_line_num = gjl.je_line_num
            )
            AND (
              gjl.kca_seq_date >= (
                SELECT
                  (
                    executebegints - prune_days
                  )
                FROM bec_etl_ctrl.batch_dw_info
                WHERE
                  dw_table_name = 'fact_gl_account_analysis' AND batch_name = 'gl'
              )
              OR gjh.kca_seq_date >= (
                SELECT
                  (
                    executebegints - prune_days
                  )
                FROM bec_etl_ctrl.batch_dw_info
                WHERE
                  dw_table_name = 'fact_gl_account_analysis' AND batch_name = 'gl'
              )
              OR gjb.kca_seq_date >= (
                SELECT
                  (
                    executebegints - prune_days
                  )
                FROM bec_etl_ctrl.batch_dw_info
                WHERE
                  dw_table_name = 'fact_gl_account_analysis' AND batch_name = 'gl'
              )
              OR fnm.is_deleted_flg = 'Y'
              OR fsv1.is_deleted_flg = 'Y'
              OR fsv2.is_deleted_flg = 'Y'
              OR gjst.is_deleted_flg = 'Y'
              OR gdct.is_deleted_flg = 'Y'
              OR gjb.is_deleted_flg = 'Y'
              OR gjl.is_deleted_flg = 'Y'
              OR gjh.is_deleted_flg = 'Y'
              OR gjb.is_deleted_flg = 'Y'
              OR PAPF.is_deleted_flg = 'Y'
            )
        )
      ) AS A
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.LINE_NUMBER, 0) || '-' || COALESCE(ods.HEADER_ID, 0) || '-' || COALESCE(ods.LEDGER_CURRENCY, 'NA') || '-' || COALESCE(ods.TRANSLATED_FLAG, 'NA') || '-' || COALESCE(ods.TEMP_LINE_NUM, 0)
  );
/* Insert records */
WITH CTE_PO_AP AS (
  SELECT
    pa1.segment1,
    pa1.name,
    pa1.description AS BE_PROJECT_DESC,
    pt1.task_number,
    pt1.description AS BE_TASK_DESC,
    PHA.SEGMENT1 AS PO_NUMBER,
    AIA.invoice_num,
    AIA.INVOICE_ID,
    AIA.vendor_id,
    AIA.vendor_site_id,
    apd1.invoice_distribution_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.ap_invoice_distributions_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS apd1, (
    SELECT
      *
    FROM silver_bec_ods.po_distributions_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS pda, (
    SELECT
      *
    FROM silver_bec_ods.AP_INVOICES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS AIA, (
    SELECT
      *
    FROM silver_bec_ods.po_headers_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS pha, (
    SELECT
      *
    FROM silver_bec_ods.pa_projects_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS pa1, (
    SELECT
      *
    FROM silver_bec_ods.pa_tasks
    WHERE
      is_deleted_flg <> 'Y'
  ) AS pt1
  WHERE
    1 = 1
    AND pt1.TASK_ID() = apd1.task_id
    AND apd1.INVOICE_ID = AIA.INVOICE_ID
    AND pa1.PROJECT_ID() = apd1.project_id
    AND PDA.PO_DISTRIBUTION_ID() = apd1.PO_DISTRIBUTION_ID
    AND PHA.PO_HEADER_ID() = PDA.PO_HEADER_ID
    AND (
      apd1.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_gl_account_analysis' AND batch_name = 'gl'
      )
      OR AIA.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_gl_account_analysis' AND batch_name = 'gl'
      )
    )
), gjl_gjh AS (
  SELECT
    gjh.default_effective_date,
    gjh.creation_date,
    gjh.last_update_date,
    gjh.reference_date,
    gjh.posting_acct_seq_value,
    gjh.close_acct_seq_value,
    gjh.je_header_id AS header_id,
    gjh.description AS HEADER_DESCRIPTION,
    gjh.je_category,
    gjh.je_header_id,
    gjh.NAME,
    gjh.ACCRUAL_REV_PERIOD_NAME,
    gjh.ACCRUAL_REV_STATUS,
    gjh.external_reference,
    gjh.currency_code,
    gjh.currency_conversion_rate,
    gjh.currency_conversion_date,
    gjh.currency_conversion_type,
    gjh.actual_flag,
    gjh.je_from_sla_flag,
    gjh.budget_version_id,
    gjh.encumbrance_type_id,
    gjh.je_batch_id,
    gjh.posting_acct_seq_version_id,
    gjh.close_acct_seq_version_id,
    gjh.je_source,
    gjl.ledger_id,
    gjl.code_combination_id,
    gjl.period_name,
    gjl.created_by,
    gjl.je_line_num AS GL_LINE_NUMBER,
    gjl.effective_date,
    gjl.je_line_num AS LINE_NUMBER,
    gjl.je_line_num AS ORIG_LINE_NUMBER,
    gjl.description AS LINE_DESCRIPTION,
    gjl.entered_dr,
    gjl.entered_cr,
    gjl.accounted_dr,
    gjl.accounted_cr,
    gjl.stat_amount,
    gjl.jgzz_recon_ref_11i,
    gjl.attribute1,
    gjl.attribute2
  FROM (
    SELECT
      *
    FROM silver_bec_ods.gl_je_lines
    WHERE
      is_deleted_flg <> 'Y'
  ) AS gjl, (
    SELECT
      *
    FROM silver_bec_ods.gl_je_headers
    WHERE
      is_deleted_flg <> 'Y'
  ) AS gjh
  WHERE
    gjh.je_header_id = gjl.je_header_id
    AND gjl.last_update_date >= TO_DATE('2020-01-01 12:00:00', 'YYYY-MM-DD HH:MI:SS')
    AND gjh.last_update_date >= TO_DATE('2020-01-01 12:00:00', 'YYYY-MM-DD HH:MI:SS')
    AND (
      gjl.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_gl_account_analysis' AND batch_name = 'gl'
      )
      OR gjh.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_gl_account_analysis' AND batch_name = 'gl'
      )
    )
    AND COALESCE(gjh.je_from_sla_flag, 'N') = 'N'
), gjst /* -----------------------------------------------------------------------------------------------------------  */ AS (
  SELECT
    gjst.user_je_source_name AS user_je_source_name,
    gjst.je_source_name AS je_source_name,
    gjst.language AS `language`
  FROM (
    SELECT
      *
    FROM silver_bec_ods.gl_je_sources_tl
    WHERE
      is_deleted_flg <> 'Y'
  ) AS gjst
  WHERE
    gjst.language = 'US'
), gdct /* ----------------------------------------------------------------------------------------------------------- */ AS (
  SELECT
    gdct.user_conversion_type,
    gdct.conversion_type
  FROM (
    SELECT
      *
    FROM silver_bec_ods.gl_daily_conversion_types
    WHERE
      is_deleted_flg <> 'Y'
  ) AS gdct
), gjb_papf /* ----------------------------------------------------------------------------------------------------------- */ AS (
  SELECT
    gjb.je_batch_id,
    gjb.NAME,
    gjb.status,
    gjb.APPROVER_EMPLOYEE_ID,
    gjb.creation_date,
    gjb.posted_date,
    papf.person_id,
    papf.full_name,
    papf.effective_start_date,
    papf.effective_end_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.gl_je_batches
    WHERE
      is_deleted_flg <> 'Y'
  ) AS gjb, (
    SELECT
      *
    FROM silver_bec_ods.per_all_people_f
    WHERE
      is_deleted_flg <> 'Y'
  ) AS papf
  WHERE
    gjb.APPROVER_EMPLOYEE_ID = papf.PERSON_ID()
    AND gjb.creation_date BETWEEN papf.EFFECTIVE_START_DATE() AND COALESCE(papf.EFFECTIVE_END_DATE(), CURRENT_TIMESTAMP() + 1)
    AND gjb.status = 'P'
    AND (
      gjb.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_gl_account_analysis' AND batch_name = 'gl'
      )
    )
) /* ----------------------------------------------------------------------------------------------------------- */
INSERT INTO gold_bec_dwh.FACT_GL_ACCOUNT_ANALYSIS (
  GL_DATE,
  created_by_id,
  creation_date,
  last_update_date,
  GL_TRANSFER_DATE,
  reference_date,
  COMPLETED_DATE,
  TRANSACTION_NUMBER,
  TRANSACTION_DATE,
  ACCOUNTING_SEQUENCE_NAME,
  ACCOUNTING_SEQUENCE_VERSION,
  ACCOUNTING_SEQUENCE_NUMBER,
  REPORTING_SEQUENCE_NAME,
  REPORTING_SEQUENCE_VERSION,
  REPORTING_SEQUENCE_NUMBER,
  DOCUMENT_CATEGORY,
  DOCUMENT_SEQUENCE_NAME,
  DOCUMENT_SEQUENCE_NUMBER,
  APPLICATION_ID,
  APPLICATION_NAME,
  HEADER_ID,
  HEADER_DESCRIPTION,
  FUND_STATUS,
  je_category,
  JE_SOURCE_NAME,
  EVENT_ID,
  EVENT_DATE,
  EVENT_NUMBER,
  EVENT_CLASS_CODE,
  EVENT_CLASS_NAME,
  EVENT_TYPE_CODE,
  EVENT_TYPE_NAME,
  GL_BATCH_NAME,
  POSTED_DATE,
  je_header_id,
  GL_JE_NAME,
  REVERSAL_PERIOD,
  REVERSAL_STATUS,
  EXTERNAL_REFERENCE,
  GL_LINE_NUMBER,
  effective_date,
  LINE_NUMBER,
  ORIG_LINE_NUMBER,
  ACCOUNTING_CLASS_CODE,
  ACCOUNTING_CLASS_NAME,
  LINE_DESCRIPTION,
  ENTERED_CURRENCY,
  CONVERSION_RATE,
  CONVERSION_RATE_DATE,
  CONVERSION_RATE_TYPE_CODE,
  CONVERSION_RATE_TYPE,
  ENTERED_DR,
  ENTERED_CR,
  UNROUNDED_ACCOUNTED_DR,
  UNROUNDED_ACCOUNTED_CR,
  ACCOUNTED_DR,
  ACCOUNTED_CR,
  STATISTICAL_AMOUNT,
  RECONCILIATION_REFERENCE,
  PARTY_TYPE_CODE,
  PARTY_TYPE,
  LEDGER_ID,
  LEDGER_SHORT_NAME,
  LEDGER_DESCRIPTION,
  LEDGER_NAME,
  LEDGER_CURRENCY,
  PERIOD_YEAR,
  PERIOD_NUMBER,
  PERIOD_NAME,
  PERIOD_START_DATE,
  PERIOD_END_DATE,
  BALANCE_TYPE_CODE,
  BALANCE_TYPE,
  BUDGET_NAME,
  ENCUMBRANCE_TYPE,
  BEGIN_BALANCE_DR,
  BEGIN_BALANCE_CR,
  PERIOD_NET_DR,
  PERIOD_NET_CR,
  CODE_COMBINATION_ID,
  ACCOUNTING_CODE_COMBINATION,
  CODE_COMBINATION_DESCRIPTION,
  CONTROL_ACCOUNT_FLAG,
  CONTROL_ACCOUNT,
  BALANCING_SEGMENT,
  NATURAL_ACCOUNT_SEGMENT,
  COST_CENTER_SEGMENT,
  MANAGEMENT_SEGMENT,
  INTERCOMPANY_SEGMENT,
  BALANCING_SEGMENT_DESC,
  NATURAL_ACCOUNT_DESC,
  COST_CENTER_DESC,
  MANAGEMENT_SEGMENT_DESC,
  INTERCOMPANY_SEGMENT_DESC,
  segment1,
  segment2,
  segment3,
  segment4,
  segment5,
  segment6,
  segment7,
  segment8,
  segment9,
  segment10,
  BEGIN_RUNNING_TOTAL_CR,
  BEGIN_RUNNING_TOTAL_DR,
  END_RUNNING_TOTAL_CR,
  END_RUNNING_TOTAL_DR,
  LEGAL_ENTITY_ID,
  LEGAL_ENTITY_NAME,
  LE_ADDRESS_LINE_1,
  LE_ADDRESS_LINE_2,
  LE_ADDRESS_LINE_3,
  LE_CITY,
  LE_REGION_1,
  LE_REGION_2,
  LE_REGION_3,
  LE_POSTAL_CODE,
  LE_COUNTRY,
  LE_REGISTRATION_NUMBER,
  LE_REGISTRATION_EFFECTIVE_FROM,
  LE_BR_DAILY_INSCRIPTION_NUMBER,
  LE_BR_DAILY_INSCRIPTION_DATE,
  LE_BR_DAILY_ENTITY,
  LE_BR_DAILY_LOCATION,
  LE_BR_DIRECTOR_NUMBER,
  LE_BR_ACCOUNTANT_NUMBER,
  LE_BR_ACCOUNTANT_NAME,
  BE_PROJECT_NO,
  BE_PROJECT_DESC,
  BE_TASK_NO,
  BE_TASK_DESC,
  approver_name,
  PO_NUMBER,
  invoice_num,
  INVOICE_ID,
  vendor_id,
  vendor_site_id,
  TRANSLATED_FLAG,
  TEMP_LINE_NUM,
  je_category_KEY,
  je_header_id_KEY,
  LEDGER_ID_KEY,
  CODE_COMBINATION_ID_KEY,
  LEGAL_ENTITY_ID_KEY,
  INVOICE_ID_KEY,
  vendor_id_KEY,
  Update_flg,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT DISTINCT
    A.GL_DATE,
    A.created_by_id,
    A.creation_date,
    A.last_update_date,
    A.GL_TRANSFER_DATE,
    A.reference_date,
    A.COMPLETED_DATE,
    A.TRANSACTION_NUMBER,
    A.TRANSACTION_DATE,
    A.ACCOUNTING_SEQUENCE_NAME,
    A.ACCOUNTING_SEQUENCE_VERSION,
    A.ACCOUNTING_SEQUENCE_NUMBER,
    A.REPORTING_SEQUENCE_NAME,
    A.REPORTING_SEQUENCE_VERSION,
    A.REPORTING_SEQUENCE_NUMBER,
    A.DOCUMENT_CATEGORY,
    A.DOCUMENT_SEQUENCE_NAME,
    A.DOCUMENT_SEQUENCE_NUMBER,
    A.APPLICATION_ID,
    A.APPLICATION_NAME,
    A.HEADER_ID,
    A.HEADER_DESCRIPTION,
    A.FUND_STATUS,
    A.je_category,
    A.JE_SOURCE_NAME,
    A.EVENT_ID,
    A.EVENT_DATE,
    A.EVENT_NUMBER,
    A.EVENT_CLASS_CODE,
    A.EVENT_CLASS_NAME,
    A.EVENT_TYPE_CODE,
    A.EVENT_TYPE_NAME,
    A.GL_BATCH_NAME,
    A.POSTED_DATE,
    A.je_header_id,
    A.GL_JE_NAME,
    A.REVERSAL_PERIOD,
    A.REVERSAL_STATUS,
    A.EXTERNAL_REFERENCE,
    A.GL_LINE_NUMBER,
    A.effective_date,
    A.LINE_NUMBER,
    A.ORIG_LINE_NUMBER,
    A.ACCOUNTING_CLASS_CODE,
    A.ACCOUNTING_CLASS_NAME,
    A.LINE_DESCRIPTION,
    A.ENTERED_CURRENCY,
    A.CONVERSION_RATE,
    A.CONVERSION_RATE_DATE,
    A.CONVERSION_RATE_TYPE_CODE,
    A.CONVERSION_RATE_TYPE,
    A.ENTERED_DR,
    A.ENTERED_CR,
    A.UNROUNDED_ACCOUNTED_DR,
    A.UNROUNDED_ACCOUNTED_CR,
    A.ACCOUNTED_DR,
    A.ACCOUNTED_CR,
    A.STATISTICAL_AMOUNT,
    A.RECONCILIATION_REFERENCE,
    A.PARTY_TYPE_CODE,
    A.PARTY_TYPE,
    A.LEDGER_ID,
    A.LEDGER_SHORT_NAME,
    A.LEDGER_DESCRIPTION,
    A.LEDGER_NAME,
    A.LEDGER_CURRENCY,
    A.PERIOD_YEAR,
    A.PERIOD_NUMBER,
    A.PERIOD_NAME,
    A.PERIOD_START_DATE,
    A.PERIOD_END_DATE,
    A.BALANCE_TYPE_CODE,
    A.BALANCE_TYPE,
    A.BUDGET_NAME,
    A.ENCUMBRANCE_TYPE,
    A.BEGIN_BALANCE_DR,
    A.BEGIN_BALANCE_CR,
    A.PERIOD_NET_DR,
    A.PERIOD_NET_CR,
    A.CODE_COMBINATION_ID,
    A.ACCOUNTING_CODE_COMBINATION,
    A.CODE_COMBINATION_DESCRIPTION,
    A.CONTROL_ACCOUNT_FLAG,
    A.CONTROL_ACCOUNT,
    A.BALANCING_SEGMENT,
    A.NATURAL_ACCOUNT_SEGMENT,
    A.COST_CENTER_SEGMENT,
    A.MANAGEMENT_SEGMENT,
    A.INTERCOMPANY_SEGMENT,
    A.BALANCING_SEGMENT_DESC,
    A.NATURAL_ACCOUNT_DESC,
    A.COST_CENTER_DESC,
    A.MANAGEMENT_SEGMENT_DESC,
    A.INTERCOMPANY_SEGMENT_DESC,
    A.segment1,
    A.segment2,
    A.segment3,
    A.segment4,
    A.segment5,
    A.segment6,
    A.segment7,
    A.segment8,
    A.segment9,
    A.segment10,
    A.BEGIN_RUNNING_TOTAL_CR,
    A.BEGIN_RUNNING_TOTAL_DR,
    A.END_RUNNING_TOTAL_CR,
    A.END_RUNNING_TOTAL_DR,
    A.LEGAL_ENTITY_ID,
    A.LEGAL_ENTITY_NAME,
    A.LE_ADDRESS_LINE_1,
    A.LE_ADDRESS_LINE_2,
    A.LE_ADDRESS_LINE_3,
    A.LE_CITY,
    A.LE_REGION_1,
    A.LE_REGION_2,
    A.LE_REGION_3,
    A.LE_POSTAL_CODE,
    A.LE_COUNTRY,
    A.LE_REGISTRATION_NUMBER,
    A.LE_REGISTRATION_EFFECTIVE_FROM,
    A.LE_BR_DAILY_INSCRIPTION_NUMBER,
    A.LE_BR_DAILY_INSCRIPTION_DATE,
    A.LE_BR_DAILY_ENTITY,
    A.LE_BR_DAILY_LOCATION,
    A.LE_BR_DIRECTOR_NUMBER,
    A.LE_BR_ACCOUNTANT_NUMBER,
    A.LE_BR_ACCOUNTANT_NAME,
    A.BE_PROJECT_NO,
    A.BE_PROJECT_DESC,
    A.BE_TASK_NO,
    A.BE_TASK_DESC,
    A.approver_name,
    A.PO_NUMBER,
    A.invoice_num,
    A.INVOICE_ID,
    A.vendor_id,
    A.vendor_site_id,
    A.TRANSLATED_FLAG,
    A.TEMP_LINE_NUM,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || A.je_category AS je_category_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || A.je_header_id AS je_header_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || A.LEDGER_ID AS LEDGER_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || A.CODE_COMBINATION_ID AS CODE_COMBINATION_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || A.LEGAL_ENTITY_ID AS LEGAL_ENTITY_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || A.INVOICE_ID AS INVOICE_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || A.vendor_id AS vendor_id_KEY, /* audit columns */
    'Y' AS Update_flg,
    'N' AS is_deleted_flg,
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
    ) || '-' || COALESCE(A.LINE_NUMBER, 0) || '-' || COALESCE(A.HEADER_ID, 0) || '-' || COALESCE(A.LEDGER_CURRENCY, 'NA') || '-' || COALESCE(A.TRANSLATED_FLAG, 'NA') || '-' || COALESCE(A.TEMP_LINE_NUM, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    (
      SELECT
        XLA.GL_DATE,
        OTT.created_by_id,
        XLA.creation_date,
        XLA.last_update_date,
        XLA.GL_TRANSFER_DATE,
        XLA.reference_date,
        XLA.COMPLETED_DATE,
        XLA.TRANSACTION_NUMBER,
        XLA.TRANSACTION_DATE,
        XLA.ACCOUNTING_SEQUENCE_NAME,
        XLA.ACCOUNTING_SEQUENCE_VERSION,
        XLA.ACCOUNTING_SEQUENCE_NUMBER,
        XLA.REPORTING_SEQUENCE_NAME,
        XLA.REPORTING_SEQUENCE_VERSION,
        XLA.REPORTING_SEQUENCE_NUMBER,
        NULL AS DOCUMENT_CATEGORY,
        XLA.DOCUMENT_SEQUENCE_NAME,
        XLA.DOCUMENT_SEQUENCE_NUMBER,
        XLA.APPLICATION_ID,
        NULL AS APPLICATION_NAME,
        XLA.HEADER_ID,
        XLA.HEADER_DESCRIPTION,
        XLA.FUND_STATUS,
        XLA.je_category_name AS je_category,
        OTT.JE_SOURCE_NAME,
        XLA.EVENT_ID,
        XLA.EVENT_DATE,
        XLA.EVENT_NUMBER,
        XLA.EVENT_CLASS_CODE,
        XLA.EVENT_CLASS_NAME,
        XLA.EVENT_TYPE_CODE,
        XLA.EVENT_TYPE_NAME,
        OTT.GL_BATCH_NAME,
        OTT.POSTED_DATE,
        OTT.je_header_id,
        OTT.GL_JE_NAME,
        OTT.REVERSAL_PERIOD,
        OTT.REVERSAL_STATUS,
        OTT.EXTERNAL_REFERENCE,
        OTT.GL_LINE_NUMBER,
        OTT.effective_date,
        XLA.LINE_NUMBER,
        XLA.ORIG_LINE_NUMBER,
        XLA.ACCOUNTING_CLASS_CODE,
        XLA.ACCOUNTING_CLASS_NAME,
        XLA.LINE_DESCRIPTION,
        XLA.ENTERED_CURRENCY,
        XLA.CONVERSION_RATE,
        XLA.CONVERSION_RATE_DATE,
        XLA.CONVERSION_RATE_TYPE_CODE,
        XLA.CONVERSION_RATE_TYPE,
        XLA.ENTERED_DR,
        XLA.ENTERED_CR,
        XLA.UNROUNDED_ACCOUNTED_DR,
        XLA.UNROUNDED_ACCOUNTED_CR,
        XLA.ACCOUNTED_DR,
        XLA.ACCOUNTED_CR,
        XLA.STATISTICAL_AMOUNT,
        XLA.RECONCILIATION_REFERENCE,
        XLA.PARTY_TYPE_CODE,
        NULL AS PARTY_TYPE,
        OTT.LEDGER_ID,
        OTT.LEDGER_SHORT_NAME,
        OTT.LEDGER_DESCRIPTION,
        OTT.LEDGER_NAME,
        OTT.LEDGER_CURRENCY,
        OTT.PERIOD_YEAR,
        OTT.PERIOD_NUMBER,
        OTT.PERIOD_NAME,
        OTT.PERIOD_START_DATE,
        OTT.PERIOD_END_DATE,
        OTT.BALANCE_TYPE_CODE,
        OTT.BALANCE_TYPE,
        OTT.BUDGET_NAME,
        OTT.ENCUMBRANCE_TYPE,
        OTT.BEGIN_BALANCE_DR,
        OTT.BEGIN_BALANCE_CR,
        OTT.PERIOD_NET_DR,
        OTT.PERIOD_NET_CR,
        OTT.CODE_COMBINATION_ID,
        OTT.ACCOUNTING_CODE_COMBINATION,
        OTT.CODE_COMBINATION_DESCRIPTION,
        OTT.CONTROL_ACCOUNT_FLAG,
        OTT.CONTROL_ACCOUNT,
        OTT.BALANCING_SEGMENT,
        OTT.NATURAL_ACCOUNT_SEGMENT,
        OTT.COST_CENTER_SEGMENT,
        OTT.MANAGEMENT_SEGMENT,
        OTT.INTERCOMPANY_SEGMENT,
        OTT.BALANCING_SEGMENT_DESC,
        OTT.NATURAL_ACCOUNT_DESC,
        OTT.COST_CENTER_DESC,
        OTT.MANAGEMENT_SEGMENT_DESC,
        OTT.INTERCOMPANY_SEGMENT_DESC,
        OTT.segment1 AS SEGMENT1,
        OTT.segment2 AS SEGMENT2,
        OTT.segment3 AS SEGMENT3,
        OTT.segment4 AS SEGMENT4,
        OTT.segment5 AS SEGMENT5,
        OTT.segment6 AS SEGMENT6,
        OTT.segment7 AS SEGMENT7,
        OTT.segment8 AS SEGMENT8,
        OTT.segment9 AS SEGMENT9,
        OTT.segment10 AS SEGMENT10,
        OTT.BEGIN_RUNNING_TOTAL_CR,
        OTT.BEGIN_RUNNING_TOTAL_DR,
        OTT.END_RUNNING_TOTAL_CR,
        OTT.END_RUNNING_TOTAL_DR,
        OTT.LEGAL_ENTITY_ID,
        OTT.LEGAL_ENTITY_NAME,
        OTT.LE_ADDRESS_LINE_1,
        OTT.LE_ADDRESS_LINE_2,
        OTT.LE_ADDRESS_LINE_3,
        OTT.LE_CITY,
        OTT.LE_REGION_1,
        OTT.LE_REGION_2,
        OTT.LE_REGION_3,
        OTT.LE_POSTAL_CODE,
        OTT.LE_COUNTRY,
        OTT.LE_REGISTRATION_NUMBER,
        OTT.LE_REGISTRATION_EFFECTIVE_FROM,
        OTT.LE_BR_DAILY_INSCRIPTION_NUMBER,
        OTT.LE_BR_DAILY_INSCRIPTION_DATE,
        OTT.LE_BR_DAILY_ENTITY,
        OTT.LE_BR_DAILY_LOCATION,
        OTT.LE_BR_DIRECTOR_NUMBER,
        OTT.LE_BR_ACCOUNTANT_NUMBER,
        OTT.LE_BR_ACCOUNTANT_NAME,
        (
          CASE
            WHEN XLA.event_class_code = 'SALES_ORDER'
            AND OTT.user_je_source_name = 'Cost Management'
            AND XLA.ent_entity_code = 'MTL_ACCOUNTING_EVENTS'
            THEN COALESCE(XLA.order_Number, ' ')
            ELSE POAP.segment1 || ' - ' || POAP.name
          END
        ) AS BE_PROJECT_NO,
        POAP.BE_PROJECT_DESC,
        (
          CASE
            WHEN XLA.entity_code = 'TRANSACTIONS' AND OTT.user_je_source_name = 'Receivables'
            THEN COALESCE(XLA.REC_address1, '')
            WHEN XLA.event_class_code = 'ADJUSTMENT' AND OTT.user_je_source_name = 'Receivables'
            THEN COALESCE(XLA.ADJ_address1, '')
            WHEN XLA.event_class_code = 'SALES_ORDER'
            AND OTT.user_je_source_name = 'Cost Management'
            AND XLA.ent_entity_code = 'MTL_ACCOUNTING_EVENTS'
            THEN COALESCE(XLA.address1, ' ')
            ELSE POAP.task_number || ' '
          END
        ) AS BE_TASK_NO,
        POAP.BE_TASK_DESC,
        OTT.approver_name,
        (
          COALESCE(POAP.PO_NUMBER, ' ')
        ) AS PO_NUMBER,
        POAP.invoice_num,
        POAP.INVOICE_ID,
        POAP.vendor_id,
        POAP.vendor_site_id,
        OTT.TRANSLATED_FLAG,
        XLA.TEMP_LINE_NUM
      FROM gold_bec_dwh.FACT_GL_XLA_STG AS XLA, gold_bec_dwh.FACT_GL_JOURNAL_STG AS OTT, CTE_PO_AP AS POAP
      WHERE
        1 = 1
        AND OTT.gl_sl_link_id = XLA.gl_sl_link_id
        AND OTT.gl_sl_link_table = XLA.gl_sl_link_table
        AND XLA.balance_type_code = OTT.balance_type_code
        AND COALESCE(XLA.budget_version_id, -19999) = COALESCE(OTT.budget_version_id, -19999)
        AND COALESCE(XLA.encumbrance_type_id, -19999) = COALESCE(OTT.encumbrance_type_id, -19999)
        AND POAP.INVOICE_DISTRIBUTION_ID() = XLA.alloc_to_dist_id_num_1
    )
    UNION ALL
    (
      /* Main Query Starts Here */ /* ----------------------------------------------------------------------------------------------------------- */
      SELECT
        gjl_gjh.default_effective_date AS GL_DATE,
        gjl_gjh.created_by AS created_by_id,
        gjl_gjh.CREATION_DATE,
        gjl_gjh.LAST_UPDATE_DATE,
        CAST(NULL AS TIMESTAMP) AS GL_TRANSFER_DATE,
        gjl_gjh.REFERENCE_DATE,
        CAST(NULL AS TIMESTAMP) AS COMPLETED_DATE,
        NULL AS TRANSACTION_NUMBER,
        CAST(NULL AS TIMESTAMP) AS TRANSACTION_DATE,
        fsv1.header_name AS ACCOUNTING_SEQUENCE_NAME,
        fsv1.version_name AS ACCOUNTING_SEQUENCE_VERSION,
        gjl_gjh.posting_acct_seq_value AS ACCOUNTING_SEQUENCE_NUMBER,
        fsv2.header_name AS REPORTING_SEQUENCE_NAME,
        fsv2.version_name AS REPORTING_SEQUENCE_VERSION,
        gjl_gjh.close_acct_seq_value AS REPORTING_SEQUENCE_NUMBER,
        NULL AS DOCUMENT_CATEGORY,
        NULL AS DOCUMENT_SEQUENCE_NAME,
        NULL AS DOCUMENT_SEQUENCE_NUMBER,
        NULL AS APPLICATION_ID,
        NULL AS APPLICATION_NAME, /* 1 */
        gjl_gjh.HEADER_ID,
        gjl_gjh.HEADER_DESCRIPTION,
        NULL AS FUND_STATUS,
        gjl_gjh.je_category,
        gjst.user_je_source_name AS JE_SOURCE_NAME,
        NULL AS EVENT_ID,
        CAST(NULL AS TIMESTAMP) AS EVENT_DATE,
        NULL AS EVENT_NUMBER,
        NULL AS EVENT_CLASS_CODE,
        NULL AS EVENT_CLASS_NAME,
        NULL AS EVENT_TYPE_CODE,
        NULL AS EVENT_TYPE_NAME,
        gjb_papf.NAME AS GL_BATCH_NAME,
        gjb_papf.POSTED_DATE,
        gjl_gjh.je_header_id,
        gjl_gjh.NAME AS GL_JE_NAME,
        gjl_gjh.ACCRUAL_REV_PERIOD_NAME AS REVERSAL_PERIOD,
        gjl_gjh.ACCRUAL_REV_STATUS AS REVERSAL_STATUS,
        gjl_gjh.external_reference AS EXTERNAL_REFERENCE,
        gjl_gjh.GL_LINE_NUMBER,
        gjl_gjh.effective_date,
        gjl_gjh.LINE_NUMBER,
        gjl_gjh.ORIG_LINE_NUMBER,
        NULL AS ACCOUNTING_CLASS_CODE,
        NULL AS ACCOUNTING_CLASS_NAME,
        gjl_gjh.LINE_DESCRIPTION,
        gjl_gjh.currency_code AS ENTERED_CURRENCY,
        gjl_gjh.currency_conversion_rate AS CONVERSION_RATE,
        gjl_gjh.currency_conversion_date AS CONVERSION_RATE_DATE,
        gjl_gjh.currency_conversion_type AS CONVERSION_RATE_TYPE_CODE,
        gdct.user_conversion_type AS CONVERSION_RATE_TYPE,
        gjl_gjh.entered_dr AS ENTERED_DR,
        gjl_gjh.entered_cr AS ENTERED_CR,
        NULL AS UNROUNDED_ACCOUNTED_DR,
        NULL AS UNROUNDED_ACCOUNTED_CR,
        gjl_gjh.accounted_dr AS ACCOUNTED_DR,
        gjl_gjh.accounted_cr AS ACCOUNTED_CR,
        gjl_gjh.stat_amount AS STATISTICAL_AMOUNT,
        gjl_gjh.jgzz_recon_ref_11i AS RECONCILIATION_REFERENCE,
        NULL AS PARTY_TYPE_CODE,
        NULL AS PARTY_TYPE,
        glbgt.ledger_id AS LEDGER_ID,
        glbgt.ledger_short_name AS LEDGER_SHORT_NAME,
        glbgt.ledger_description AS LEDGER_DESCRIPTION,
        glbgt.ledger_name AS LEDGER_NAME,
        glbgt.ledger_currency AS LEDGER_CURRENCY,
        glbgt.period_year AS PERIOD_YEAR,
        glbgt.period_number AS PERIOD_NUMBER,
        glbgt.period_name AS PERIOD_NAME,
        glbgt.period_start_date,
        glbgt.period_end_date,
        glbgt.balance_type_code AS BALANCE_TYPE_CODE,
        glbgt.balance_type AS BALANCE_TYPE,
        glbgt.budget_name AS BUDGET_NAME,
        glbgt.encumbrance_type AS ENCUMBRANCE_TYPE,
        glbgt.begin_balance_dr AS BEGIN_BALANCE_DR,
        glbgt.begin_balance_cr AS BEGIN_BALANCE_CR,
        glbgt.period_net_dr AS PERIOD_NET_DR,
        glbgt.period_net_cr AS PERIOD_NET_CR,
        glbgt.code_combination_id AS CODE_COMBINATION_ID,
        CAST('NA' AS STRING) AS ACCOUNTING_CODE_COMBINATION,
        CAST('NA' AS STRING) AS CODE_COMBINATION_DESCRIPTION,
        glbgt.control_account_flag AS CONTROL_ACCOUNT_FLAG,
        glbgt.control_account AS CONTROL_ACCOUNT,
        CAST('NA' AS STRING) AS BALANCING_SEGMENT,
        CAST('NA' AS STRING) AS NATURAL_ACCOUNT_SEGMENT,
        CAST('NA' AS STRING) AS COST_CENTER_SEGMENT,
        CAST('NA' AS STRING) AS MANAGEMENT_SEGMENT,
        CAST('NA' AS STRING) AS INTERCOMPANY_SEGMENT,
        CAST('NA' AS STRING) AS BALANCING_SEGMENT_DESC,
        CAST('NA' AS STRING) AS NATURAL_ACCOUNT_DESC,
        CAST('NA' AS STRING) AS COST_CENTER_DESC,
        CAST('NA' AS STRING) AS MANAGEMENT_SEGMENT_DESC,
        CAST('NA' AS STRING) AS INTERCOMPANY_SEGMENT_DESC,
        glbgt.segment1 AS SEGMENT1,
        glbgt.segment2 AS SEGMENT2,
        glbgt.segment3 AS SEGMENT3,
        glbgt.segment4 AS SEGMENT4,
        glbgt.segment5 AS SEGMENT5,
        glbgt.segment6 AS SEGMENT6,
        glbgt.segment7 AS SEGMENT7,
        glbgt.segment8 AS SEGMENT8,
        glbgt.segment9 AS SEGMENT9,
        glbgt.segment10 AS SEGMENT10,
        CAST('NA' AS STRING) AS BEGIN_RUNNING_TOTAL_CR,
        CAST('NA' AS STRING) AS BEGIN_RUNNING_TOTAL_DR,
        CAST('NA' AS STRING) AS END_RUNNING_TOTAL_CR,
        CAST('NA' AS STRING) AS END_RUNNING_TOTAL_DR,
        CAST('NA' AS STRING) AS LEGAL_ENTITY_ID,
        CAST('NA' AS STRING) AS LEGAL_ENTITY_NAME,
        CAST('NA' AS STRING) AS LE_ADDRESS_LINE_1,
        CAST('NA' AS STRING) AS LE_ADDRESS_LINE_2,
        CAST('NA' AS STRING) AS LE_ADDRESS_LINE_3,
        CAST('NA' AS STRING) AS LE_CITY,
        CAST('NA' AS STRING) AS LE_REGION_1,
        CAST('NA' AS STRING) AS LE_REGION_2,
        CAST('NA' AS STRING) AS LE_REGION_3,
        CAST('NA' AS STRING) AS LE_POSTAL_CODE,
        CAST('NA' AS STRING) AS LE_COUNTRY,
        CAST('NA' AS STRING) AS LE_REGISTRATION_NUMBER,
        CAST('NA' AS STRING) AS LE_REGISTRATION_EFFECTIVE_FROM,
        CAST('NA' AS STRING) AS LE_BR_DAILY_INSCRIPTION_NUMBER,
        CAST(NULL AS TIMESTAMP) AS LE_BR_DAILY_INSCRIPTION_DATE,
        CAST('NA' AS STRING) AS LE_BR_DAILY_ENTITY,
        CAST('NA' AS STRING) AS LE_BR_DAILY_LOCATION,
        CAST('NA' AS STRING) AS LE_BR_DIRECTOR_NUMBER,
        CAST('NA' AS STRING) AS LE_BR_ACCOUNTANT_NUMBER,
        CAST('NA' AS STRING) AS LE_BR_ACCOUNTANT_NAME,
        gjl_gjh.attribute1 AS BE_PROJECT_NO,
        NULL AS BE_PROJECT_DESC,
        gjl_gjh.attribute2 AS BE_TASK_NO,
        NULL AS BE_TASK_DESC,
        gjb_papf.full_name AS approver_name,
        NULL AS PO_NUMBER,
        NULL AS invoice_num,
        NULL AS INVOICE_ID,
        NULL AS vendor_id,
        NULL AS vendor_site_id,
        glbgt.TRANSLATED_FLAG,
        0 AS TEMP_LINE_NUM
      FROM gold_bec_dwh.FACT_XLA_REPORT_BALANCES_STG AS glbgt, gjl_gjh, (
        SELECT
          *
        FROM silver_bec_ods.fun_seq_versions
        WHERE
          is_deleted_flg <> 'Y'
      ) AS fsv1, (
        SELECT
          *
        FROM silver_bec_ods.fun_seq_versions
        WHERE
          is_deleted_flg <> 'Y'
      ) AS fsv2, gjst, gdct, gjb_papf
      WHERE
        gjl_gjh.ledger_id = glbgt.ledger_id
        AND gjl_gjh.code_combination_id = glbgt.code_combination_id
        AND (
          gjl_gjh.effective_date BETWEEN glbgt.period_start_date AND glbgt.period_end_date
        )
        AND gjl_gjh.period_name = glbgt.period_name /* AND   gjh.je_header_id                 = gjl.je_header_id */
        AND gjl_gjh.actual_flag = glbgt.balance_type_code
        AND CASE
          WHEN gjl_gjh.currency_code = 'STAT'
          THEN gjl_gjh.currency_code
          ELSE glbgt.ledger_currency
        END = glbgt.ledger_currency
        AND COALESCE(gjl_gjh.budget_version_id, -19999) = COALESCE(glbgt.budget_version_id, -19999)
        AND COALESCE(gjl_gjh.encumbrance_type_id, -19999) = COALESCE(glbgt.encumbrance_type_id, -19999)
        AND gjb_papf.je_batch_id = gjl_gjh.je_batch_id /* AND   fdu.user_id                      = gjl.created_by --change */
        AND fsv1.SEQ_VERSION_ID() = gjl_gjh.posting_acct_seq_version_id
        AND fsv2.SEQ_VERSION_ID() = gjl_gjh.close_acct_seq_version_id /*       AND   --gjct.je_category_name            = gjh.je_category */
        AND gjst.je_source_name = gjl_gjh.je_source
        AND gdct.CONVERSION_TYPE() = gjl_gjh.currency_conversion_type
    )
    UNION ALL
    (
      SELECT
        gjh.default_effective_date AS GL_DATE,
        gjl.created_by AS created_by_id,
        gjh.creation_date AS CREATION_DATE,
        gjh.last_update_date AS LAST_UPDATE_DATE,
        CAST(NULL AS TIMESTAMP) AS GL_TRANSFER_DATE,
        gjh.reference_date AS REFERENCE_DATE,
        CAST(NULL AS TIMESTAMP) AS COMPLETED_DATE,
        NULL AS TRANSACTION_NUMBER,
        CAST(NULL AS TIMESTAMP) AS TRANSACTION_DATE,
        fsv1.header_name AS ACCOUNTING_SEQUENCE_NAME,
        fsv1.version_name AS ACCOUNTING_SEQUENCE_VERSION,
        gjh.posting_acct_seq_value AS ACCOUNTING_SEQUENCE_NUMBER,
        fsv2.header_name AS REPORTING_SEQUENCE_NAME,
        fsv2.version_name AS REPORTING_SEQUENCE_VERSION,
        gjh.close_acct_seq_value AS REPORTING_SEQUENCE_NUMBER,
        NULL AS DOCUMENT_CATEGORY,
        NULL AS DOCUMENT_SEQUENCE_NAME,
        NULL AS DOCUMENT_SEQUENCE_NUMBER,
        NULL AS APPLICATION_ID,
        NULL AS APPLICATION_NAME,
        NULL AS HEADER_ID,
        gjh.description AS HEADER_DESCRIPTION,
        NULL AS FUND_STATUS,
        gjh.je_category,
        gjst.user_je_source_name AS JE_SOURCE_NAME,
        NULL AS EVENT_ID,
        CAST(NULL AS TIMESTAMP) AS EVENT_DATE,
        NULL AS EVENT_NUMBER,
        NULL AS EVENT_CLASS_CODE,
        NULL AS EVENT_CLASS_NAME,
        NULL AS EVENT_TYPE_CODE,
        NULL AS EVENT_TYPE_NAME,
        gjb.NAME AS GL_BATCH_NAME,
        gjb.posted_date,
        gjh.je_header_id,
        gjh.NAME AS GL_JE_NAME,
        gjh.ACCRUAL_REV_PERIOD_NAME AS REVERSAL_PERIOD,
        gjh.ACCRUAL_REV_STATUS AS REVERSAL_STATUS,
        gjh.external_reference AS EXTERNAL_REFERENCE,
        gjl.je_line_num AS GL_LINE_NUMBER,
        gjl.effective_date,
        gjl.je_line_num AS LINE_NUMBER,
        gjl.je_line_num AS ORIG_LINE_NUMBER,
        NULL AS ACCOUNTING_CLASS_CODE,
        NULL AS ACCOUNTING_CLASS_NAME,
        gjl.description AS LINE_DESCRIPTION,
        gjh.currency_code AS ENTERED_CURRENCY,
        gjh.currency_conversion_rate AS CONVERSION_RATE,
        gjh.currency_conversion_date AS CONVERSION_RATE_DATE,
        gjh.currency_conversion_type AS CONVERSION_RATE_TYPE_CODE,
        gdct.user_conversion_type AS CONVERSION_RATE_TYPE,
        gjl.entered_dr AS ENTERED_DR,
        gjl.entered_cr AS ENTERED_CR,
        NULL AS UNROUNDED_ACCOUNTED_DR,
        NULL AS UNROUNDED_ACCOUNTED_CR,
        gjl.accounted_dr AS ACCOUNTED_DR,
        gjl.accounted_cr AS ACCOUNTED_CR,
        gjl.stat_amount AS STATISTICAL_AMOUNT,
        gjl.jgzz_recon_ref_11i AS RECONCILIATION_REFERENCE,
        NULL AS PARTY_TYPE_CODE,
        NULL AS PARTY_TYPE,
        glbgt.ledger_id AS LEDGER_ID,
        glbgt.ledger_short_name AS LEDGER_SHORT_NAME,
        glbgt.ledger_description AS LEDGER_DESCRIPTION,
        glbgt.ledger_name AS LEDGER_NAME,
        glbgt.ledger_currency AS LEDGER_CURRENCY,
        glbgt.period_year AS PERIOD_YEAR,
        glbgt.period_number AS PERIOD_NUMBER,
        glbgt.period_name AS PERIOD_NAME,
        glbgt.period_start_date,
        glbgt.period_end_date,
        glbgt.balance_type_code AS BALANCE_TYPE_CODE,
        glbgt.balance_type AS BALANCE_TYPE,
        glbgt.budget_name AS BUDGET_NAME,
        glbgt.encumbrance_type AS ENCUMBRANCE_TYPE,
        glbgt.begin_balance_dr AS BEGIN_BALANCE_DR,
        glbgt.begin_balance_cr AS BEGIN_BALANCE_CR,
        glbgt.period_net_dr AS PERIOD_NET_DR,
        glbgt.period_net_cr AS PERIOD_NET_CR,
        glbgt.code_combination_id AS CODE_COMBINATION_ID,
        CAST('NA' AS STRING) AS ACCOUNTING_CODE_COMBINATION,
        CAST('NA' AS STRING) AS CODE_COMBINATION_DESCRIPTION,
        glbgt.control_account_flag AS CONTROL_ACCOUNT_FLAG,
        glbgt.control_account AS CONTROL_ACCOUNT,
        CAST('NA' AS STRING) AS BALANCING_SEGMENT,
        CAST('NA' AS STRING) AS NATURAL_ACCOUNT_SEGMENT,
        CAST('NA' AS STRING) AS COST_CENTER_SEGMENT,
        CAST('NA' AS STRING) AS MANAGEMENT_SEGMENT,
        CAST('NA' AS STRING) AS INTERCOMPANY_SEGMENT,
        CAST('NA' AS STRING) AS BALANCING_SEGMENT_DESC,
        CAST('NA' AS STRING) AS NATURAL_ACCOUNT_DESC,
        CAST('NA' AS STRING) AS COST_CENTER_DESC,
        CAST('NA' AS STRING) AS MANAGEMENT_SEGMENT_DESC,
        CAST('NA' AS STRING) AS INTERCOMPANY_SEGMENT_DESC,
        glbgt.segment1 AS SEGMENT1,
        glbgt.segment2 AS SEGMENT2,
        glbgt.segment3 AS SEGMENT3,
        glbgt.segment4 AS SEGMENT4,
        glbgt.segment5 AS SEGMENT5,
        glbgt.segment6 AS SEGMENT6,
        glbgt.segment7 AS SEGMENT7,
        glbgt.segment8 AS SEGMENT8,
        glbgt.segment9 AS SEGMENT9,
        glbgt.segment10 AS SEGMENT10,
        CAST('NA' AS STRING) AS BEGIN_RUNNING_TOTAL_CR,
        CAST('NA' AS STRING) AS BEGIN_RUNNING_TOTAL_DR,
        CAST('NA' AS STRING) AS END_RUNNING_TOTAL_CR,
        CAST('NA' AS STRING) AS END_RUNNING_TOTAL_DR,
        CAST('NA' AS STRING) AS LEGAL_ENTITY_ID,
        CAST('NA' AS STRING) AS LEGAL_ENTITY_NAME,
        CAST('NA' AS STRING) AS LE_ADDRESS_LINE_1,
        CAST('NA' AS STRING) AS LE_ADDRESS_LINE_2,
        CAST('NA' AS STRING) AS LE_ADDRESS_LINE_3,
        CAST('NA' AS STRING) AS LE_CITY,
        CAST('NA' AS STRING) AS LE_REGION_1,
        CAST('NA' AS STRING) AS LE_REGION_2,
        CAST('NA' AS STRING) AS LE_REGION_3,
        CAST('NA' AS STRING) AS LE_POSTAL_CODE,
        CAST('NA' AS STRING) AS LE_COUNTRY,
        CAST('NA' AS STRING) AS LE_REGISTRATION_NUMBER,
        CAST('NA' AS STRING) AS LE_REGISTRATION_EFFECTIVE_FROM,
        CAST('NA' AS STRING) AS LE_BR_DAILY_INSCRIPTION_NUMBER,
        CAST(NULL AS TIMESTAMP) AS LE_BR_DAILY_INSCRIPTION_DATE,
        CAST('NA' AS STRING) AS LE_BR_DAILY_ENTITY,
        CAST('NA' AS STRING) AS LE_BR_DAILY_LOCATION,
        CAST('NA' AS STRING) AS LE_BR_DIRECTOR_NUMBER,
        CAST('NA' AS STRING) AS LE_BR_ACCOUNTANT_NUMBER,
        CAST('NA' AS STRING) AS LE_BR_ACCOUNTANT_NAME,
        gjl.attribute1 AS BE_PROJECT_NO,
        NULL AS BE_PROJECT_DESC,
        gjl.attribute2 AS BE_TASK_NO,
        NULL AS BE_TASK_DESC,
        PAPF.FULL_NAME AS APPROVER_NAME,
        NULL AS PO_NUMBER,
        NULL AS invoice_num,
        NULL AS INVOICE_ID,
        NULL AS vendor_id,
        NULL AS vendor_site_id,
        glbgt.TRANSLATED_FLAG,
        0 AS TEMP_LINE_NUM
      FROM gold_bec_dwh.FACT_XLA_REPORT_BALANCES_STG AS glbgt, (
        SELECT
          *
        FROM silver_bec_ods.fnd_new_messages
        WHERE
          is_deleted_flg <> 'Y'
      ) AS fnm, (
        SELECT
          *
        FROM silver_bec_ods.fun_seq_versions
        WHERE
          is_deleted_flg <> 'Y'
      ) AS fsv1, (
        SELECT
          *
        FROM silver_bec_ods.fun_seq_versions
        WHERE
          is_deleted_flg <> 'Y'
      ) AS fsv2, (
        SELECT
          *
        FROM silver_bec_ods.gl_je_sources_tl
        WHERE
          is_deleted_flg <> 'Y'
      ) AS gjst, (
        SELECT
          *
        FROM silver_bec_ods.gl_daily_conversion_types
        WHERE
          is_deleted_flg <> 'Y'
      ) AS gdct, (
        SELECT
          *
        FROM silver_bec_ods.gl_je_lines
        WHERE
          is_deleted_flg <> 'Y'
      ) AS gjl, (
        SELECT
          *
        FROM silver_bec_ods.gl_je_headers
        WHERE
          is_deleted_flg <> 'Y'
      ) AS gjh, (
        SELECT
          *
        FROM silver_bec_ods.gl_je_batches
        WHERE
          is_deleted_flg <> 'Y'
      ) AS gjb, (
        SELECT
          *
        FROM silver_bec_ods.PER_ALL_PEOPLE_F
        WHERE
          is_deleted_flg <> 'Y'
      ) AS PAPF
      WHERE
        gjl.ledger_id = glbgt.ledger_id
        AND gjl.code_combination_id = glbgt.code_combination_id
        AND gjl.effective_date BETWEEN glbgt.period_start_date AND glbgt.period_end_date
        AND gjl.period_name = glbgt.period_name
        AND gjh.je_header_id = gjl.je_header_id
        AND gjh.actual_flag = glbgt.balance_type_code
        AND CASE
          WHEN gjh.currency_code = 'STAT'
          THEN gjh.currency_code
          ELSE glbgt.ledger_currency
        END = glbgt.ledger_currency /* added bug 6686541 */
        AND COALESCE(gjh.je_from_sla_flag, 'N') = 'U'
        AND fnm.application_id = 101
        AND fnm.language_code = 'US'
        AND fnm.message_name IN ('PPOS0220', 'PPOS0221', 'PPOS0222', 'PPOS0243', 'PPOS0222_G', 'PPOSO275')
        AND gjl.description = fnm.message_text
        AND COALESCE(gjh.budget_version_id, -19999) = COALESCE(glbgt.budget_version_id, -19999)
        AND COALESCE(gjh.encumbrance_type_id, -19999) = COALESCE(glbgt.encumbrance_type_id, -19999)
        AND gjb.je_batch_id = gjh.je_batch_id
        AND gjb.status = 'P'
        AND GJB.APPROVER_EMPLOYEE_ID = PAPF.PERSON_ID()
        AND gjb.creation_date BETWEEN papf.EFFECTIVE_START_DATE() AND COALESCE(papf.EFFECTIVE_END_DATE(), CURRENT_TIMESTAMP() + 1) /*       AND   fdu.user_id                      = gjl.created_by  --change */
        AND fsv1.SEQ_VERSION_ID() = gjh.posting_acct_seq_version_id
        AND fsv2.SEQ_VERSION_ID() = gjh.close_acct_seq_version_id /* AND   gjct.je_category_name            = gjh.je_category */
        AND gjst.je_source_name = gjh.je_source
        AND gjst.language = 'US'
        AND gdct.CONVERSION_TYPE() = gjh.currency_conversion_type
        AND NOT EXISTS(
          SELECT
            'x'
          FROM (
            SELECT
              *
            FROM silver_bec_ods.gl_import_references
            WHERE
              is_deleted_flg <> 'Y'
          ) AS gir
          WHERE
            gir.je_header_id = gjl.je_header_id AND gir.je_line_num = gjl.je_line_num
        )
        AND (
          gjl.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_gl_account_analysis' AND batch_name = 'gl'
          )
          OR gjh.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_gl_account_analysis' AND batch_name = 'gl'
          )
          OR gjb.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_gl_account_analysis' AND batch_name = 'gl'
          )
        )
    )
  ) AS A
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_gl_account_analysis' AND batch_name = 'gl';