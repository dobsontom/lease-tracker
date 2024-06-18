WITH
    invoice_c AS (
        SELECT
            id,
            is_deleted,
            name,
            created_date,
            created_by_id,
            last_modified_date,
            last_modified_by_id,
            system_modstamp,
            connection_received_id,
            connection_sent_id,
            bill_period_end_c,
            bill_period_start_c,
            billed_amount_c,
            billing_source_c,
            billing_status_c,
            invoice_number_c,
            leasing_request_c,
            wholesale_credit_rebill_c,
            wholesale_or_retail_c,
            IFNULL(
                invoice_number_c,
                (
                    IFNULL(external_invoice_id_c, internal_invoice_id_c)
                )
            ) AS invoice_number,
            IFNULL(
                billing_status_c,
                (
                    IFNULL(
                        external_billing_status_c,
                        internal_billing_status_c
                    )
                )
            ) AS billing_status
        FROM
            inm-bi.sdp_salesforce_src.invoice_c
        WHERE
            is_deleted = FALSE
    ),
    leasing_request_c AS (
        SELECT
            *,
            CAST(
                CONCAT(
                    CAST(CAST(lease_start_date_c AS DATE) AS STRING),
                    "T",
                    LEFT(CAST(lease_start_time_c AS STRING), 12)
                ) AS DATETIME
            ) AS start_date_of_current_lease,
            CAST(
                CONCAT(
                    CAST(CAST(end_date_c AS DATE) AS STRING),
                    "T",
                    LEFT(CAST(lease_end_time_c AS STRING), 12)
                ) AS DATETIME
            ) AS end_date_of_current_lease
        FROM
            inm-bi.sdp_salesforce_src.leasing_request_c
        WHERE
            contract_number_c IS NOT NULL
    ),
    user AS (
        SELECT
            id AS user_id,
            name AS account_nanager_name,
            division AS account_manager_division
        FROM
            inm-bi.sdp_salesforce_src.user
    ),
    leasing_request_user AS (
        SELECT
            *
        FROM
            leasing_request_c lr
            LEFT JOIN user u ON lr.account_manager_c = u.user_id
    ),
    billing_data_output AS (
        SELECT
            lrq.contract_number_c AS ssp_number,
            lrq.lsp_c AS lsp,
            lrq.lsr_c AS lsr,
            lrq.name AS end_customer,
            lrq.account_number_c AS account_number,
            lrq.land_or_maritime_c AS land_aero_maritime,
            i.wholesale_or_retail_c AS invoice_wholesale_or_retail,
            i.invoice_number,
            i.name AS invoice_name,
            i.wholesale_credit_rebill_c AS invoice_wholesale_credit_rebill,
            i.billing_source_c AS invoice_billing_source,
            lrq.lease_service_type_c AS service_type,
            lrq.start_date_of_current_lease,
            lrq.end_date_of_current_lease,
            i.bill_period_start_c AS invoice_bill_period_start,
            i.bill_period_end_c AS invoice_bill_period_end,
            i.billing_status_c AS invoice_billing_status,
            i.billed_amount_c AS invoice_billed_amount,
            i.created_date AS invoice_created_date
        FROM
            leasing_request_user lrq
            JOIN invoice_c i ON lrq.id = i.leasing_request_c
    ),
     billing_invoice_number_pivot AS (
SELECT
    ssp_number,
    retail AS retail_invoice_id,
    wholesale AS wholesale_invoice_id
FROM
    (
        SELECT
            *
        FROM
            billing_data_output
    ) PIVOT(
        STRING_AGG(invoice_number, "; ")
        FOR invoice_wholesale_or_retail IN ('Retail', 'Wholesale')
    )
       )