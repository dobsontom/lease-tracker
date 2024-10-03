CREATE OR REPLACE VIEW `inm-iar-data-warehouse-dev.lease_tracker.lease_tracker_billing_data` AS (
    WITH lease_base AS (
        SELECT
            id,
            ssp_number,
            lsp,
            lsr,
            end_customer,
            account_number,
            land_aero_maritime,
            service_type,
            start_date_of_current_lease,
            end_date_of_current_lease,
            last_refresh_time
        FROM
            `inm-iar-data-warehouse-dev.lease_tracker.lease_tracker_base`
    ),

    invoice_data AS (
        SELECT
            id AS ssp_number,
            leasing_request_c AS leasing_request,
            name AS invoice_name,
            bill_period_end_c AS invoice_bill_period_end,
            bill_period_start_c AS invoice_bill_period_start,
            billed_amount_c AS invoice_billed_amount,
            billing_source_c AS invoice_billing_source,
            wholesale_credit_rebill_c AS invoice_wholesale_credit_rebill,
            wholesale_or_retail_c AS invoice_wholesale_or_retail,
            created_date AS invoice_created_date,
            COALESCE(
                invoice_number_c,
                external_invoice_id_c,
                internal_invoice_id_c
            ) AS invoice_number,
            COALESCE(
                billing_status_c,
                external_billing_status_c,
                internal_billing_status_c
            ) AS invoice_billing_status
        FROM
            `inm-iar-data-warehouse-dev.sdp_salesforce_src.invoice_c`
        WHERE
            is_deleted = FALSE
    ),

    billing_data AS (
        SELECT
            lb.ssp_number,
            lb.lsp,
            lb.lsr,
            lb.end_customer,
            lb.account_number,
            lb.land_aero_maritime,
            inv.invoice_wholesale_or_retail,
            inv.invoice_number,
            inv.invoice_name,
            inv.invoice_wholesale_credit_rebill,
            inv.invoice_billing_source,
            lb.service_type,
            lb.start_date_of_current_lease,
            lb.end_date_of_current_lease,
            inv.invoice_bill_period_start,
            inv.invoice_bill_period_end,
            inv.invoice_billing_status,
            inv.invoice_billed_amount,
            inv.invoice_created_date,
            lb.last_refresh_time
        FROM
            lease_base AS lb
        INNER JOIN invoice_data AS inv ON lb.id = inv.leasing_request
    )

    SELECT
        ssp_number AS `SSP number`,
        lsp AS `LSP`,
        lsr AS `LSR`,
        end_customer AS `End Customer`,
        account_number AS `Account Number`,
        land_aero_maritime AS `Land Aero Maritime`,
        invoice_wholesale_or_retail AS `Invoice Wholesale or Retail`,
        invoice_number AS `Invoice Number`,
        invoice_name AS `Invoice Name`,
        invoice_wholesale_credit_rebill AS `Invoice Wholesale Credit Rebill`,
        invoice_billing_source AS `Invoice Billing Source`,
        service_type AS `Service Type`,
        start_date_of_current_lease AS `Start Date of Current Lease`,
        end_date_of_current_lease AS `End Date of Current Lease`,
        invoice_bill_period_start AS `Invoice Bill Period Start`,
        invoice_bill_period_end AS `Invoice Bill Period End`,
        invoice_billing_status AS `Invoice Billing Status`,
        invoice_billed_amount AS `Invoice Billed Amount`,
        invoice_created_date AS `Invoice Created Date`,
        last_refresh_time AS `Last Refresh Time`
    FROM
        billing_data
);
