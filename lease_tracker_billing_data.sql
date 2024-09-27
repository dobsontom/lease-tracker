CREATE OR REPLACE VIEW `inm-iar-data-warehouse-dev.lease_tracker.lease_tracker_billing_data` AS (
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
        created_date AS `Invoice Created Date`,
        last_refresh_time AS `Last Refresh Time`
    FROM
        `inm-iar-data-warehouse-dev.lease_tracker.lease_tracker_base`
);
