CREATE OR REPLACE VIEW `inm-iar-data-warehouse-dev.lease_tracker.gx_capacity_charge_journal` AS (
    WITH lease_base AS (
        SELECT
            id,
            lease_update_status,
            ssp_number,
            end_customer,
            lsr,
            lsp,
            retail_invoice_id,
            wholesale_sap_account_code,
            retail_sap_account_code,
            business_unit,
            service_type,
            number_of_beams,
            type_of_beam,
            spot_beam_equivalent,
            type_of_beam_equivalent,
            satellite,
            satellite_2,
            satellite_3,
            satellite_4,
            start_date_of_current_lease,
            end_date_of_current_lease
        FROM
            `inm-iar-data-warehouse-dev.lease_tracker.lease_tracker_base`
    ),

    -- Fetches and formats invoice data from GCP
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

    -- Joins non-aggregated invoice data. Aggregated invoice data is joined in lease_tracker_base.sql
    lease_invoice_data AS (
        SELECT
            base.*,
            inv.invoice_name,
            inv.invoice_bill_period_end,
            inv.invoice_bill_period_start,
            inv.invoice_billed_amount,
            inv.invoice_billing_source,
            inv.invoice_wholesale_credit_rebill,
            inv.invoice_wholesale_or_retail,
            inv.invoice_created_date,
            inv.invoice_number,
            inv.invoice_billing_status
        FROM
            lease_base AS base
        LEFT JOIN invoice_data AS inv
            ON base.id = inv.leasing_request
    ),

    gx_capacity_charge_base AS (
        SELECT
            l.lease_update_status AS status,
            l.ssp_number AS lease_contract_no,
            l.end_customer AS leasing_request_lease_customer_name,
            l.lsr,
            l.lsp,
            l.retail_invoice_id AS retail_sap_account_codes,
            l.wholesale_sap_account_code,
            l.retail_sap_account_code,
            l.business_unit,
            l.service_type,
            l.number_of_beams,
            l.type_of_beam,
            l.spot_beam_equivalent AS number_of_beams_equivalent,
            l.type_of_beam_equivalent,
            l.satellite,
            l.satellite_2,
            l.satellite_3,
            l.satellite_4,
            CASE
                WHEN business_unit = 'Enterprise' THEN '2533'
                WHEN business_unit = 'Global Government' THEN '4500'
                WHEN business_unit = 'US Government' THEN '4020'
            END AS profit_center,
            IF(l.wholesale_sap_account_code = '60006791', l.retail_sap_account_code, l.wholesale_sap_account_code)
                AS customer,
            REGEXP_REPLACE(ssp_number, '\\S(\\s*\\(FREE USE\\)\\s*)$', '') AS normalised_lease_contract_no,
            FORMAT_DATE('%d-%m-%Y', DATE(l.start_date_of_current_lease)) AS lease_start_date,
            FORMAT_DATE('%d-%m-%Y', DATE(l.end_date_of_current_lease)) AS lease_end_date,
            -- Replace any forward/return bandwidth values with 100, the only bandwidth offered for GX
            IF(l.satellite IS NOT NULL, 100, 0) AS forward_bandwidth_mhz,
            IF(l.satellite IS NOT NULL, 100, 0) AS return_bandwidth_mhz,
            IF(l.satellite_2 IS NOT NULL, 100, 0) AS forward_bandwidth_mhz_2,
            IF(l.satellite_2 IS NOT NULL, 100, 0) AS return_bandwidth_mhz_2,
            IF(l.satellite_3 IS NOT NULL, 100, 0) AS forward_bandwidth_mhz_3,
            IF(l.satellite_3 IS NOT NULL, 100, 0) AS return_bandwidth_mhz_3,
            IF(l.satellite_4 IS NOT NULL, 100, 0) AS forward_bandwidth_mhz_4,
            IF(l.satellite_4 IS NOT NULL, 100, 0) AS return_bandwidth_mhz_4,
            CONCAT(
                'GX Leases capacity charges ',
                FORMAT_DATE('%b %Y', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
            ) AS assignment,
            GREATEST(DATE_TRUNC(CURRENT_DATE(), MONTH), DATE(l.start_date_of_current_lease))
                AS lease_start_date_calculation,
            LEAST(LAST_DAY(CURRENT_DATE(), MONTH), DATE(l.end_date_of_current_lease))
                AS lease_end_date_calculation,
            DATE_DIFF(LAST_DAY(CURRENT_DATE(), MONTH), DATE_TRUNC(CURRENT_DATE(), MONTH), DAY) + 1
                AS days_in_current_month
        FROM
            lease_invoice_data AS l
        WHERE
            -- Exclude all 'Pending' leases as these fall outside of month end
            l.lease_update_status NOT IN ('Pending')
            -- Exclude active leases that start after month end as these are forecast and not actuals
            AND NOT (
                l.lease_update_status = 'Active'
                AND GREATEST(DATE_TRUNC(CURRENT_DATE(), MONTH), DATE(l.start_date_of_current_lease))
                > DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 1 DAY
            )
            -- Exclude any leases that have expired before month end as these are already captured in actuals.
            AND NOT (
                l.lease_update_status = 'Expired'
                OR LEAST(LAST_DAY(CURRENT_DATE(), MONTH), DATE(l.end_date_of_current_lease))
                <= DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 1 DAY
            )
            -- Exclude non-'GX' leases
            AND l.ssp_number LIKE '%GX%'
            -- Exclude "HGX" leases
            AND l.ssp_number NOT LIKE '%HGX%'
    ),

    active_months_and_cost AS (
        SELECT
            *,
            CONCAT(
                COALESCE(lease_contract_no, ''), ' - ',
                COALESCE(service_type, ''), ' - ',
                COALESCE(leasing_request_lease_customer_name, ''), '-',
                COALESCE(wholesale_sap_account_code, '')
            ) AS text,
            DATE_DIFF(lease_end_date_calculation, lease_start_date_calculation, DAY) + 1
            / days_in_current_month AS months_active,
            (
                forward_bandwidth_mhz
                + forward_bandwidth_mhz_2
                + forward_bandwidth_mhz_3
                + forward_bandwidth_mhz_4
                + return_bandwidth_mhz
                + return_bandwidth_mhz_2
                + return_bandwidth_mhz_3
                + return_bandwidth_mhz_4
            ) * 1283 AS total_cost
        FROM
            gx_capacity_charge_base
    ),

    total_cost AS (
        SELECT
            *,
            ROUND(total_cost * months_active, 2) AS amount_in_doc_currency
        FROM active_months_and_cost
    ),

    -- Determine whether there is a parent for each lease
    parent_lease_flag AS (
        SELECT
            normalised_lease_contract_no,
            MAX(CASE WHEN lease_contract_no NOT LIKE '%(FREE USE)%' THEN 1 ELSE 0 END)
                AS has_parent_lease
        FROM total_cost
        GROUP BY normalised_lease_contract_no
    ),

    add_parent_lease_flag AS (
        SELECT
            calc.*,
            parent.has_parent_lease
        FROM total_cost AS calc
        LEFT JOIN parent_lease_flag AS parent
            ON calc.normalised_lease_contract_no = parent.normalised_lease_contract_no
    ),

    final_data AS (
        SELECT DISTINCT
            lease_contract_no,
            normalised_lease_contract_no,
            profit_center,
            '70102179' AS product_material,
            NULL AS wbs_element,
            NULL AS trading_partner,
            text,
            '40' AS posting_key,
            assignment,
            NULL AS tax_amount,
            NULL AS tax_on_sales_code,
            'X' AS profitability_segment,
            customer,
            -- Set the total cost to zero for leases with a parent
            CASE
                WHEN has_parent_lease = 1 AND lease_contract_no LIKE '%(FREE USE)%' THEN 0
                ELSE amount_in_doc_currency
            END AS amount_in_doc_currency
        FROM
            add_parent_lease_flag
    )

    SELECT
        lease_contract_no,
        normalised_lease_contract_no,
        profit_center,
        product_material,
        wbs_element,
        trading_partner,
        text,
        posting_key,
        assignment,
        tax_amount,
        tax_on_sales_code,
        profitability_segment,
        customer,
        amount_in_doc_currency
    FROM
        final_data
);
