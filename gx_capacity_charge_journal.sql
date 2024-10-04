CREATE OR REPLACE VIEW `inm-iar-data-warehouse-dev.lease_tracker.gx_capacity_charge_journal` AS (
    WITH lease_base AS (
        SELECT
            *
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

    -- Joins non-aggregated invoice data. Aggregated invoice data is joined
    -- in lease_tracker_base.sql
    lease_invoice_data AS (
        SELECT
            base.*,
            inv.* EXCEPT (ssp_number)
        FROM
            lease_base AS base
        LEFT JOIN invoice_data AS inv
            ON base.id = inv.leasing_request
    ),

    gx_capacity_charge_base AS (
        SELECT
            l.lease_update_status AS status,
            l.ssp_number AS lease_contract_no,
            l.account_number,
            l.account_manager,
            l.end_customer AS leasing_request_lease_customer_name,
            l.lsr,
            l.lsp,
            l.retail_invoice_id AS retail_sap_account_codes,
            l.account_number AS wholesale_sap_account_code, -- This needs attention as it sometimes has the wrong value.
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
            REGEXP_REPLACE(ssp_number, '\\S(\\s*\\(FREE USE\\)\\s*)$', '') AS normalised_lease_contract_no,
            FORMAT_DATE('%d-%m-%Y', DATE(l.start_date_of_current_lease)) AS lease_start_date,
            FORMAT_DATE('%d-%m-%Y', DATE(l.end_date_of_current_lease)) AS lease_end_date,
            CASE WHEN l.satellite IS NOT NULL THEN 100 END AS forward_bandwidth_mhz,
            CASE WHEN l.satellite IS NOT NULL THEN 100 END AS return_bandwidth_mhz,
            CASE WHEN l.satellite_2 IS NOT NULL THEN 100 END AS forward_bandwidth_mhz_2,
            CASE WHEN l.satellite_2 IS NOT NULL THEN 100 END AS return_bandwidth_mhz_2,
            CASE WHEN l.satellite_3 IS NOT NULL THEN 100 END AS forward_bandwidth_mhz_3,
            CASE WHEN l.satellite_3 IS NOT NULL THEN 100 END AS return_bandwidth_mhz_3,
            CASE WHEN l.satellite_4 IS NOT NULL THEN 100 END AS forward_bandwidth_mhz_4,
            CASE WHEN l.satellite_4 IS NOT NULL THEN 100 END AS return_bandwidth_mhz_4,
            CONCAT(
                'GX Leases capacity charges ',
                FORMAT_DATE('%b %Y', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
            ) AS assignment,
            GREATEST(DATE_TRUNC(CURRENT_DATE(), MONTH), DATE(l.start_date_of_current_lease))
                AS lease_start_date_2022_working,
            LEAST(LAST_DAY(CURRENT_DATE(), MONTH), DATE(l.end_date_of_current_lease))
                AS lease_end_date_2022_workings,
            IF(l.wholesale_invoice_id = '60006791', l.customer_name, l.wholesale_invoice_id)
                AS customer
        FROM
            lease_invoice_data AS l
        WHERE
        -- Remove all 'Pending' leases
            l.lease_update_status NOT IN ('Pending')
            AND NOT (
                l.lease_update_status = 'Active'
                AND DATE(l.start_date_of_current_lease)
                > DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 1 DAY
            )
            AND NOT (
                -- l.lease_update_status = 'Expired' // AND
                DATE(l.end_date_of_current_lease)
                < DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 1 DAY
            )
            -- Keep only lease contract numbers containing "GX"
            AND l.ssp_number LIKE '%GX%'
            -- Remove lease contract numbers containing "HGX"
            AND l.ssp_number NOT LIKE '%HGX%'
    ),

    calculate_months_and_cost AS (
        SELECT
            *,
            CONCAT(
                COALESCE(lease_contract_no, ''), ' - ',
                COALESCE(service_type, ''), ' - ',
                COALESCE(leasing_request_lease_customer_name, ''), '-',
                COALESCE(wholesale_sap_account_code, '')
            ) AS text,
            DATE_DIFF(lease_start_date_2022_working, lease_end_date_2022_workings, MONTH)
                AS months_active,
            (
                COALESCE(forward_bandwidth_mhz, 0)
                + COALESCE(forward_bandwidth_mhz_2, 0)
                + COALESCE(forward_bandwidth_mhz_3, 0)
                + COALESCE(forward_bandwidth_mhz_4, 0)
                + COALESCE(return_bandwidth_mhz, 0)
                + COALESCE(return_bandwidth_mhz_2, 0)
                + COALESCE(return_bandwidth_mhz_3, 0)
                + COALESCE(return_bandwidth_mhz_4, 0)
            ) * 1283 AS total_cost
        FROM
            gx_capacity_charge_base
    ),

    calculate_total_cost AS (
        SELECT
            *,
            ROUND(total_cost * months_active, 2) AS amount_in_doc_currency
        FROM calculate_months_and_cost
    ),

    parent_lease_flag AS (
        SELECT
            normalised_lease_contract_no,
            MAX(CASE WHEN lease_contract_no NOT LIKE '%(FREE USE)%' THEN 1 ELSE 0 END)
                AS has_parent_lease
        FROM calculate_total_cost
        GROUP BY normalised_lease_contract_no
    ),

    add_parent_lease_flag AS (
        SELECT
            calc.*,
            parent.has_parent_lease
        FROM calculate_total_cost AS calc
        LEFT JOIN
            parent_lease_flag AS parent
            ON calc.normalised_lease_contract_no = parent.normalised_lease_contract_no
    )

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
        wholesale_sap_account_code AS customer, -- Needs attention
        CASE
            WHEN has_parent_lease = 1 AND lease_contract_no LIKE '%(FREE USE)%' THEN 0
            ELSE amount_in_doc_currency
        END AS amount_in_doc_currency
    FROM
        add_parent_lease_flag
);
