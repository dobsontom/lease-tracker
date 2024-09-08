CREATE OR REPLACE TABLE `inm-iar-data-warehouse-dev.lease_tracker.call_off_tracker` AS (
    WITH
    leasing_request_call_off_block AS (
        SELECT
            lr.id,
            cob.any_other_technical_details_required_cob_c
                AS any_other_technical_details_required_cob,
            lr.approval_document_id_c AS approval_document_id,
            lr.business_unit_c AS business_unit,
            lr.name AS end_customer,
            lr.lsp_c AS lsp,
            lr.lsr_c AS lsr,
            lr.project_name_c AS project_name,
            lr.contract_number_c AS ssp_number,
            lr.total_value_of_the_order_c AS total_value,
            cob.cob_start_time_c,
            cob.cob_end_time_c AS cob_end_time,
            cob.forward_bandwidth_k_hz_cob_c AS forward_bandwidth_khz,
            cob.power_d_bw_cob_c AS power_dbw,
            cob.price_plan_cob_c AS price_plan,
            cob.retail_contract_value_cob_c AS retail_block_value,
            cob.return_bandwidth_k_hz_cob_c AS return_bandwidth_khz,
            cob.id AS cob_id,
            cob.name AS call_off_block_name,
            cob.status_cob_c AS status,
            cob.wholesale_contract_value_cob_c AS wholesale_block_value,
            cob.daily_usage_charge_cob_c AS daily_usage_charge_cob,
            cob.daily_usage_charge_retail_c AS daily_usage_charge_retail,
            COALESCE(lr.project_id_c, '') AS project_id,
            DATE(cob.cob_start_date_c) AS start_date,
            DATE(cob.cob_end_date_c) AS end_date,
            FORMAT_TIMESTAMP(
                '%Y-%m-%d %H:%M:%S',
                TIMESTAMP(CONCAT(DATE(cob.cob_start_date_c), ' ', cob.cob_start_time_c))
            ) AS start_date_time,
            FORMAT_TIMESTAMP(
                '%Y-%m-%d %H:%M:%S',
                TIMESTAMP(CONCAT(DATE(cob.cob_end_date_c), ' ', cob.cob_end_time_c))
            ) AS end_date_time,
            COALESCE(cob.retail_contract_value_cob_c, cob.wholesale_contract_value_cob_c)
                AS call_off_block_value
        FROM
            `inm-iar-data-warehouse-dev.sdp_salesforce_src.leasing_request_c` AS lr
        INNER JOIN
            `inm-iar-data-warehouse-dev.sdp_salesforce_src.call_off_block_c` AS cob
            ON lr.id = cob.leasing_request_cob_c
    ),

    daily_charge AS (
        SELECT
            a.*,
            COALESCE(
                a.daily_usage_charge_retail,
                a.daily_usage_charge_cob,
                b.retail_periodic_payment_amount,
                b.wholesale_periodic_payment_amount
            ) AS daily_charge
        FROM
            leasing_request_call_off_block AS a
        LEFT JOIN
            `inm-iar-data-warehouse-dev.call_off_tracker.call_off_blocks_needing_daily_charge_for_sf_upload`
                AS b
            ON a.id = b.cob_id
            AND a.ssp_number = b.ssp_number
            AND a.call_off_block_name = b.call_off_block_name
        ORDER BY
            a.project_id ASC,
            a.start_date_time ASC
    ),

    end_date_time_and_value AS (
        SELECT
            a.*,
            b.max_end_date_time,
            b.sum_call_off_block_value
        FROM
            daily_charge AS a
        INNER JOIN (
            SELECT
                project_id,
                MAX(end_date_time) AS max_end_date_time,
                SUM(call_off_block_value) AS sum_call_off_block_value
            FROM
                daily_charge
            GROUP BY
                project_id
        ) AS b ON a.project_id = b.project_id
        WHERE
            a.status != 'Cancelled'
    ),

    final_data AS (
        SELECT
            a.project_id,
            a.max_end_date_time,
            a.sum_call_off_block_value,
            a.project_name,
            a.ssp_number,
            a.status,
            a.price_plan,
            b.business_unit,
            a.lsp,
            a.lsr,
            a.end_customer,
            a.approval_document_id,
            a.forward_bandwidth_khz,
            a.return_bandwidth_khz,
            a.power_dbw,
            a.total_value,
            a.wholesale_block_value,
            a.retail_block_value,
            a.start_date,
            a.end_date,
            a.start_date_time,
            a.end_date_time,
            a.any_other_technical_details_required_cob,
            a.call_off_block_name,
            a.call_off_block_value,
            a.daily_usage_charge_retail,
            a.daily_charge,
            b.start_date_of_current_lease,
            b.end_date_of_current_lease
        FROM
            end_date_time_and_value AS a
        INNER JOIN
            `inm-iar-data-warehouse-dev.lease_tracker.lease_tracker` AS b
            ON a.ssp_number = b.ssp_number
    )

    SELECT
        *,
        1 AS raw_data_flag,
        CASE
            WHEN lsp = 'Inmarsat Solutions (Canada) Inc.'
                AND end_date_of_current_lease
                >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH)
                AND start_date_of_current_lease
                <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
                THEN 1
            ELSE 0
        END AS rtl_flag,
        CASE
            WHEN lsp != 'Inmarsat Solutions (Canada) Inc.'
                AND end_date_of_current_lease
                >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH)
                AND start_date_of_current_lease
                <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
                THEN 1
            ELSE 0
        END AS whs_flag
    FROM
        final_data
);
