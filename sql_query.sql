WITH loan AS (
            SELECT
                select_product_id AS "ИД сделки",
                id AS "КРЕДИТ - ИД",
                bank_name AS "Банк",
                partner_name AS "Партнер"
            FROM online.fact_loan
            WHERE partner_name LIKE 'FIT Service%%'
                AND stage_code = 'attention'
        ),
        credit AS (
            SELECT
                select_product_id AS "ИД сделки",
                id AS "КРЕДИТ - ИД",
                bank_name AS "Банк",
                partner_name AS "Партнер"
            FROM online.fact_credit
            WHERE partner_name LIKE 'FIT Service%%'
                AND stage_code = 'attention'
        )
        SELECT * FROM loan
        UNION ALL
        SELECT * FROM credit;