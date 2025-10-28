INSERT INTO temp.telegram_bot_state (credit_id, deal_id)
VALUES (%s, %s)
ON CONFLICT (credit_id)
DO UPDATE SET deal_id = EXCLUDED.deal_id, last_update = CURRENT_TIMESTAMP;
