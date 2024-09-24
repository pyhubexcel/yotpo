import uuid
from sqlalchemy import insert, select

def save_customer_history(customer_history_data, customer_history_table, conn):
    customer_ids = {}
    for item in customer_history_data:
        # Generate a new UUID for customer_id
        customer_id = str(uuid.uuid4())
        customer_ids[item.get('email')] = customer_id  # Map email to customer_id
        
        

        # Extract values from the JSON response, including nested properties
        customer_data = {
            'customer_id': customer_id,
            'total_spend_cents': item.get('total_spend_cents'),
            'total_purchases': item.get('total_purchases'),
            'perks_redeemed': item.get('perks_redeemed'),
            'last_purchase_at': item.get('last_purchase_at'),
            'first_name': item.get('first_name'),
            'last_name': item.get('last_name'),
            'phone_number': item.get('phone_number'),
            'email': item.get('email'),
            'points_balance': item.get('points_balance'),
            'points_earned': item.get('points_earned'),
            'last_seen_at': item.get('last_seen_at'),
            'third_party_id': item.get('third_party_id'),  
            'pos_account_id': item.get('pos_account_id'),
            'has_store_account': item.get('has_store_account'),
            'credit_balance': item.get('credit_balance'),
            'credit_balance_in_customer_currency': item.get('credit_balance_in_customer_currency'),
            'opt_in': item.get('opt_in'),
            'opted_in_at': item.get('opted_in_at'),
            'points_expire_at': item.get('points_expire_at'),

            # Extracting VIP tier values from nested structures
            'vip_tier_name': item.get('vip_tier_name'),
            'vip_tier_entry_date': item.get('vip_tier_entry_date'),
            'vip_tier_expiration': item.get('vip_tier_expiration'),
            'vip_tier_actions_completed_points_earned': item.get('vip_tier_actions_completed', {}).get('points_earned'),
            'vip_tier_actions_completed_amount_spent_cents': item.get('vip_tier_actions_completed', {}).get('amount_spent_cents'),
            'vip_tier_actions_completed_amount_spent_cents_in_customer_currency': item.get('vip_tier_actions_completed', {}).get('amount_spent_cents_in_customer_currency'),
            'vip_tier_actions_completed_purchases_made': item.get('vip_tier_actions_completed', {}).get('purchases_made'),
            'vip_tier_actions_completed_referrals_completed': item.get('vip_tier_actions_completed', {}).get('referrals_completed'),

            'vip_tier_maintenance_requirements_points_needed': item.get('vip_tier_maintenance_requirements', {}).get('points_needed',0),
            'vip_tier_maintenance_requirements_amount_cents_needed': item.get('vip_tier_maintenance_requirements', {}).get('amount_cents_needed',0),
            'vip_tier_maintenance_requirements_amount_cents_needed_in_customer_currency': item.get('vip_tier_maintenance_requirements', {}).get('amount_cents_needed_in_customer_currency',0),
            'vip_tier_maintenance_requirements_purchases_needed': item.get('vip_tier_maintenance_requirements', {}).get('purchases_needed',0),
            'vip_tier_maintenance_requirements_referrals_needed': item.get('vip_tier_maintenance_requirements', {}).get('referrals_needed',0),

            'vip_tier_upgrade_requirements_points_needed': item.get('vip_tier_upgrade_requirements', {}).get('points_needed'),
            'vip_tier_upgrade_requirements_amount_cents_needed': item.get('vip_tier_upgrade_requirements', {}).get('amount_cents_needed'),
            'vip_tier_upgrade_requirements_amount_cents_needed_in_customer_currency': item.get('vip_tier_upgrade_requirements', {}).get('amount_cents_needed_in_customer_currency'),
            'vip_tier_upgrade_requirements_purchases_needed': item.get('vip_tier_upgrade_requirements', {}).get('purchases_needed'),
            'vip_tier_upgrade_requirements_referrals_needed': item.get('vip_tier_upgrade_requirements', {}).get('referrals_needed'),
        }

        stmt = insert(customer_history_table).values(customer_data)
        conn.execute(stmt)
    conn.commit()
    return customer_ids
 

def save_history_items(history_items_data, history_items_table, orders_table,conn):
    try:
        for item in history_items_data:
            history_item_id = str(uuid.uuid4())
            history_item_data = {
                'customer_id': item.get('customer_id'),  # Use customer_id from the item
                'history_item_id': history_item_id,
                'created_at': item.get('created_at'),
                'date': item.get('date'),
                'completed_at': item.get('completed_at'),
                'action': item.get('action'),
                'points': item.get('points'),
                'status': item.get('status'),
                'action_name': item.get('action_name'),
            }

            stmt = insert(history_items_table).values(history_item_data)
            conn.execute(stmt)
            
            
             # Now save order IDs
            order_ids = item.get('order_ids', [])
            for order_id in order_ids:
                order_data = {
                    'history_item_id': history_item_id,
                    'order_ids': order_id
                }
                stmt = insert(orders_table).values(order_data)
                conn.execute(stmt)

        conn.commit()
        print("History Items and Order IDs Data Saved Successfully")

    except Exception as e:
        print(f"Error saving history items: {e}")



