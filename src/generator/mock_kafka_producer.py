import json
import time
import random
import uuid
from datetime import datetime
from confluent_kafka import Producer
from faker import Faker

# --- Configuration ---
KAFKA_TOPIC = 'orders'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # 注意：Python 在外部，連 9092
GENERATION_INTERVAL = 0.5  # 每 0.5 秒產生一筆 (可自行調整快慢)

# --- Setup ---
fake = Faker()
# key: order_id, value: {'status': 'CREATED', 'user_id': 'u-123'}
order_cache = {}  
# 狀態流轉規則
STATUS_FLOW = {
    'CREATED': ['PAID', 'CANCELLED'],
    'PAID': ['SHIPPED', 'RETURNED'],
    'SHIPPED': [],    # final state
    'CANCELLED': [],  # final state
    'RETURNED': []    # final state
}

# Kafka Producer Config
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'python-mock-generator',
}
producer = Producer(conf)

def delivery_report(err, msg):
    """Kafka Callback: Check message delivery result"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        # print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
        pass

def generate_event():
    """Generate either a new order event or an update to an existing order."""

    # 70% create new order, 30% update existing order
    if not order_cache or random.random() < 0.7:
        return create_new_order()
    else:
        return update_existing_order()

def create_new_order():
    order_id = f"ord-{uuid.uuid4().hex[:8]}"
    user_id = f"u-{random.randint(1000, 9999)}"
    
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_CREATED",
        "event_timestamp": int(time.time()),
        "order_id": order_id,
        "user_id": user_id,
        "total_amount": round(random.uniform(50, 500), 2),
        "currency": "USD",
        "payment_method": random.choice(["CREDIT_CARD", "PAYPAL", "APPLE_PAY"]),
        "items": [
            {
                "sku": f"SKU-{random.randint(100, 999)}",
                "quantity": random.randint(1, 5),
                "unit_price": round(random.uniform(10, 100), 2),
                "category": random.choice(["Electronics", "Books", "Home", "Clothing"])
            } 
            for _ in range(random.randint(1, 3)) # random 1-3 items
        ],
        "current_status": "CREATED"
    }
    
    # Store in Cache
    order_cache[order_id] = {
        "status": "CREATED",
        "user_id": user_id
    }
    return event

def update_existing_order():
    # Pick a random active order from cache
    active_orders = [oid for oid, data in order_cache.items() if STATUS_FLOW[data['status']]]
    
    if not active_orders:
        return create_new_order() # if no active orders, create new one
        
    order_id = random.choice(active_orders)
    order_data = order_cache[order_id]
    current_status = order_data['status']
    user_id = order_data['user_id']
    
    # Determine next status
    next_status = random.choice(STATUS_FLOW[current_status])
    
    # Mapping status to event_type
    event_type_map = {
        'PAID': 'ORDER_PAID',
        'CANCELLED': 'ORDER_CANCELLED',
        'SHIPPED': 'ORDER_SHIPPED',
        'RETURNED': 'ORDER_RETURNED'
    }
    
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type_map[next_status],
        "event_timestamp": int(time.time()),
        "order_id": order_id,
        "user_id": user_id,
        "current_status": next_status
    }

    # Update Cache
    order_cache[order_id]['status'] = next_status
    
    # If final state, remove from cache
    if not STATUS_FLOW[next_status]:
        del order_cache[order_id]
        
    return event

if __name__ == '__main__':
    print("Starting Mock Data Generator...")

    try:
        while True:
            event = generate_event()
            
            # *** assign key as order_id for correct distribution ***
            producer.produce(
                topic=KAFKA_TOPIC,
                key=event['order_id'],
                value=json.dumps(event),
                on_delivery=delivery_report
            )
            
            producer.poll(0)
            
            print(f"Sent: {event['event_type']:<15} | Ord: {event['order_id']} | User: {event['user_id']} | Status: {event.get('current_status')}")
            
            time.sleep(GENERATION_INTERVAL)
            
    except KeyboardInterrupt:
        print("\n Stopping generator...")
    finally:
        producer.flush()