import json
import time
import random
import uuid
from datetime import datetime
from confluent_kafka import Producer
from faker import Faker

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_VIDEOS = 'cdc.content.videos'
TOPIC_USERS = 'cdc.users.profiles'

# --- Domain Constants ---
CATEGORIES = ["Gaming", "Beauty", "Comedy", "Education", "Sports", "Tech", "Dance"]
REGIONS = ["US", "UK", "JP", "BR", "DE", "FR", "IN"]
LTV_SEGMENTS = ["Standard", "High_Potential", "VIP"]
DEVICE_OS = ["iOS", "Android"]

fake = Faker()

# Kafka Producer Config
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'python-cdc-generator',
}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')

class CDCGenerator:
    def __init__(self):
        self.user_ids = []
        self.video_ids = []

    def generate_user(self, op='c', user_id=None):
        """
        Generates a User Profile CDC event.
        op: 'c' (Create) or 'u' (Update)
        """
        if op == 'c':
            user_id = f"u_{uuid.uuid4().hex[:8]}"
            self.user_ids.append(user_id)
            is_creator = random.choice([True, False])
            segment = "Standard"
        else:
            # Update existing user
            if not self.user_ids: return None
            user_id = user_id or random.choice(self.user_ids)
            is_creator = True # Upgraded to creator
            segment = random.choice(LTV_SEGMENTS)

        payload = {
            "op": op,
            "ts_ms": int(time.time() * 1000),
            "after": {
                "user_id": user_id,
                "register_country": random.choice(REGIONS),
                "device_os": random.choice(DEVICE_OS),
                "is_creator": is_creator,
                "ltv_segment": segment,
                "join_at": datetime.utcnow().isoformat()
            }
        }
        return user_id, payload

    def generate_video(self, op='c', video_id=None):
        """
        Generates a Video Metadata CDC event.
        """
        if op == 'c':
            video_id = f"v_{uuid.uuid4().hex[:8]}"
            self.video_ids.append(video_id)
            status = "active"
            # Must link to an existing user if possible
            creator_id = random.choice(self.user_ids) if self.user_ids else f"u_{uuid.uuid4().hex[:8]}"
        else:
            if not self.video_ids: return None
            video_id = video_id or random.choice(self.video_ids)
            status = random.choice(["active", "banned", "copyright_strike"])
            creator_id = None # Not changing owner on update usually

        payload = {
            "op": op,
            "ts_ms": int(time.time() * 1000),
            "after": {
                "video_id": video_id,
                "creator_id": creator_id, # Only present on create usually, but keeping simple
                "category": random.choice(CATEGORIES),
                "hashtags": [f"#{fake.word()}" for _ in range(random.randint(1, 5))],
                "duration_ms": random.randint(5000, 60000),
                "status": status,
                "upload_time": datetime.now(datetime.timezone.utc).isoformat()
            }
        }
        
        # On update, we might not send all fields in real CDC, but for this Upsert logic we send full row
        if op == 'u':
            # Keep creator_id consistent for updates in this mock
            payload['after']['creator_id'] = "existing_id_placeholder" 

        return video_id, payload

    def bootstrap(self, n_users=100, n_videos=50):
        print(f"--- Bootstrapping {n_users} Users and {n_videos} Videos ---")
        
        # 1. Create Users
        for _ in range(n_users):
            uid, event = self.generate_user(op='c')
            producer.produce(TOPIC_USERS, key=uid, value=json.dumps(event), on_delivery=delivery_report)
        
        producer.flush()
        print("Users seeded.")

        # 2. Create Videos (linked to users)
        for _ in range(n_videos):
            vid, event = self.generate_video(op='c')
            producer.produce(TOPIC_VIDEOS, key=vid, value=json.dumps(event), on_delivery=delivery_report)
        
        producer.flush()
        print("Videos seeded.")

    def run_continuous(self):
        print("--- Starting Continuous CDC Stream ---")
        try:
            while True:
                # 80% chance to create new content/user, 20% update
                if random.random() < 0.8:
                    # Create New
                    if random.random() < 0.7: # More videos than users
                        vid, event = self.generate_video(op='c')
                        topic = TOPIC_VIDEOS
                        key = vid
                        print(f"[NEW VIDEO] {vid} ({event['after']['category']})")
                    else:
                        uid, event = self.generate_user(op='c')
                        topic = TOPIC_USERS
                        key = uid
                        print(f"[NEW USER] {uid}")
                else:
                    # Update Existing
                    if random.random() < 0.5:
                        vid, event = self.generate_video(op='u')
                        if vid:
                            topic = TOPIC_VIDEOS
                            key = vid
                            print(f"[UPDATE VIDEO] {vid} -> {event['after']['status']}")
                    else:
                        uid, event = self.generate_user(op='u')
                        if uid:
                            topic = TOPIC_USERS
                            key = uid
                            print(f"[UPDATE USER] {uid} -> {event['after']['ltv_segment']}")

                if 'topic' in locals():
                    producer.produce(topic, key=key, value=json.dumps(event), on_delivery=delivery_report)
                    producer.poll(0)
                
                time.sleep(1) # Slow trickle

        except KeyboardInterrupt:
            print("Stopping CDC Generator...")
            producer.flush()

if __name__ == "__main__":
    gen = CDCGenerator()
    # Initial Load
    gen.bootstrap(n_users=1000, n_videos=500)
    # Continuous Stream
    gen.run_continuous()