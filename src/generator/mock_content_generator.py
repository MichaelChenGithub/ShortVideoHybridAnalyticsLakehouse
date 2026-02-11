import json
import time
import random
import uuid
import numpy as np
from datetime import datetime
from confluent_kafka import Producer
try:
    from pyiceberg.catalog import load_catalog
except ImportError:
    print("Warning: pyiceberg not installed. Install with: pip install 'pyiceberg[s3fs]'")

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_EVENTS = 'content_events'
EVENTS_PER_SEC = 50  # Adjust for load testing
ICEBERG_REST_URI = "http://iceberg-rest:8181"
MINIO_ENDPOINT = "http://minio:9000"
MINIO_CREDS = {"admin": "admin", "password": "password"}
POOL_SIZE_LIMIT = 50000  # Cap the number of IDs in memory to prevent OOM

# --- Simulation Constants ---
# NUM_VIDEOS/USERS determined by Iceberg data
ZIPF_PARAM = 1.5  # Higher = more skewed (more viral supernovas)

# Global pools (managed by Generator class)
VIDEO_POOL = []
USER_POOL = []

# Kafka Producer Config
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'python-content-gen',
    'linger.ms': 10,  # Batching for throughput
    'compression.type': 'lz4'
}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')

class ContentEventGenerator:
    def __init__(self):
        self.video_probs = None
        self.last_refresh = time.time()
        self.video_metadata = {}
        self.catalog = None
        try:
            self.catalog = load_catalog("default", **{
                "type": "rest",
                "uri": ICEBERG_REST_URI,
                "s3.endpoint": MINIO_ENDPOINT,
                "s3.access-key-id": MINIO_CREDS["admin"],
                "s3.secret-access-key": MINIO_CREDS["password"]
            })
        except Exception as e:
            print(f"Catalog init failed: {e}")

        self.refresh_pools()

    def refresh_pools(self):
        """Hydrate ID pools from Iceberg Dimension Tables"""
        global VIDEO_POOL, USER_POOL
        try:
            if not self.catalog: return
            
            # Load Videos
            tbl_v = self.catalog.load_table("lakehouse.dims.dim_videos")
            rows = tbl_v.scan(selected_fields=("video_id", "duration_ms"), limit=POOL_SIZE_LIMIT).to_arrow().to_pylist()
            VIDEO_POOL = [r["video_id"] for r in rows]
            self.video_metadata = {r["video_id"]: r.get("duration_ms", 60000) for r in rows}
            # Load Users
            tbl_u = self.catalog.load_table("lakehouse.dims.dim_users")
            USER_POOL = [r["user_id"] for r in tbl_u.scan(selected_fields=("user_id",), limit=POOL_SIZE_LIMIT).to_arrow().to_pylist()]
            
            print(f"Refreshed Pools from Iceberg: {len(VIDEO_POOL)} Videos, {len(USER_POOL)} Users")
            self._recalc_zipf()
        except Exception as e:
            print(f"Iceberg refresh failed (Tables might not exist yet): {e}")

    def _recalc_zipf(self):
        if not VIDEO_POOL: return
        ranks = np.arange(1, len(VIDEO_POOL) + 1)
        weights = 1.0 / np.power(ranks, ZIPF_PARAM)
        self.video_probs = weights / np.sum(weights)
        print(f"Zipf Distribution Updated. Top 1% videos get {np.sum(self.video_probs[:int(len(VIDEO_POOL)*0.01)])*100:.1f}% of traffic.")

    def generate_session(self):
        """
        Simulates a user session: Impression -> Play -> (Like/Share/Skip)
        """
        # 1. Pick a User (Uniform)
        if not USER_POOL: return []
        user_id = random.choice(USER_POOL)
        
        # 2. Pick a Video (Zipfian - simulating recommendation algo)
        if not VIDEO_POOL: return []
        video_id = np.random.choice(VIDEO_POOL, p=self.video_probs)
        duration = self.video_metadata.get(video_id, 60000) or 60000
        
        base_event = {
            "event_id": str(uuid.uuid4()),
            "event_timestamp": datetime.now(datetime.timezone.utc).isoformat(),
            "video_id": video_id,
            "user_id": user_id,
            "payload": {
                "device_os": random.choice(["iOS", "Android"]),
                "app_version": "14.2.0",
                "network_type": random.choice(["5G", "WiFi", "4G"])
            }
        }

        events = []

        # --- Event 1: Impression ---
        # Always happens
        imp = base_event.copy()
        imp["event_type"] = "impression"
        imp["payload"]["watch_time_ms"] = 0
        events.append(imp)

        # --- Event 2: Play Start ---
        # 85% chance user stops scrolling and watches
        if random.random() < 0.85:
            play = base_event.copy()
            play["event_id"] = str(uuid.uuid4())
            play["event_type"] = "play_start"
            events.append(play)

            # --- Event 3: Engagement (Like/Share) ---
            # Logic: You can't like if you didn't play
            
            # Like (8% chance)
            if random.random() < 0.08:
                like = base_event.copy()
                like["event_id"] = str(uuid.uuid4())
                like["event_type"] = "like"
                like["payload"]["watch_time_ms"] = random.randint(0, duration) # Liked during watch
                events.append(like)
            
            # Share (1.5% chance)
            if random.random() < 0.015:
                share = base_event.copy()
                share["event_id"] = str(uuid.uuid4())
                share["event_type"] = "share"
                share["payload"]["watch_time_ms"] = random.randint(0, duration)
                events.append(share)

            # Play Finish (Completion)
            # 40% chance to finish
            if random.random() < 0.40:
                finish = base_event.copy()
                finish["event_id"] = str(uuid.uuid4())
                finish["event_type"] = "play_finish"
                finish["payload"]["watch_time_ms"] = duration
                events.append(finish)

        return events

    def run(self):
        print(f"--- Starting Content Event Stream ({EVENTS_PER_SEC}/sec) ---")
        try:
            while True:
                start_time = time.time()
                
                # Generate batch of sessions
                # Each session produces 1-4 events
                # To hit target rate, we estimate sessions needed
                sessions_needed = max(1, int(EVENTS_PER_SEC / 2.5)) 
                
                # Periodic Refresh (every 60s)
                if time.time() - self.last_refresh > 60:
                    self.refresh_pools()
                    self.last_refresh = time.time()

                for _ in range(sessions_needed):
                    session_events = self.generate_session()
                    for event in session_events:
                        producer.produce(
                            TOPIC_EVENTS, 
                            key=event['video_id'], # Partition by Video ID for Spark ordering
                            value=json.dumps(event), 
                            on_delivery=delivery_report
                        )
                
                producer.poll(0)
                
                # Throttle
                elapsed = time.time() - start_time
                sleep_time = max(0, 1.0 - elapsed)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    
        except KeyboardInterrupt:
            print("Stopping Generator...")
            producer.flush()

if __name__ == "__main__":
    # Ensure numpy is installed: pip install numpy
    gen = ContentEventGenerator()
    gen.run()
