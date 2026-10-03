import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "site_events"))
os.environ.setdefault("POWERTOOLS_METRICS_NAMESPACE", "SiteEvents")
os.environ.setdefault("POWERTOOLS_SERVICE_NAME", "site-events")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
