"""Simulate website traffic against the site-events API.

Each simulated visitor runs a session: page views, searches, clicks, cart
activity and sometimes a purchase. Every action is POSTed as JSON to the
events endpoint, either `sam local start-api` or the deployed API Gateway URL.
"""
import argparse
import json
import os
import random
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor

EVENTS_URL = os.getenv("EVENTS_URL", "http://127.0.0.1:3000/events")

PAGES = ["/", "/catalog", "/catalog/shoes", "/catalog/jackets", "/about", "/help", "/blog"]
SEARCH_TERMS = ["running shoes", "winter jacket", "socks", "backpack", "sale", "gift card"]
PRODUCTS = [
    {"sku": "SKU-1001", "name": "Trail Runner", "price": 89.99},
    {"sku": "SKU-1002", "name": "City Sneaker", "price": 64.50},
    {"sku": "SKU-2001", "name": "Down Jacket", "price": 199.00},
    {"sku": "SKU-3001", "name": "Wool Socks", "price": 12.00},
    {"sku": "SKU-4001", "name": "Day Pack", "price": 54.95},
]
DEVICES = ["desktop", "mobile", "tablet"]
ERRORS = [("payment_declined", 402), ("not_found", 404), ("server_error", 500)]


def post_event(url, payload):
    """POST one event. Returns (http_status, parsed_body)."""
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        try:
            body = json.loads(exc.read() or b"{}")
        except ValueError:
            body = {}
        return exc.code, body


class Visitor:
    def __init__(self, user_id, args):
        self.user_id = user_id
        self.session_id = str(uuid.uuid4())
        self.device = random.choice(DEVICES)
        self.cart = []
        self.args = args
        self.sent = 0
        self.failed = 0

    def emit(self, event_type, **properties):
        payload = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "ts": int(time.time() * 1000),
            "user_id": self.user_id,
            "session_id": self.session_id,
            "properties": {"device": self.device, **properties},
        }
        try:
            status, body = post_event(self.args.url, payload)
        except (urllib.error.URLError, ValueError, TimeoutError) as exc:
            status, body = None, {"error": str(exc)}
        ok = status is not None and 200 <= status < 300
        self.sent += ok
        self.failed += not ok
        if self.args.verbose or not ok:
            print(f"[{self.user_id}] {event_type:<17} -> {status} {body}", flush=True)
        time.sleep(random.uniform(*self.args.think_time))

    def run(self):
        logged_in = self.user_id is not None and random.random() < 0.5
        self.emit("page_view", page="/", referrer=random.choice(["google", "direct", "email", "ads"]))

        if self.user_id is None and random.random() < 0.15:
            self.user_id = f"user-{uuid.uuid4().hex[:8]}"
            self.emit("signup", method=random.choice(["email", "google", "apple"]))
            logged_in = True
        elif self.user_id and not logged_in and random.random() < 0.6:
            self.emit("login", method="password")
            logged_in = True

        for _ in range(random.randint(1, 6)):
            roll = random.random()
            if roll < 0.35:
                self.emit("page_view", page=random.choice(PAGES))
            elif roll < 0.55:
                term = random.choice(SEARCH_TERMS)
                self.emit("search", query=term, results=random.randint(0, 40))
            elif roll < 0.75:
                product = random.choice(PRODUCTS)
                self.emit("click", element="product_card", sku=product["sku"])
            elif roll < 0.92:
                product = random.choice(PRODUCTS)
                qty = random.randint(1, 3)
                self.cart.append({**product, "qty": qty})
                self.emit("add_to_cart", sku=product["sku"], qty=qty, price=product["price"])
            elif self.cart:
                item = self.cart.pop(random.randrange(len(self.cart)))
                self.emit("remove_from_cart", sku=item["sku"], qty=item["qty"])

        if self.cart and random.random() < 0.6:
            total = round(sum(i["price"] * i["qty"] for i in self.cart), 2)
            self.emit("checkout_started", items=len(self.cart), total=total)
            if random.random() < 0.15:
                code, http = random.choice(ERRORS)
                self.emit("error", code=code, http_status=http, page="/checkout")
            elif random.random() < 0.8:
                self.emit(
                    "purchase",
                    order_id=f"ord-{uuid.uuid4().hex[:10]}",
                    items=[{"sku": i["sku"], "qty": i["qty"]} for i in self.cart],
                    total=total,
                    currency="USD",
                )

        if self.args.bad_events and random.random() < self.args.bad_events:
            self.emit("bogus_event", note="intentionally invalid")

        if logged_in and random.random() < 0.3:
            self.emit("logout")
        return self.sent, self.failed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sessions", type=int, default=20, help="sessions to simulate (0 = forever)")
    parser.add_argument("--concurrency", type=int, default=4, help="parallel visitors")
    parser.add_argument("--known-users", type=int, default=50, help="size of returning-user pool")
    parser.add_argument("--think-time", type=float, nargs=2, default=(0.05, 0.4),
                        metavar=("MIN", "MAX"), help="seconds between a visitor's actions")
    parser.add_argument("--bad-events", type=float, default=0.05,
                        help="probability a session also sends an invalid event")
    parser.add_argument("--url", default=EVENTS_URL, help="events endpoint (env EVENTS_URL)")
    parser.add_argument("-v", "--verbose", action="store_true", help="print every response")
    args = parser.parse_args()

    known_users = [f"user-{i:04d}" for i in range(args.known_users)]

    def new_visitor():
        user_id = random.choice(known_users) if random.random() < 0.6 else None
        return Visitor(user_id, args).run()

    print(f"Simulating traffic against {args.url}", flush=True)
    sent = failed = done = 0
    started = time.time()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        while args.sessions == 0 or done < args.sessions:
            batch = args.concurrency if args.sessions == 0 else min(args.concurrency, args.sessions - done)
            for s, f in pool.map(lambda _: new_visitor(), range(batch)):
                sent += s
                failed += f
            done += batch
            print(f"sessions={done} accepted={sent} rejected={failed} "
                  f"elapsed={time.time() - started:.1f}s", flush=True)


if __name__ == "__main__":
    main()
