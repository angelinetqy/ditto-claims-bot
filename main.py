"""Ditto Deals claims bot - Stages 1 + 2 + 3
Stage 1: post Shopify items to the Telegram channel.
Stage 2: detect "claim" comments, sync stock with Shopify, reply to buyers.
Stage 3: auto invoices (Shopify draft orders), payment check, no-show release, strikes.
"""
import asyncio
import html
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone

import httpx
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    MessageOriginChannel,
    ReplyParameters,
    Update,
)
from telegram.error import BadRequest, Forbidden
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ditto")

# ---- config (Railway environment variables) ----
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
SHOP = os.environ.get("SHOPIFY_STORE_DOMAIN", "dittodealsstore.myshopify.com")
CLIENT_ID = os.environ["SHOPIFY_CLIENT_ID"]
CLIENT_SECRET = os.environ["SHOPIFY_CLIENT_SECRET"]
CHANNEL = os.environ.get("CHANNEL_USERNAME", "@dittodeals")
ADMIN_IDS = {int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()}
DB_PATH = os.environ.get("DB_PATH", "/data/bot.db")
API_VERSION = os.environ.get("SHOPIFY_API_VERSION", "2026-01")
SALE_TAG = os.environ.get("SALE_TAG", "telegram")

POST_DELAY = int(os.environ.get("POST_DELAY", "30"))  # seconds between channel posts
INVOICE_DELAY_MIN = int(os.environ.get("INVOICE_DELAY_MIN", "15"))  # after buyer's LAST claim
START_DEADLINE_MIN = int(os.environ.get("START_DEADLINE_MIN", "60"))  # to press Start on the bot
PAY_DEADLINE_MIN = int(os.environ.get("PAY_DEADLINE_MIN", "60"))  # to pay after invoice is sent
CONFIRM_WINDOW_HOURS = int(os.environ.get("CONFIRM_WINDOW_HOURS", "12"))  # time you get to confirm a checked-out order is paid
STRIKE_LIMIT = int(os.environ.get("STRIKE_LIMIT", "3"))
REPOST_COOLDOWN_MIN = int(os.environ.get("REPOST_COOLDOWN_MIN", "60"))  # /post skips cards posted this recently (guards against double runs)
KEEP_OLD_POSTS = os.environ.get("KEEP_OLD_POSTS", "1") == "1"  # 1 = keep old posts and keep them updated; 0 = delete them
SYNC_INTERVAL_MIN = int(os.environ.get("SYNC_INTERVAL_MIN", "5"))  # how often posts are refreshed from Shopify
SYNC_WINDOW_DAYS = int(os.environ.get("SYNC_WINDOW_DAYS", "30"))  # only posts newer than this are auto-updated
OFFERS_ON_ALL = os.environ.get("OFFERS_ON_ALL", "1") == "1"  # 1 = every card accepts offers; 0 = only cards tagged "offers"
OFFER_TTL = int(os.environ.get("OFFER_EXPIRY_HOURS", "24")) * 3600  # offers/counters expire after this


stock_lock = asyncio.Lock()  # one stock change at a time
_invoice_alerted = {}  # user_id -> last time we told admins an invoice failed


def now() -> int:
    return int(time.time())


# ---------------- database ----------------
def ensure_column(conn, table, col, decl):
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
    if col not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")


def db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS posts (
            product_id TEXT PRIMARY KEY,
            variant_id TEXT,
            channel_msg_id INTEGER,
            title TEXT,
            price TEXT,
            posted_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS threads (
            group_chat_id INTEGER,
            group_msg_id INTEGER,
            channel_msg_id INTEGER,
            PRIMARY KEY (group_chat_id, group_msg_id)
        );
        CREATE TABLE IF NOT EXISTS claims (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT,
            variant_id TEXT,
            title TEXT,
            price REAL,
            qty INTEGER,
            user_id INTEGER,
            user_name TEXT,
            group_chat_id INTEGER,
            group_msg_id INTEGER,
            claimed_at INTEGER,
            status TEXT DEFAULT 'unpaid',
            UNIQUE (group_chat_id, group_msg_id)
        );
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            chat_id INTEGER,
            started_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            user_name TEXT,
            chat_id INTEGER,
            draft_id TEXT,
            invoice_url TEXT,
            total REAL,
            created_at INTEGER,
            reminded INTEGER DEFAULT 0,
            alerted INTEGER DEFAULT 0,
            status TEXT DEFAULT 'sent'
        );
        CREATE TABLE IF NOT EXISTS post_variants (
            variant_id TEXT PRIMARY KEY,
            product_id TEXT,
            name TEXT,
            price REAL,
            position INTEGER
        );
        CREATE TABLE IF NOT EXISTS offers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT,
            variant_id TEXT,
            title TEXT,
            listed_price REAL,
            offer_price REAL,
            counter_price REAL,
            final_price REAL,
            user_id INTEGER,
            user_name TEXT,
            group_chat_id INTEGER,
            group_msg_id INTEGER,
            status TEXT DEFAULT 'pending',
            created_at INTEGER,
            updated_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS post_copies (
            channel_msg_id INTEGER PRIMARY KEY,
            product_id TEXT
        );
        CREATE TABLE IF NOT EXISTS strikes (
            user_id INTEGER PRIMARY KEY,
            name TEXT,
            count INTEGER DEFAULT 0
        );
        """
    )
    ensure_column(conn, "claims", "invoice_id", "INTEGER")
    ensure_column(conn, "claims", "list_price", "REAL")
    ensure_column(conn, "claims", "offer_id", "INTEGER")
    ensure_column(conn, "posts", "offers_enabled", "INTEGER DEFAULT 0")
    ensure_column(conn, "posts", "sold_out", "INTEGER DEFAULT 0")
    ensure_column(conn, "posts", "last_caption", "TEXT")
    return conn


def get_strikes(user_id: int) -> int:
    conn = db()
    row = conn.execute("SELECT count FROM strikes WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return row[0] if row else 0


def add_strike(user_id: int, name: str) -> int:
    conn = db()
    conn.execute(
        "INSERT INTO strikes (user_id, name, count) VALUES (?,?,1) "
        "ON CONFLICT(user_id) DO UPDATE SET count = count + 1, name = excluded.name",
        (user_id, name),
    )
    conn.commit()
    count = conn.execute("SELECT count FROM strikes WHERE user_id=?", (user_id,)).fetchone()[0]
    conn.close()
    return count


# ---------------- Shopify ----------------
class Shopify:
    def __init__(self):
        self.token = None
        self.expires = 0

    async def _get_token(self, client: httpx.AsyncClient) -> str:
        if self.token and time.time() < self.expires - 300:
            return self.token
        r = await client.post(
            f"https://{SHOP}/admin/oauth/access_token",
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
        )
        r.raise_for_status()
        d = r.json()
        self.token = d["access_token"]
        self.expires = time.time() + int(d.get("expires_in", 86000))
        return self.token

    async def gql(self, query: str, variables: dict | None = None) -> dict:
        async with httpx.AsyncClient(timeout=30) as c:
            token = await self._get_token(c)
            r = await c.post(
                f"https://{SHOP}/admin/api/{API_VERSION}/graphql.json",
                headers={"X-Shopify-Access-Token": token},
                json={"query": query, "variables": variables or {}},
            )
            r.raise_for_status()
            data = r.json()
            if "errors" in data:
                raise RuntimeError(data["errors"])
            return data["data"]


shopify = Shopify()

PRODUCTS_QUERY = """
query($q: String!) {
  products(first: 50, query: $q, sortKey: CREATED_AT, reverse: false) {
    nodes {
      id
      title
      tags
      featuredImage { url }
      variants(first: 20) { nodes { id title price inventoryQuantity } }
    }
  }
}
"""

VARIANT_QUERY = """
query($id: ID!) {
  productVariant(id: $id) {
    inventoryQuantity
    inventoryItem {
      id
      inventoryLevels(first: 1) { nodes { location { id } } }
    }
  }
}
"""

ADJUST_MUTATION = """
mutation($input: InventoryAdjustQuantitiesInput!) {
  inventoryAdjustQuantities(input: $input) {
    userErrors { field message }
  }
}
"""

DRAFT_CREATE = """
mutation($input: DraftOrderInput!) {
  draftOrderCreate(input: $input) {
    draftOrder { id name invoiceUrl }
    userErrors { field message }
  }
}
"""

DRAFT_STATUS = """
query($id: ID!) {
  draftOrder(id: $id) {
    status
    order { id name displayFinancialStatus }
  }
}
"""

DRAFT_DELETE = """
mutation($input: DraftOrderDeleteInput!) {
  draftOrderDelete(input: $input) {
    deletedId
    userErrors { field message }
  }
}
"""

ORDER_PAID = """
mutation($input: OrderMarkAsPaidInput!) {
  orderMarkAsPaid(input: $input) {
    order { id }
    userErrors { field message }
  }
}
"""

ORDER_CANCEL = """
mutation($orderId: ID!) {
  orderCancel(orderId: $orderId, reason: OTHER, refund: false, restock: true) {
    job { id }
    orderCancelUserErrors { field message }
  }
}
"""


PRODUCT_VARIANTS_QUERY = """
query($id: ID!) {
  product(id: $id) {
    variants(first: 20) { nodes { id title price inventoryQuantity } }
  }
}
"""


def _variants_from_nodes(nodes):
    multi = len(nodes) > 1
    return [
        {
            "id": v["id"],
            "name": v["title"] if multi else None,
            "price": float(v["price"]),
            "qty": int(v["inventoryQuantity"] or 0),
        }
        for v in nodes
    ]


def natural_key(title: str):
    """Case-insensitive A-Z sort where numbers sort as numbers (9 before 10)."""
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", title.casefold())]


async def fetch_sale_products():
    data = await shopify.gql(PRODUCTS_QUERY, {"q": f"tag:{SALE_TAG} AND status:active"})
    items = []
    for p in data["products"]["nodes"]:
        if not p["variants"]["nodes"]:
            continue
        items.append(
            {
                "product_id": p["id"],
                "title": p["title"],
                "variants": _variants_from_nodes(p["variants"]["nodes"]),
                "offers": OFFERS_ON_ALL or "offers" in [t.lower() for t in p.get("tags", [])],
                "image": (p["featuredImage"] or {}).get("url"),
            }
        )
    return sorted(items, key=lambda i: natural_key(i["title"]))  # A-Z, numbers in order


async def fetch_product_variants(product_id: str):
    d = await shopify.gql(PRODUCT_VARIANTS_QUERY, {"id": product_id})
    p = d["product"]
    return _variants_from_nodes(p["variants"]["nodes"]) if p else []


BULK_QUERY = """
query($ids: [ID!]!) {
  nodes(ids: $ids) {
    ... on Product {
      id
      tags
      variants(first: 20) { nodes { id title price inventoryQuantity } }
    }
  }
}
"""


async def fetch_variants_bulk(product_ids):
    """{product_id: (variants, offers_tag_present)} for many products in a few calls."""
    out = {}
    for i in range(0, len(product_ids), 100):
        d = await shopify.gql(BULK_QUERY, {"ids": product_ids[i : i + 100]})
        for n in d["nodes"]:
            if n and n.get("id") and n.get("variants"):
                out[n["id"]] = (
                    _variants_from_nodes(n["variants"]["nodes"]),
                    OFFERS_ON_ALL or "offers" in [t.lower() for t in n.get("tags", [])],
                )
    return out


def get_post_variants(conn, product_id: str):
    """Variants (with names + posted prices) belonging to a channel post."""
    rows = conn.execute(
        "SELECT variant_id, name, price FROM post_variants WHERE product_id=? ORDER BY position",
        (product_id,),
    ).fetchall()
    if rows:
        return [{"variant_id": r[0], "name": r[1], "price": r[2]} for r in rows]
    row = conn.execute(
        "SELECT variant_id, price FROM posts WHERE product_id=?", (product_id,)
    ).fetchone()  # posts made before variants existed
    return [{"variant_id": row[0], "name": None, "price": float(row[1])}] if row else []


async def get_stock(variant_id: str):
    """Returns (available, inventory_item_id, location_id)."""
    d = await shopify.gql(VARIANT_QUERY, {"id": variant_id})
    v = d["productVariant"]
    levels = v["inventoryItem"]["inventoryLevels"]["nodes"]
    return (
        int(v["inventoryQuantity"] or 0),
        v["inventoryItem"]["id"],
        levels[0]["location"]["id"],
    )


async def adjust_stock(item_id: str, location_id: str, delta: int):
    d = await shopify.gql(
        ADJUST_MUTATION,
        {
            "input": {
                "reason": "correction",
                "name": "available",
                "changes": [
                    {"delta": delta, "inventoryItemId": item_id, "locationId": location_id}
                ],
            }
        },
    )
    errors = d["inventoryAdjustQuantities"]["userErrors"]
    if errors:
        raise RuntimeError(errors)


async def get_draft(draft_id: str):
    d = await shopify.gql(DRAFT_STATUS, {"id": draft_id})
    return d["draftOrder"]  # None if it no longer exists


# ---------------- post text ----------------
def make_caption(item) -> str:
    """item = {"title": str, "offers": bool, "variants": [{"name", "price", "qty"}, ...]}"""
    title = f"✨<b>{html.escape(item['title'])}</b>✨"
    variants = item["variants"]
    offers = item.get("offers")

    if len(variants) == 1:
        v = variants[0]
        if v["qty"] <= 0:
            return f"{title}\n• Price: ${v['price']:.2f}\n• SOLD OUT 🚫"
        text = (
            f"{title}\n"
            f"• Price: ${v['price']:.2f}\n"
            f"• Quantity: {v['qty']}\n\n"
            f"<b>💭How to claim:</b>\n"
            f'• Comment "claim" to claim! 😊'
        )
        if offers:
            text += '\n• Want to haggle? Comment "offer &lt;price&gt;" 💸'
        return text

    lines = []
    for v in variants:
        stock = f"{v['qty']} left" if v["qty"] > 0 else "SOLD OUT 🚫"
        lines.append(f"• {html.escape(v['name'])} — ${v['price']:.2f} ({stock})")
    body = "\n".join(lines)
    if all(v["qty"] <= 0 for v in variants):
        return f"{title}\n{body}\n\n🚫 SOLD OUT"
    cmds = [f'"claim {html.escape(v["name"].lower())}"' for v in variants if v["qty"] > 0]
    joined = cmds[0] if len(cmds) == 1 else ", ".join(cmds[:-1]) + " or " + cmds[-1]
    text = (
        f"{title}\n{body}\n\n"
        f"<b>💭How to claim:</b>\n"
        f"• Comment {joined} to claim! 😊"
    )
    if offers:
        example = html.escape(variants[0]["name"].lower())
        text += f'\n• Want to haggle? Comment "offer {example} &lt;price&gt;" 💸'
    return text


async def refresh_post(bot, channel_msg_id: int, item):
    """Edit the channel post so quantities / SOLD OUT status are current."""
    caption = make_caption(item)
    try:
        await bot.edit_message_caption(
            CHANNEL, channel_msg_id, caption=caption, parse_mode="HTML"
        )
    except BadRequest as e:
        if "not modified" in str(e).lower():
            return
        try:  # post might be text-only
            await bot.edit_message_text(
                caption, CHANNEL, channel_msg_id, parse_mode="HTML"
            )
        except BadRequest as e2:
            if "not modified" not in str(e2).lower():
                log.warning("Could not edit post %s: %s", channel_msg_id, e2)


async def refresh_product_post(bot, product_id: str):
    conn = db()
    row = conn.execute(
        "SELECT channel_msg_id, title, offers_enabled FROM posts WHERE product_id=?",
        (product_id,),
    ).fetchone()
    conn.close()
    if not row:
        return
    try:
        variants = await fetch_product_variants(product_id)
        if variants and all(v["qty"] <= 0 for v in variants):
            c2 = db()
            c2.execute("UPDATE posts SET sold_out=1 WHERE product_id=?", (product_id,))
            c2.commit()
            c2.close()
        if variants:
            item = {"title": row[1], "variants": variants, "offers": OFFERS_ON_ALL or bool(row[2])}
            await refresh_post(bot, row[0], item)
            c4 = db()
            c4.execute(
                "UPDATE posts SET last_caption=? WHERE product_id=?", (make_caption(item), product_id)
            )
            c4.commit()
            c4.close()
            c3 = db()
            copies = c3.execute(
                "SELECT channel_msg_id FROM post_copies WHERE product_id=?", (product_id,)
            ).fetchall()
            c3.close()
            for (copy_id,) in copies:  # older posts of the same card stay in sync
                await refresh_post(bot, copy_id, item)
    except Exception:
        log.exception("Could not refresh post for %s", product_id)


async def refresh_products_for_variants(bot, variant_ids):
    conn = db()
    product_ids = set()
    for vid in variant_ids:
        r = conn.execute(
            "SELECT product_id FROM post_variants WHERE variant_id=? "
            "UNION SELECT product_id FROM posts WHERE variant_id=?",
            (vid, vid),
        ).fetchone()
        if r:
            product_ids.add(r[0])
    conn.close()
    for pid in product_ids:
        await refresh_product_post(bot, pid)


# ---------------- helpers ----------------
def is_admin(update: Update) -> bool:
    return update.effective_user is not None and update.effective_user.id in ADMIN_IDS


def channel_msg_from(msg):
    """If msg is (a copy of) a post from our channel, return the channel message id."""
    if msg is None:
        return None
    origin = msg.forward_origin
    if isinstance(origin, MessageOriginChannel):
        uname = (origin.chat.username or "").lower()
        if f"@{uname}" == CHANNEL.lower():
            return origin.message_id
    return None


def plural(n: int) -> str:
    return "copy" if n == 1 else "copies"


async def notify_admins(bot, text: str, markup=None):
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text, reply_markup=markup)
        except Exception:
            log.warning("Could not message admin %s", admin_id)


async def safe_dm(bot, chat_id: int, text: str, markup=None) -> bool:
    try:
        await bot.send_message(chat_id, text, reply_markup=markup)
        return True
    except (Forbidden, BadRequest) as e:
        log.warning("DM to %s failed: %s", chat_id, e)
        return False


# ---------------- commands ----------------
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if update.effective_chat.type == "private" and user:
        conn = db()
        conn.execute(
            "INSERT OR REPLACE INTO users VALUES (?,?,?)",
            (user.id, update.effective_chat.id, now()),
        )
        conn.commit()
        conn.close()
    await update.message.reply_text(
        "Hi! 💜 You're all set. Your invoice will be sent here shortly after you finish claiming."
    )


async def cmd_myid(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Your Telegram ID: {update.effective_user.id}")


async def retire_old_post(bot, old_msg_id: int) -> str:
    """Remove an outdated channel post. Falls back to editing it if Telegram won't delete it."""
    try:
        await bot.delete_message(CHANNEL, old_msg_id)
        return "deleted"
    except Exception as e:
        log.warning("Could not delete old post %s: %s", old_msg_id, e)
    note = "🔁 This listing was updated. Please see the newest post."
    try:
        await bot.edit_message_caption(CHANNEL, old_msg_id, caption=note)
        return "edited"
    except Exception:
        try:
            await bot.edit_message_text(note, CHANNEL, old_msg_id)
            return "edited"
        except Exception as e2:
            log.warning("Could not edit old post %s: %s", old_msg_id, e2)
    return "failed"


async def cmd_post(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/post        -> post EVERYTHING tagged 'telegram' (A-Z). A card posted before gets a fresh
                    post at its current price/stock; its older post is kept and updated to match
                    (set KEEP_OLD_POSTS=0 to delete older posts instead).
    /post test   -> send to this chat only; nothing is saved or deleted
    /post force  -> also repost cards that were posted in the last few minutes"""
    if not is_admin(update):
        return
    args = [a.lower() for a in ctx.args]
    test, force = "test" in args, "force" in args
    target = update.effective_chat.id if test else CHANNEL

    state = ctx.application.bot_data
    if not test and state.get("post_running"):
        await update.message.reply_text("A post run is already in progress. Please wait for it to finish ⏳")
        return

    await update.message.reply_text("Checking Shopify…")
    try:
        items = await fetch_sale_products()
    except Exception as e:
        log.exception("Shopify fetch failed")
        await update.message.reply_text(f"Couldn't reach Shopify: {e}")
        return

    if not test:
        state["post_running"] = True
    conn = db()
    posted, replaced, skipped, undeleted = 0, 0, [], []
    try:
        for item in items:
            row = conn.execute(
                "SELECT channel_msg_id, posted_at FROM posts WHERE product_id=?",
                (item["product_id"],),
            ).fetchone()
            if row and not test and not force and now() - row[1] < REPOST_COOLDOWN_MIN * 60:
                mins = (now() - row[1]) // 60
                skipped.append(f"{item['title']} (posted {mins} min ago; use /post force)")
                continue
            try:
                msg = await send_item_post(ctx.bot, target, item)
                if not test:
                    first = item["variants"][0]
                    offers = 1 if item.get("offers") else 0
                    if row:
                        conn.execute(
                            "UPDATE posts SET variant_id=?, channel_msg_id=?, title=?, price=?, "
                            "posted_at=?, offers_enabled=?, sold_out=0 WHERE product_id=?",
                            (first["id"], msg.message_id, item["title"], str(first["price"]),
                             now(), offers, item["product_id"]),
                        )
                    else:
                        conn.execute(
                            "INSERT INTO posts (product_id, variant_id, channel_msg_id, title, price, "
                            "posted_at, offers_enabled) VALUES (?,?,?,?,?,?,?)",
                            (item["product_id"], first["id"], msg.message_id, item["title"],
                             str(first["price"]), now(), offers),
                        )
                    for i, v in enumerate(item["variants"]):
                        conn.execute(
                            "INSERT OR REPLACE INTO post_variants VALUES (?,?,?,?,?)",
                            (v["id"], item["product_id"], v["name"], v["price"], i),
                        )
                    conn.execute(
                        "UPDATE posts SET last_caption=? WHERE product_id=?",
                        (make_caption(item), item["product_id"]),
                    )
                    conn.commit()
                    if row:
                        replaced += 1
                        if KEEP_OLD_POSTS:
                            conn.execute(
                                "INSERT OR REPLACE INTO post_copies VALUES (?,?)",
                                (row[0], item["product_id"]),
                            )
                            conn.commit()
                            await refresh_product_post(ctx.bot, item["product_id"])
                        else:
                            outcome = await retire_old_post(ctx.bot, row[0])
                            if outcome != "deleted":
                                undeleted.append(f"{item['title']} ({outcome})")
                posted += 1
                if not test:
                    await asyncio.sleep(POST_DELAY)
            except Exception as e:
                log.exception("Post failed")
                skipped.append(f"{item['title']} ({e})")
    finally:
        conn.close()
        state["post_running"] = False

    where = "this chat (test)" if test else CHANNEL
    text = f"Posted {posted} item(s) to {where}."
    if replaced:
        text += (
            f"\n{replaced} card(s) were posted before: their older posts were kept and updated to the new price/stock."
            if KEEP_OLD_POSTS
            else f"\nReplaced {replaced} older post(s)."
        )
    if undeleted:
        text += (
            "\n⚠️ Couldn't delete these old posts (Telegram may limit deleting older posts), "
            "so I marked them as updated instead. You can delete them by hand:\n"
            + "\n".join(f"• {x}" for x in undeleted)
        )
    if skipped:
        text += "\nSkipped:\n" + "\n".join(f"• {x}" for x in skipped)
    await update.message.reply_text(text)


async def send_item_post(bot, target, item):
    caption = make_caption(item)
    if item["image"]:
        return await bot.send_photo(target, item["image"], caption=caption, parse_mode="HTML")
    return await bot.send_message(target, caption, parse_mode="HTML")


async def cmd_claims(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/claims -> open claims grouped by buyer."""
    if not is_admin(update):
        return
    conn = db()
    rows = conn.execute(
        "SELECT c.user_id, c.user_name, c.title, c.price, c.qty, c.status, "
        "u.user_id IS NOT NULL FROM claims c "
        "LEFT JOIN users u ON u.user_id = c.user_id "
        "WHERE c.status IN ('unpaid','invoiced') ORDER BY c.user_id, c.id"
    ).fetchall()
    conn.close()
    if not rows:
        await update.message.reply_text("No open claims.")
        return
    buyers = {}
    for uid, name, title, price, qty, status, started in rows:
        b = buyers.setdefault(
            uid, {"name": name, "lines": [], "total": 0.0, "started": bool(started), "inv": False}
        )
        b["lines"].append(f"  {qty}× {title} @ ${price:.2f}")
        b["total"] += price * qty
        if status == "invoiced":
            b["inv"] = True
    parts = []
    for b in buyers.values():
        flags = ""
        if not b["started"]:
            flags += " ⚠️ hasn't started bot"
        if b["inv"]:
            flags += " 🧾 invoiced"
        parts.append(f"👤 {b['name']} — ${b['total']:.2f}{flags}\n" + "\n".join(b["lines"]))
    await update.message.reply_text("\n\n".join(parts)[:4000])


async def cmd_sync(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/sync -> refresh quantities on recent channel posts from Shopify."""
    if not is_admin(update):
        return
    cutoff = now() - 14 * 86400
    conn = db()
    rows = conn.execute(
        "SELECT product_id FROM posts WHERE posted_at > ?", (cutoff,)
    ).fetchall()
    conn.close()
    for (product_id,) in rows:
        await refresh_product_post(ctx.bot, product_id)
        await asyncio.sleep(0.5)
    await update.message.reply_text(f"Synced {len(rows)} post(s).")


async def cmd_invoice(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/invoice -> send invoices right now to every buyer who has started the bot."""
    if not is_admin(update):
        return
    await send_due_invoices(ctx.bot, force=True)
    await update.message.reply_text("Invoices sent to everyone who has started the bot.")


async def cmd_strikes(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    conn = db()
    rows = conn.execute(
        "SELECT user_id, name, count FROM strikes WHERE count > 0 ORDER BY count DESC"
    ).fetchall()
    conn.close()
    if not rows:
        await update.message.reply_text("No strikes recorded.")
        return
    lines = [
        f"{'🚫' if c >= STRIKE_LIMIT else '⚠️'} {n} — {c}/{STRIKE_LIMIT} (ID {uid})"
        for uid, n, c in rows
    ]
    await update.message.reply_text("\n".join(lines) + "\n\nClear with /clearstrikes <ID>")


async def cmd_clearstrikes(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not ctx.args or not ctx.args[0].isdigit():
        await update.message.reply_text("Usage: /clearstrikes <ID> (see /strikes)")
        return
    conn = db()
    conn.execute("UPDATE strikes SET count = 0 WHERE user_id = ?", (int(ctx.args[0]),))
    conn.commit()
    conn.close()
    await update.message.reply_text("Strikes cleared ✅")


# ---------------- comments / claims ----------------
async def on_auto_forward(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Telegram copies each channel post into the comments group. Remember which is which."""
    msg = update.message
    channel_msg_id = channel_msg_from(msg)
    if channel_msg_id is None:
        return
    conn = db()
    conn.execute(
        "INSERT OR REPLACE INTO threads VALUES (?,?,?)",
        (msg.chat_id, msg.message_id, channel_msg_id),
    )
    conn.commit()
    conn.close()


CLAIM_START = re.compile(r"^\s*claim\b(.*)$", re.I | re.S)
QTY_ONLY = re.compile(r"^[x×]?\s*(\d{1,3})\s*[x×]?$")
QTY_TRAILING = re.compile(r"^(.*\S)\s+[x×]?(\d{1,3})[x×]?$")
QTY_LEADING = re.compile(r"^[x×]?(\d{1,3})[x×]?\s+(.*\S)$")


def norm(text: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9×]+", " ", text.lower()).split())


def match_variants(name: str, variants):
    if not name:
        return []
    exact = [v for v in variants if norm(v["name"]) == name]
    if len(exact) == 1:
        return exact
    return [
        v for v in variants
        if norm(v["name"]).startswith(name) or name in norm(v["name"]).split()
    ]


def parse_claim(rest: str, variants):
    """Returns (status, variant, qty). status: 'ok' | 'ask' (which one?) | 'ignore'."""
    text = norm(rest)
    if len(variants) == 1:  # normal single-item post: "claim" or "claim 2"
        if text == "":
            return "ok", variants[0], 1
        m = QTY_ONLY.match(text)
        if m and int(m.group(1)) >= 1:
            return "ok", variants[0], int(m.group(1))
        return "ignore", None, None

    if not text:
        return "ask", None, None
    candidates = [(text, 1)]
    m = QTY_TRAILING.match(text)
    if m:
        candidates.append((m.group(1), int(m.group(2))))
    m = QTY_LEADING.match(text)
    if m:
        candidates.append((m.group(2), int(m.group(1))))
    for name, qty in candidates:
        if qty < 1:
            continue
        found = match_variants(name, variants)
        if len(found) == 1:
            return "ok", found[0], qty
        if len(found) > 1:
            return "ask", None, None
    return "ask", None, None


async def handle_claim(update: Update, ctx: ContextTypes.DEFAULT_TYPE, rest: str):
    msg = update.message

    conn = db()
    try:
        # which channel post is this comment under?
        root_id = msg.message_thread_id
        channel_msg_id = None
        if root_id:
            row = conn.execute(
                "SELECT channel_msg_id FROM threads WHERE group_chat_id=? AND group_msg_id=?",
                (msg.chat_id, root_id),
            ).fetchone()
            channel_msg_id = row[0] if row else None
        if channel_msg_id is None:
            channel_msg_id = channel_msg_from(msg.reply_to_message)
        if channel_msg_id is None:
            return

        post = product_for_channel_msg(conn, channel_msg_id)
        if not post:
            return
        product_id, title = post[0], post[1]
        variants = get_post_variants(conn, product_id)
        if not variants:
            return

        status, variant, qty = parse_claim(rest, variants)
        if status == "ignore":
            return
        if status == "ask":
            if len(rest.split()) <= 5:
                options = " / ".join(f"claim {v['name'].lower()}" for v in variants)
                await msg.reply_text(f"Which one would you like? 😊 Try: {options}")
            return

        variant_id = variant["variant_id"]
        price = float(variant["price"])
        claim_title = title if variant["name"] is None else f"{title} — {variant['name']}"

        user = msg.from_user
        first = user.first_name or "there"

        if get_strikes(user.id) >= STRIKE_LIMIT:
            await msg.reply_text(
                "Sorry, claims are paused for your account. Please message the shop. 🙏"
            )
            return

        async with stock_lock:
            # already handled (Telegram redelivery)?
            if conn.execute(
                "SELECT 1 FROM claims WHERE group_chat_id=? AND group_msg_id=?",
                (msg.chat_id, msg.message_id),
            ).fetchone():
                return

            try:
                stock, item_id, loc_id = await get_stock(variant_id)
            except Exception:
                log.exception("Stock lookup failed")
                await msg.reply_text("Something went wrong, please try again in a moment 🙏")
                return

            if stock <= 0:
                await msg.reply_text("😢 Sorry, this one is sold out!")
                return
            if qty > stock:
                await msg.reply_text(
                    f"❌ Sorry {first}, only {stock} left! Try “claim {stock}”."
                    if len(variants) == 1
                    else f"❌ Sorry {first}, only {stock} left of that one!"
                )
                return

            try:
                await adjust_stock(item_id, loc_id, -qty)
            except Exception:
                log.exception("Stock adjust failed")
                await msg.reply_text("Something went wrong, please try again in a moment 🙏")
                return

            conn.execute(
                "INSERT INTO claims (product_id, variant_id, title, price, qty, user_id, "
                "user_name, group_chat_id, group_msg_id, claimed_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    product_id, variant_id, claim_title, price, qty, user.id,
                    user.full_name, msg.chat_id, msg.message_id, now(),
                ),
            )
            conn.commit()
            remaining = stock - qty
    finally:
        conn.close()

    left_line = f"📦 {remaining} still available" if remaining > 0 else "🚫 Sold out!"
    button = InlineKeyboardMarkup(
        [[InlineKeyboardButton("📩 Get my invoice", url=f"https://t.me/{ctx.bot.username}?start=invoice")]]
    )
    await msg.reply_text(
        f"✅ Claim confirmed!\n\n"
        f"{first} claimed {qty} {plural(qty)} of:\n"
        f"{claim_title}\n"
        f"Price: ${price:.2f} each\n\n"
        f"{left_line}\n\n"
        f"💌 Tap the button below and press Start within {START_DEADLINE_MIN} minutes "
        f"so I can send your invoice.",
        reply_markup=button,
    )
    await refresh_product_post(ctx.bot, product_id)


# ---------------- stage 4: offers ----------------
OFFER_START = re.compile(r"^\s*offer\b(.*)$", re.I | re.S)
ACCEPT_START = re.compile(r"^\s*accept\b(.*)$", re.I | re.S)
PRICE_RE = re.compile(r"\$?\s*(\d{1,6}(?:\.\d{1,2})?)")


async def on_comment(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Routes comments in the discussion group: claim / offer / accept."""
    msg = update.message
    if not msg or not msg.text or not msg.from_user or msg.from_user.is_bot:
        return
    if len(msg.text) > 80:
        return
    m = CLAIM_START.match(msg.text)
    if m:
        await handle_claim(update, ctx, m.group(1))
        return
    m = OFFER_START.match(msg.text)
    if m:
        await handle_offer(update, ctx, m.group(1))
        return
    m = ACCEPT_START.match(msg.text)
    if m:
        await handle_accept(update, ctx, m.group(1))


def product_for_channel_msg(conn, channel_msg_id: int):
    """(product_id, title, offers_enabled) for a channel post, including older kept copies."""
    row = conn.execute(
        "SELECT product_id, title, offers_enabled FROM posts WHERE channel_msg_id=?",
        (channel_msg_id,),
    ).fetchone()
    if row:
        return row
    return conn.execute(
        "SELECT p.product_id, p.title, p.offers_enabled FROM post_copies c "
        "JOIN posts p ON p.product_id = c.product_id WHERE c.channel_msg_id=?",
        (channel_msg_id,),
    ).fetchone()


def locate_post(conn, msg):
    """Which channel post is this comment under? -> (product_id, title, offers_enabled) or None."""
    channel_msg_id = None
    if msg.message_thread_id:
        row = conn.execute(
            "SELECT channel_msg_id FROM threads WHERE group_chat_id=? AND group_msg_id=?",
            (msg.chat_id, msg.message_thread_id),
        ).fetchone()
        channel_msg_id = row[0] if row else None
    if channel_msg_id is None:
        channel_msg_id = channel_msg_from(msg.reply_to_message)
    if channel_msg_id is None:
        return None
    return product_for_channel_msg(conn, channel_msg_id)


def display_name(user) -> str:
    return f"@{user.username}" if user.username else user.full_name


def parse_offer(rest: str, variants):
    """Returns (status, variant, price). status: 'ok' | 'ask' | 'ignore'."""
    text = rest.lower().replace(",", ".")
    nums = list(PRICE_RE.finditer(text))
    if not nums:
        return "ignore", None, None
    m = nums[-1]
    price = float(m.group(1))
    if price <= 0:
        return "ignore", None, None
    remainder = norm(text[: m.start()] + " " + text[m.end():])
    if len(variants) == 1:
        return ("ok", variants[0], price) if remainder == "" else ("ignore", None, None)
    found = match_variants(remainder, variants)
    if len(found) == 1:
        return "ok", found[0], price
    return "ask", None, None


def offer_admin_message(o):
    """o = (id, title, listed_price, offer_price, user_name)"""
    pct = round(o[3] / o[2] * 100) if o[2] else 0
    text = (
        f"📝 New offer from {o[4]}\n\n"
        f"Card: {o[1]}\n"
        f"Listed: ${o[2]:.2f}\n"
        f"Offered: ${o[3]:.2f} ({pct}%)"
    )
    markup = InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("✅ Accept", callback_data=f"of:a:{o[0]}"),
            InlineKeyboardButton("💰 Counter", callback_data=f"of:c:{o[0]}"),
            InlineKeyboardButton("❌ Decline", callback_data=f"of:d:{o[0]}"),
        ]]
    )
    return text, markup


async def send_offer_to_admins(bot, offer_id: int):
    conn = db()
    o = conn.execute(
        "SELECT id, title, listed_price, offer_price, user_name FROM offers WHERE id=?",
        (offer_id,),
    ).fetchone()
    conn.close()
    if o:
        text, markup = offer_admin_message(o)
        await notify_admins(bot, text, markup)


async def post_in_thread(bot, chat_id: int, reply_to: int, text: str, markup=None):
    try:
        await bot.send_message(
            chat_id,
            text,
            reply_parameters=ReplyParameters(message_id=reply_to, allow_sending_without_reply=True),
            reply_markup=markup,
        )
    except Exception:
        log.exception("Could not post in comments")


async def handle_offer(update: Update, ctx: ContextTypes.DEFAULT_TYPE, rest: str):
    msg = update.message
    user = msg.from_user
    conn = db()
    try:
        post = locate_post(conn, msg)
        if not post:
            return
        product_id, title, offers_on = post
        variants = get_post_variants(conn, product_id)
        if not variants:
            return
        if not (OFFERS_ON_ALL or offers_on):
            if len(rest.split()) <= 3:
                await msg.reply_text(
                    "Offers aren't open on this one, but you can claim it at the listed price ☺️"
                )
            return

        status, variant, price = parse_offer(rest, variants)
        if status == "ignore":
            return
        if status == "ask":
            if len(rest.split()) <= 6:
                options = " / ".join(f"offer {v['name'].lower()} <price>" for v in variants)
                await msg.reply_text(f"Which one is your offer for? 😊 Try: {options}")
            return

        if get_strikes(user.id) >= STRIKE_LIMIT:
            await msg.reply_text(
                "Sorry, claims and offers are paused for your account. Please message the shop. 🙏"
            )
            return

        listed = float(variant["price"])
        card = title if variant["name"] is None else f"{title} — {variant['name']}"
        if price >= listed:
            await msg.reply_text(
                "That's at or above the listed price, so just comment “claim”"
                + ("" if variant["name"] is None else f" {variant['name'].lower()}")
                + " to grab it! 😊"
            )
            return
        try:
            stock, _, _ = await get_stock(variant["variant_id"])
        except Exception:
            log.exception("Stock lookup failed")
            await msg.reply_text("Something went wrong, please try again in a moment 🙏")
            return
        if stock <= 0:
            await msg.reply_text("😢 Sorry, this one is sold out!")
            return

        # a new offer replaces the buyer's earlier open offer on the same card
        conn.execute(
            "UPDATE offers SET status='withdrawn', updated_at=? "
            "WHERE user_id=? AND variant_id=? AND status IN ('pending','countered')",
            (now(), user.id, variant["variant_id"]),
        )
        cur = conn.execute(
            "INSERT INTO offers (product_id, variant_id, title, listed_price, offer_price, "
            "user_id, user_name, group_chat_id, group_msg_id, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                product_id, variant["variant_id"], card, listed, price, user.id,
                display_name(user), msg.chat_id, msg.message_id, now(), now(),
            ),
        )
        offer_id = cur.lastrowid
        conn.commit()
    finally:
        conn.close()

    pct = round(price / listed * 100)
    await msg.reply_text(
        f"📝 Offer received from {display_name(user)}\n\n"
        f"Card: {card}\n"
        f"Listed: ${listed:.2f}\n"
        f"Offered: ${price:.2f} ({pct}%)\n\n"
        f"The seller will review and respond."
    )
    await send_offer_to_admins(ctx.bot, offer_id)


async def accept_offer(bot, offer_id: int, price: float, reply_to: int | None = None):
    """Turn an open offer into a real claim at `price`.
    Returns 'ok' | 'sold_out' | 'error' | 'handled'."""
    conn = db()
    result, remaining, o = "error", 0, None
    try:
        o = conn.execute(
            "SELECT product_id, variant_id, title, listed_price, user_id, user_name, "
            "group_chat_id, group_msg_id, status FROM offers WHERE id=?",
            (offer_id,),
        ).fetchone()
        if not o or o[8] not in ("pending", "countered"):
            return "handled"
        product_id, variant_id, title, listed, uid, uname, gchat, gmsg, _ = o
        async with stock_lock:
            try:
                stock, item_id, loc_id = await get_stock(variant_id)
            except Exception:
                log.exception("Stock lookup failed")
                return "error"
            if stock <= 0:
                conn.execute(
                    "UPDATE offers SET status='expired', updated_at=? WHERE id=?", (now(), offer_id)
                )
                conn.commit()
                result = "sold_out"
            else:
                try:
                    await adjust_stock(item_id, loc_id, -1)
                except Exception:
                    log.exception("Stock adjust failed")
                    return "error"
                conn.execute(
                    "INSERT INTO claims (product_id, variant_id, title, price, qty, user_id, "
                    "user_name, group_chat_id, group_msg_id, claimed_at, list_price, offer_id) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (product_id, variant_id, title, price, 1, uid, uname, gchat, gmsg, now(), listed, offer_id),
                )
                conn.execute(
                    "UPDATE offers SET status='accepted', final_price=?, updated_at=? WHERE id=?",
                    (price, now(), offer_id),
                )
                conn.commit()
                remaining = stock - 1
                result = "ok"
    finally:
        conn.close()

    target = reply_to or gmsg
    if result == "sold_out":
        await post_in_thread(
            bot, gchat, target, f"😢 Sorry {uname}, this card sold before the offer could be accepted."
        )
    elif result == "ok":
        left_line = f"📦 {remaining} still available" if remaining > 0 else "🚫 Sold out!"
        button = InlineKeyboardMarkup(
            [[InlineKeyboardButton("📩 Get my invoice", url=f"https://t.me/{bot.username}?start=invoice")]]
        )
        await post_in_thread(
            bot, gchat, target,
            f"✅ Offer accepted for {uname}!\n\n"
            f"Card: {title}\n"
            f"Price: ${price:.2f} (listed ${listed:.2f})\n\n"
            f"{left_line}\n\n"
            f"💌 Tap the button below and press Start within {START_DEADLINE_MIN} minutes "
            f"so I can send your invoice.",
            button,
        )
        await refresh_product_post(bot, product_id)
    return result


async def handle_accept(update: Update, ctx: ContextTypes.DEFAULT_TYPE, rest: str):
    """Buyer comments 'accept' on a counter offer."""
    msg = update.message
    user = msg.from_user
    conn = db()
    try:
        post = locate_post(conn, msg)
        if not post:
            return
        product_id = post[0]
        rows = conn.execute(
            "SELECT id, variant_id, counter_price, updated_at FROM offers "
            "WHERE user_id=? AND product_id=? AND status='countered' ORDER BY updated_at DESC",
            (user.id, product_id),
        ).fetchall()
        if not rows:
            return  # nothing to accept here: stay silent
        live = [r for r in rows if now() - r[3] <= OFFER_TTL]
        if not live:
            await msg.reply_text("⌛ That counter offer has expired. You're welcome to make a new offer or claim at the listed price ☺️")
            return
        chosen = None
        if len(live) == 1:
            chosen = live[0]
        else:
            names = {v["variant_id"]: v["name"] for v in get_post_variants(conn, product_id)}
            want = norm(rest)
            cand = [
                r for r in live
                if want and names.get(r[1])
                and (norm(names[r[1]]).startswith(want) or want in norm(names[r[1]]).split())
            ]
            if len(cand) == 1:
                chosen = cand[0]
        if not chosen:
            await msg.reply_text("You have more than one counter offer here. Try “accept <name>” 😊")
            return
        offer_id, counter_price = chosen[0], chosen[2]
    finally:
        conn.close()

    result = await accept_offer(ctx.bot, offer_id, counter_price, reply_to=msg.message_id)
    if result == "error":
        await msg.reply_text("Something went wrong, please try again in a moment 🙏")


async def on_offer_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id not in ADMIN_IDS:
        await q.answer("Admins only", show_alert=True)
        return
    _, action, oid = q.data.split(":")
    oid = int(oid)
    conn = db()
    o = conn.execute(
        "SELECT user_name, title, listed_price, offer_price, group_chat_id, group_msg_id, "
        "status, updated_at FROM offers WHERE id=?",
        (oid,),
    ).fetchone()
    conn.close()
    if not o or o[6] != "pending":
        await q.answer("Already handled.")
        return
    name, title, listed, offered, gchat, gmsg, _, updated = o
    if now() - updated > OFFER_TTL:
        await q.answer("This offer has expired.", show_alert=True)
        return

    if action == "a":
        result = await accept_offer(ctx.bot, oid, offered)
        if result == "ok":
            await q.edit_message_text(f"✅ Accepted {name}'s offer of ${offered:.2f} on {title}.")
        elif result == "sold_out":
            await q.edit_message_text(f"😢 {title} sold out before you could accept {name}'s offer.")
        else:
            await q.answer("Something went wrong. Check the logs / try again.", show_alert=True)
            return
    elif action == "d":
        conn = db()
        conn.execute("UPDATE offers SET status='declined', updated_at=? WHERE id=?", (now(), oid))
        conn.commit()
        conn.close()
        await post_in_thread(
            ctx.bot, gchat, gmsg,
            f"🙏 Offer declined for {name}\n\n"
            f"Card: {title}\n"
            f"Offered: ${offered:.2f}\n\n"
            f"You're welcome to make another offer or claim at the listed price ☺️",
        )
        await q.edit_message_text(f"❌ Declined {name}'s offer of ${offered:.2f} on {title}.")
    elif action == "c":
        ctx.application.bot_data.setdefault("awaiting_counter", {})[q.from_user.id] = oid
        await ctx.bot.send_message(
            q.from_user.id,
            f"💰 Countering {name}'s offer of ${offered:.2f} on {title} (listed ${listed:.2f}).\n\n"
            f"Reply with your counter price, e.g. 12 or 12.50 (or “cancel”).",
        )
    await q.answer()


async def on_private_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin types a counter price after tapping Counter."""
    user = update.effective_user
    if not user or user.id not in ADMIN_IDS or not update.message or not update.message.text:
        return
    waiting = ctx.application.bot_data.get("awaiting_counter", {})
    oid = waiting.get(user.id)
    if not oid:
        return
    text = update.message.text.strip().lower()
    if text == "cancel":
        waiting.pop(user.id, None)
        await update.message.reply_text("Cancelled.")
        return
    m = re.match(r"^\$?\s*(\d{1,6}(?:[.,]\d{1,2})?)$", text)
    if not m:
        await update.message.reply_text("Please send just a price, like 12 or 12.50 (or “cancel”).")
        return
    price = float(m.group(1).replace(",", "."))
    conn = db()
    o = conn.execute(
        "SELECT user_name, title, listed_price, offer_price, group_chat_id, group_msg_id, status "
        "FROM offers WHERE id=?",
        (oid,),
    ).fetchone()
    if not o or o[6] != "pending":
        conn.close()
        waiting.pop(user.id, None)
        await update.message.reply_text("That offer was already handled.")
        return
    name, title, listed, offered, gchat, gmsg, _ = o
    if not (offered < price < listed):
        conn.close()
        await update.message.reply_text(
            f"The counter must be between ${offered:.2f} and ${listed:.2f}. Try again (or “cancel”)."
        )
        return
    conn.execute(
        "UPDATE offers SET status='countered', counter_price=?, updated_at=? WHERE id=?",
        (price, now(), oid),
    )
    conn.commit()
    conn.close()
    waiting.pop(user.id, None)
    await post_in_thread(
        ctx.bot, gchat, gmsg,
        f"💰 Counter offer for {name}\n\n"
        f"Card: {title}\n"
        f"Your offer: ${offered:.2f}\n"
        f"Counter: ${price:.2f}\n\n"
        f"Comment “accept” to accept or “offer <price>” to counter back!",
    )
    await update.message.reply_text(f"Counter of ${price:.2f} sent ✅")


async def cmd_offers(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/offers -> re-send every offer waiting for your decision, and list open counters."""
    if not is_admin(update):
        return
    conn = db()
    pending = conn.execute(
        "SELECT id, title, listed_price, offer_price, user_name FROM offers "
        "WHERE status='pending' AND updated_at > ? ORDER BY id",
        (now() - OFFER_TTL,),
    ).fetchall()
    countered = conn.execute(
        "SELECT user_name, title, counter_price FROM offers "
        "WHERE status='countered' AND updated_at > ? ORDER BY id",
        (now() - OFFER_TTL,),
    ).fetchall()
    conn.close()
    if not pending and not countered:
        await update.message.reply_text("No open offers.")
        return
    for o in pending:
        text, markup = offer_admin_message(o)
        await update.message.reply_text(text, reply_markup=markup)
    if countered:
        lines = [f"• {n}: counter ${c:.2f} on {t}" for n, t, c in countered]
        await update.message.reply_text("⏳ Waiting for buyers to reply:\n" + "\n".join(lines))


async def expire_offers(bot):
    conn = db()
    rows = conn.execute(
        "SELECT id, status, user_name, title, group_chat_id, group_msg_id FROM offers "
        "WHERE status IN ('pending','countered') AND updated_at <= ?",
        (now() - OFFER_TTL,),
    ).fetchall()
    for r in rows:
        conn.execute("UPDATE offers SET status='expired', updated_at=? WHERE id=?", (now(), r[0]))
    conn.commit()
    conn.close()
    for _, status, name, title, gchat, gmsg in rows:
        what = "counter offer" if status == "countered" else "offer"
        await post_in_thread(
            bot, gchat, gmsg,
            f"⌛ The {what} for {name} on {title} has expired. "
            f"You're welcome to make a new offer or claim at the listed price ☺️",
        )


# ---------------- stage 3: invoices, payments, no-shows ----------------
async def release_claims(bot, claim_rows, restore_stock: bool):
    """claim_rows: list of (claim_id, variant_id, qty).
    restore_stock=True  -> claim stock is still held by us, so add it back to Shopify.
    restore_stock=False -> Shopify already restocked it (draft deleted / order cancelled)."""
    if not claim_rows:
        return
    variants = {}
    for _, vid, q in claim_rows:
        variants[vid] = variants.get(vid, 0) + q
    async with stock_lock:
        if restore_stock:
            for vid, q in variants.items():
                try:
                    _, item_id, loc_id = await get_stock(vid)
                    await adjust_stock(item_id, loc_id, q)
                except Exception:
                    log.exception("Could not restore stock for %s", vid)
                    await notify_admins(bot, f"⚠️ Couldn't restore {q} stock for a released claim. Check Shopify.")
        conn = db()
        ids = [c[0] for c in claim_rows]
        conn.execute(
            f"UPDATE claims SET status='released' WHERE id IN ({','.join('?' * len(ids))})", ids
        )
        conn.commit()
        conn.close()
    if not restore_stock:
        await asyncio.sleep(3)  # give Shopify a moment to restock
    await refresh_products_for_variants(bot, list(variants))


async def strike_and_notify(bot, user_id: int, name: str, reason: str):
    count = add_strike(user_id, name)
    msg = f"⚠️ {name}: {reason}\nStrike {count}/{STRIKE_LIMIT}."
    if count >= STRIKE_LIMIT:
        msg += "\n🚫 They're now blocked from claiming. /clearstrikes to undo."
    await notify_admins(bot, msg)
    return count


async def send_due_invoices(bot, force: bool = False):
    conn = db()
    rows = conn.execute(
        "SELECT c.user_id, MAX(c.claimed_at) FROM claims c "
        "JOIN users u ON u.user_id = c.user_id "
        "WHERE c.status = 'unpaid' GROUP BY c.user_id"
    ).fetchall()
    conn.close()
    for uid, last_claim in rows:
        if force or now() - last_claim >= INVOICE_DELAY_MIN * 60:
            try:
                await create_invoice(bot, uid)
            except Exception:
                log.exception("create_invoice crashed for %s", uid)


async def create_invoice(bot, uid: int):
    conn = db()
    try:
        async with stock_lock:
            claims = conn.execute(
                "SELECT id, variant_id, title, price, qty, user_name, list_price, offer_id FROM claims "
                "WHERE user_id=? AND status='unpaid'",
                (uid,),
            ).fetchall()
            urow = conn.execute("SELECT chat_id FROM users WHERE user_id=?", (uid,)).fetchone()
            if not claims or not urow:
                return
            chat_id = urow[0]
            name = claims[0][5]

            by_variant = {}
            for c in claims:
                by_variant[c[1]] = by_variant.get(c[1], 0) + c[4]

            # accepted offers get their own line with a discount down to the agreed price
            line_items, plain = [], {}
            for c in claims:
                vid, price, q, list_price, offer_id = c[1], c[3], c[4], c[6], c[7]
                if offer_id and list_price and price < list_price - 0.004:
                    line_items.append(
                        {
                            "variantId": vid,
                            "quantity": q,
                            "appliedDiscount": {
                                "title": "Accepted offer",
                                "description": "Accepted offer via Telegram",
                                "value": round(list_price - price, 2),
                                "valueType": "FIXED_AMOUNT",
                            },
                        }
                    )
                else:
                    plain[vid] = plain.get(vid, 0) + q
            line_items += [{"variantId": vid, "quantity": q} for vid, q in plain.items()]

            # The draft order reserves the stock itself, so hand our hold back first.
            restored = []
            try:
                for vid, q in by_variant.items():
                    _, item_id, loc_id = await get_stock(vid)
                    await adjust_stock(item_id, loc_id, q)
                    restored.append((item_id, loc_id, q))
                until = (
                    datetime.now(timezone.utc)
                    + timedelta(minutes=PAY_DEADLINE_MIN + 30)
                ).strftime("%Y-%m-%dT%H:%M:%SZ")
                d = await shopify.gql(
                    DRAFT_CREATE,
                    {
                        "input": {
                            "lineItems": line_items,
                            "note": f"Telegram claims - {name} (ID {uid})",
                            "tags": ["telegram-claim"],
                            "reserveInventoryUntil": until,
                        }
                    },
                )
                res = d["draftOrderCreate"]
                if res["userErrors"]:
                    raise RuntimeError(res["userErrors"])
                draft = res["draftOrder"]
            except Exception as e:
                for item_id, loc_id, q in restored:  # put our hold back
                    try:
                        await adjust_stock(item_id, loc_id, -q)
                    except Exception:
                        log.exception("Could not re-hold stock")
                log.exception("Invoice creation failed")
                if now() - _invoice_alerted.get(uid, 0) > 1800:
                    _invoice_alerted[uid] = now()
                    await notify_admins(bot, f"⚠️ Couldn't create invoice for {name}: {e}")
                return

            total = sum(c[3] * c[4] for c in claims)
            cur = conn.execute(
                "INSERT INTO invoices (user_id, user_name, chat_id, draft_id, invoice_url, "
                "total, created_at) VALUES (?,?,?,?,?,?,?)",
                (uid, name, chat_id, draft["id"], draft["invoiceUrl"], total, now()),
            )
            inv_id = cur.lastrowid
            ids = [c[0] for c in claims]
            conn.execute(
                f"UPDATE claims SET status='invoiced', invoice_id=? "
                f"WHERE id IN ({','.join('?' * len(ids))})",
                [inv_id, *ids],
            )
            conn.commit()

        lines = "\n".join(f"{c[4]}× {c[2]} — ${c[3] * c[4]:.2f}" for c in claims)
        text = (
            f"🧾 Your Ditto Deals invoice\n\n{lines}\n\n"
            f"Items total: ${total:.2f}\n"
            f"(delivery is added at checkout)\n\n"
            f"⏰ Please check out and pay within {PAY_DEADLINE_MIN} minutes, "
            f"or your claims will be released."
        )
        button = InlineKeyboardMarkup(
            [[InlineKeyboardButton("💳 Check out & pay", url=draft["invoiceUrl"])]]
        )
        if not await safe_dm(bot, chat_id, text, button):
            await notify_admins(
                bot,
                f"⚠️ Couldn't DM the invoice to {name}. Send them this link manually:\n{draft['invoiceUrl']}",
            )
    finally:
        conn.close()


async def finish_paid(bot, inv_id: int):
    conn = db()
    inv = conn.execute(
        "SELECT user_name, chat_id, status FROM invoices WHERE id=?", (inv_id,)
    ).fetchone()
    if not inv or inv[2] == "paid":
        conn.close()
        return
    conn.execute("UPDATE invoices SET status='paid' WHERE id=?", (inv_id,))
    conn.execute("UPDATE claims SET status='paid' WHERE invoice_id=?", (inv_id,))
    conn.commit()
    conn.close()
    await safe_dm(bot, inv[1], "✅ Payment received! Thank you 💜 We'll pack your order soon.")
    await notify_admins(bot, f"✅ {inv[0]} paid.")


async def finish_expired(bot, inv_id: int, delete_draft: bool, reason: str):
    conn = db()
    inv = conn.execute(
        "SELECT user_id, user_name, chat_id, draft_id, status FROM invoices WHERE id=?", (inv_id,)
    ).fetchone()
    if not inv or inv[4] != "sent":
        conn.close()
        return
    claim_rows = conn.execute(
        "SELECT id, variant_id, qty FROM claims WHERE invoice_id=? AND status='invoiced'",
        (inv_id,),
    ).fetchall()
    conn.execute("UPDATE invoices SET status='expired' WHERE id=?", (inv_id,))
    conn.commit()
    conn.close()
    uid, name, chat_id, draft_id, _ = inv
    if delete_draft:
        try:
            await shopify.gql(DRAFT_DELETE, {"input": {"id": draft_id}})
        except Exception:
            log.exception("Draft delete failed")
    await release_claims(bot, claim_rows, restore_stock=False)
    await safe_dm(
        bot,
        chat_id,
        "⌛ Your claims were released because payment wasn't received in time. "
        "Message the shop if this is a mistake.",
    )
    await strike_and_notify(bot, uid, name, reason)


async def expire_unstarted(bot):
    """Claims from people who never pressed Start on the bot within the deadline."""
    conn = db()
    rows = conn.execute(
        "SELECT c.id, c.user_id, c.user_name, c.variant_id, c.qty FROM claims c "
        "LEFT JOIN users u ON u.user_id = c.user_id "
        "WHERE c.status='unpaid' AND u.user_id IS NULL AND c.claimed_at <= ?",
        (now() - START_DEADLINE_MIN * 60,),
    ).fetchall()
    conn.close()
    buyers = {}
    for cid, uid, name, vid, q in rows:
        buyers.setdefault((uid, name), []).append((cid, vid, q))
    for (uid, name), claim_rows in buyers.items():
        await release_claims(bot, claim_rows, restore_stock=True)
        await strike_and_notify(
            bot, uid, name, f"didn't start the bot in time; {len(claim_rows)} claim(s) released."
        )


async def check_invoices(bot):
    conn = db()
    rows = conn.execute(
        "SELECT id, user_name, chat_id, draft_id, created_at, reminded, alerted "
        "FROM invoices WHERE status='sent'"
    ).fetchall()
    conn.close()
    for inv_id, name, chat_id, draft_id, created, reminded, alerted in rows:
        try:
            draft = await get_draft(draft_id)
            order = draft["order"] if draft else None
            if order and order["displayFinancialStatus"] == "PAID":
                await finish_paid(bot, inv_id)
                continue
            age = now() - created
            if order is None:
                # buyer hasn't checked out yet
                if age >= PAY_DEADLINE_MIN * 60:
                    await finish_expired(
                        bot, inv_id, delete_draft=True,
                        reason="didn't check out/pay in time; claims released.",
                    )
                elif age >= (PAY_DEADLINE_MIN - 15) * 60 and not reminded:
                    conn = db()
                    conn.execute("UPDATE invoices SET reminded=1 WHERE id=?", (inv_id,))
                    conn.commit()
                    conn.close()
                    await safe_dm(bot, chat_id, "⏰ Reminder: about 15 minutes left to check out before your claims are released.")
            elif age >= CONFIRM_WINDOW_HOURS * 3600 and not alerted:
                # checked out, but you haven't confirmed the money yet -> ask the shop owner
                conn = db()
                conn.execute("UPDATE invoices SET alerted=1 WHERE id=?", (inv_id,))
                conn.commit()
                conn.close()
                markup = InlineKeyboardMarkup(
                    [[
                        InlineKeyboardButton("✅ Mark paid", callback_data=f"paid:{inv_id}"),
                        InlineKeyboardButton("❌ Release + strike", callback_data=f"release:{inv_id}"),
                    ]]
                )
                await notify_admins(
                    bot,
                    f"⏰ {name}'s order {order['name']} still isn't marked paid after "
                    f"{CONFIRM_WINDOW_HOURS} hours. Check your bank, then choose:",
                    markup,
                )
        except Exception:
            log.exception("check_invoices failed for invoice %s", inv_id)


async def auto_sync(bot):
    """Every few minutes: if a post's price/stock differs from Shopify, edit the post."""
    conn = db()
    rows = conn.execute(
        "SELECT product_id, title, offers_enabled, last_caption FROM posts WHERE posted_at > ?",
        (now() - SYNC_WINDOW_DAYS * 86400,),
    ).fetchall()
    conn.close()
    if not rows:
        return
    by_product = await fetch_variants_bulk([r[0] for r in rows])
    for product_id, title, offers, last_caption in rows:
        found = by_product.get(product_id)
        if not found:
            continue
        variants, offers_now = found
        if offers_now != bool(offers):  # you added/removed the "offers" tag in Shopify
            c5 = db()
            c5.execute(
                "UPDATE posts SET offers_enabled=? WHERE product_id=?",
                (1 if offers_now else 0, product_id),
            )
            c5.commit()
            c5.close()
        caption = make_caption({"title": title, "variants": variants, "offers": offers_now})
        if caption == last_caption:
            continue
        await refresh_product_post(bot, product_id)
        await asyncio.sleep(1)


async def auto_sync_job(ctx: ContextTypes.DEFAULT_TYPE):
    try:
        await auto_sync(ctx.bot)
    except Exception:
        log.exception("auto_sync failed")


async def tick(ctx: ContextTypes.DEFAULT_TYPE):
    """Runs every minute."""
    for step in (send_due_invoices, expire_unstarted, check_invoices, expire_offers):
        try:
            await step(ctx.bot)
        except Exception:
            log.exception("tick step %s failed", step.__name__)


async def on_admin_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id not in ADMIN_IDS:
        await q.answer("Admins only", show_alert=True)
        return
    action, inv_id = q.data.split(":")
    inv_id = int(inv_id)
    conn = db()
    inv = conn.execute(
        "SELECT user_name, draft_id, status FROM invoices WHERE id=?", (inv_id,)
    ).fetchone()
    conn.close()
    if not inv or inv[2] != "sent":
        await q.answer("Already handled.")
        return
    name, draft_id, _ = inv
    try:
        draft = await get_draft(draft_id)
        order = draft["order"] if draft else None
        if not order:
            await q.answer("Order not found in Shopify.", show_alert=True)
            return
        if action == "paid":
            d = await shopify.gql(ORDER_PAID, {"input": {"id": order["id"]}})
            errs = d["orderMarkAsPaid"]["userErrors"]
            if errs:
                raise RuntimeError(errs)
            await finish_paid(ctx.bot, inv_id)
            await q.edit_message_text(f"✅ Marked {name}'s order as paid.")
        else:
            d = await shopify.gql(ORDER_CANCEL, {"orderId": order["id"]})
            errs = d["orderCancel"]["orderCancelUserErrors"]
            if errs:
                raise RuntimeError(errs)
            await finish_expired(
                ctx.bot, inv_id, delete_draft=False, reason="order cancelled after unpaid deadline."
            )
            await q.edit_message_text(f"❌ Released {name}'s order and added a strike.")
        await q.answer()
    except Exception as e:
        log.exception("Admin button failed")
        await q.answer(f"Error: {e}"[:180], show_alert=True)


def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("myid", cmd_myid))
    app.add_handler(CommandHandler("post", cmd_post, block=False))
    app.add_handler(CommandHandler("repost", cmd_post, block=False))
    app.add_handler(CommandHandler("claims", cmd_claims))
    app.add_handler(CommandHandler("sync", cmd_sync))
    app.add_handler(CommandHandler("invoice", cmd_invoice))
    app.add_handler(CommandHandler("strikes", cmd_strikes))
    app.add_handler(CommandHandler("clearstrikes", cmd_clearstrikes))
    app.add_handler(CommandHandler("offers", cmd_offers))
    app.add_handler(CallbackQueryHandler(on_offer_button, pattern=r"^of:(a|d|c):\d+$"))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, on_private_text))
    app.add_handler(CallbackQueryHandler(on_admin_button, pattern=r"^(paid|release):\d+$"))
    app.add_handler(MessageHandler(filters.IS_AUTOMATIC_FORWARD, on_auto_forward), group=-1)
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, on_comment))
    app.job_queue.run_repeating(tick, interval=60, first=20)
    app.job_queue.run_repeating(auto_sync_job, interval=SYNC_INTERVAL_MIN * 60, first=90)
    log.info("Bot running")
    app.run_polling()


if __name__ == "__main__":
    main()
