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
STRIKE_LIMIT = int(os.environ.get("STRIKE_LIMIT", "3"))

# "claim", "claim 2", "claim x2", "claim 2x", "claim2"
CLAIM_RE = re.compile(r"^\s*claim(?:\s*[x×]?\s*(\d{1,3})\s*[x×]?)?\s*[!.]*\s*$", re.I)

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
        CREATE TABLE IF NOT EXISTS strikes (
            user_id INTEGER PRIMARY KEY,
            name TEXT,
            count INTEGER DEFAULT 0
        );
        """
    )
    ensure_column(conn, "claims", "invoice_id", "INTEGER")
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
  products(first: 50, query: $q) {
    nodes {
      id
      title
      featuredImage { url }
      variants(first: 1) { nodes { id price inventoryQuantity } }
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


async def fetch_sale_products():
    data = await shopify.gql(PRODUCTS_QUERY, {"q": f"tag:{SALE_TAG} AND status:active"})
    items = []
    for p in data["products"]["nodes"]:
        v = p["variants"]["nodes"][0] if p["variants"]["nodes"] else None
        if not v:
            continue
        items.append(
            {
                "product_id": p["id"],
                "variant_id": v["id"],
                "title": p["title"],
                "price": float(v["price"]),
                "qty": int(v["inventoryQuantity"] or 0),
                "image": (p["featuredImage"] or {}).get("url"),
            }
        )
    return items


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
    title = f"✨<b>{html.escape(item['title'])}</b>✨"
    if item["qty"] <= 0:
        return (
            f"{title}\n"
            f"• Price: ${item['price']:.2f}\n"
            f"• SOLD OUT 🚫"
        )
    return (
        f"{title}\n"
        f"• Price: ${item['price']:.2f}\n"
        f"• Quantity: {item['qty']}\n\n"
        f"<b>💭How to claim:</b>\n"
        f'• Comment "claim + qty" (e.g. claim 2) to claim! 😊'
    )


async def refresh_post(bot, channel_msg_id: int, title: str, price: float, qty: int):
    """Edit the channel post so the quantity / SOLD OUT status is current."""
    caption = make_caption({"title": title, "price": price, "qty": qty})
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


async def refresh_variant_post(bot, variant_id: str):
    conn = db()
    row = conn.execute(
        "SELECT channel_msg_id, title, price FROM posts WHERE variant_id=?", (variant_id,)
    ).fetchone()
    conn.close()
    if not row:
        return
    try:
        stock, _, _ = await get_stock(variant_id)
        await refresh_post(bot, row[0], row[1], float(row[2]), stock)
    except Exception:
        log.exception("Could not refresh post for %s", variant_id)


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


async def cmd_post(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/post       -> post all new Shopify items tagged 'telegram' to the channel
    /post test  -> send them to this chat only, nothing is saved"""
    if not is_admin(update):
        return
    test = bool(ctx.args) and ctx.args[0].lower() == "test"
    target = update.effective_chat.id if test else CHANNEL

    await update.message.reply_text("Checking Shopify…")
    try:
        items = await fetch_sale_products()
    except Exception as e:
        log.exception("Shopify fetch failed")
        await update.message.reply_text(f"Couldn't reach Shopify: {e}")
        return

    conn = db()
    posted, skipped = 0, []
    for item in items:
        if not test and conn.execute(
            "SELECT 1 FROM posts WHERE product_id = ?", (item["product_id"],)
        ).fetchone():
            continue
        if item["qty"] <= 0:
            skipped.append(f"{item['title']} (no stock)")
            continue
        try:
            caption = make_caption(item)
            if item["image"]:
                msg = await ctx.bot.send_photo(
                    target, item["image"], caption=caption, parse_mode="HTML"
                )
            else:
                msg = await ctx.bot.send_message(target, caption, parse_mode="HTML")
            if not test:
                conn.execute(
                    "INSERT INTO posts VALUES (?,?,?,?,?,?)",
                    (
                        item["product_id"],
                        item["variant_id"],
                        msg.message_id,
                        item["title"],
                        str(item["price"]),
                        now(),
                    ),
                )
                conn.commit()
            posted += 1
            if not test:
                await asyncio.sleep(POST_DELAY)
        except Exception as e:
            log.exception("Post failed")
            skipped.append(f"{item['title']} ({e})")
    conn.close()

    where = "this chat (test)" if test else CHANNEL
    text = f"Posted {posted} item(s) to {where}."
    if skipped:
        text += "\nSkipped:\n" + "\n".join(f"• {s}" for s in skipped)
    await update.message.reply_text(text)


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
        "SELECT variant_id FROM posts WHERE posted_at > ?", (cutoff,)
    ).fetchall()
    conn.close()
    for (variant_id,) in rows:
        await refresh_variant_post(ctx.bot, variant_id)
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


async def on_comment(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.text or not msg.from_user or msg.from_user.is_bot:
        return
    m = CLAIM_RE.match(msg.text)
    if not m:
        return
    qty = int(m.group(1) or 1)
    if qty < 1:
        return

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

        post = conn.execute(
            "SELECT product_id, variant_id, title, price FROM posts WHERE channel_msg_id=?",
            (channel_msg_id,),
        ).fetchone()
        if not post:
            return
        product_id, variant_id, title, price = post
        price = float(price)

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
                    product_id, variant_id, title, price, qty, user.id,
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
        f"{title}\n"
        f"Price: ${price:.2f} each\n\n"
        f"{left_line}\n\n"
        f"💌 Tap the button below and press Start within {START_DEADLINE_MIN} minutes "
        f"so I can send your invoice.",
        reply_markup=button,
    )
    await refresh_post(ctx.bot, channel_msg_id, title, price, remaining)


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
    for vid in variants:
        await refresh_variant_post(bot, vid)


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
                "SELECT id, variant_id, title, price, qty, user_name FROM claims "
                "WHERE user_id=? AND status='unpaid'",
                (uid,),
            ).fetchall()
            urow = conn.execute("SELECT chat_id FROM users WHERE user_id=?", (uid,)).fetchone()
            if not claims or not urow:
                return
            chat_id = urow[0]
            name = claims[0][5]

            by_variant = {}
            for _, vid, _, _, q, _ in claims:
                by_variant[vid] = by_variant.get(vid, 0) + q

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
                            "lineItems": [
                                {"variantId": vid, "quantity": q} for vid, q in by_variant.items()
                            ],
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
            if age >= PAY_DEADLINE_MIN * 60:
                if order is None:
                    # they never even checked out -> release automatically
                    await finish_expired(
                        bot, inv_id, delete_draft=True,
                        reason="didn't check out/pay in time; claims released.",
                    )
                elif not alerted:
                    # checked out but money not confirmed yet -> ask the shop owner
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
                        f"⏰ {name}'s payment window is over and order {order['name']} isn't marked "
                        f"paid. Check your bank, then choose:",
                        markup,
                    )
            elif age >= (PAY_DEADLINE_MIN - 15) * 60 and not reminded:
                conn = db()
                conn.execute("UPDATE invoices SET reminded=1 WHERE id=?", (inv_id,))
                conn.commit()
                conn.close()
                await safe_dm(bot, chat_id, "⏰ Reminder: about 15 minutes left to pay before your claims are released.")
        except Exception:
            log.exception("check_invoices failed for invoice %s", inv_id)


async def tick(ctx: ContextTypes.DEFAULT_TYPE):
    """Runs every minute."""
    for step in (send_due_invoices, expire_unstarted, check_invoices):
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
    app.add_handler(CommandHandler("claims", cmd_claims))
    app.add_handler(CommandHandler("sync", cmd_sync))
    app.add_handler(CommandHandler("invoice", cmd_invoice))
    app.add_handler(CommandHandler("strikes", cmd_strikes))
    app.add_handler(CommandHandler("clearstrikes", cmd_clearstrikes))
    app.add_handler(CallbackQueryHandler(on_admin_button, pattern=r"^(paid|release):\d+$"))
    app.add_handler(MessageHandler(filters.IS_AUTOMATIC_FORWARD, on_auto_forward), group=-1)
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, on_comment))
    app.job_queue.run_repeating(tick, interval=60, first=20)
    log.info("Bot running")
    app.run_polling()


if __name__ == "__main__":
    main()
