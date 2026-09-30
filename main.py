"""Ditto Deals claims bot - Stages 1 + 2
Stage 1: post Shopify items to the Telegram channel.
Stage 2: detect "claim" comments, sync stock with Shopify, reply to buyers.
"""
import asyncio
import logging
import os
import re
import sqlite3
import time

import httpx
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    MessageOriginChannel,
    Update,
)
from telegram.error import BadRequest
from telegram.ext import (
    Application,
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

# "claim", "claim 2", "claim x2", "claim 2x", "claim2"
CLAIM_RE = re.compile(r"^\s*claim(?:\s*[x×]?\s*(\d{1,3})\s*[x×]?)?\s*[!.]*\s*$", re.I)


# ---------------- database ----------------
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
        """
    )
    return conn


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


# ---------------- post text ----------------
def make_caption(item) -> str:
    if item["qty"] <= 0:
        return (
            f"✨{item['title']}✨\n"
            f"• Price: ${item['price']:.2f}\n"
            f"• SOLD OUT 🚫"
        )
    return (
        f"✨{item['title']}✨\n"
        f"• Price: ${item['price']:.2f}\n"
        f"• Quantity: {item['qty']}\n\n"
        f"💭How to claim:\n"
        f'• Comment "claim + qty" (e.g. claim 2) to claim! 😊'
    )


async def refresh_post(bot, channel_msg_id: int, title: str, price: float, qty: int):
    """Edit the channel post so the quantity / SOLD OUT status is current."""
    caption = make_caption({"title": title, "price": price, "qty": qty})
    try:
        await bot.edit_message_caption(CHANNEL, channel_msg_id, caption=caption)
    except BadRequest as e:
        if "not modified" in str(e).lower():
            return
        try:  # post might be text-only
            await bot.edit_message_text(caption, CHANNEL, channel_msg_id)
        except BadRequest as e2:
            if "not modified" not in str(e2).lower():
                log.warning("Could not edit post %s: %s", channel_msg_id, e2)


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


# ---------------- commands ----------------
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if update.effective_chat.type == "private" and user:
        conn = db()
        conn.execute(
            "INSERT OR REPLACE INTO users VALUES (?,?,?)",
            (user.id, update.effective_chat.id, int(time.time())),
        )
        conn.commit()
        conn.close()
    await update.message.reply_text(
        "Hi! 💜 You're all set. Your invoices will be sent here after each sale."
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
                msg = await ctx.bot.send_photo(target, item["image"], caption=caption)
            else:
                msg = await ctx.bot.send_message(target, caption)
            if not test:
                conn.execute(
                    "INSERT INTO posts VALUES (?,?,?,?,?,?)",
                    (
                        item["product_id"],
                        item["variant_id"],
                        msg.message_id,
                        item["title"],
                        str(item["price"]),
                        int(time.time()),
                    ),
                )
                conn.commit()
            posted += 1
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
    """/claims -> everything claimed so far and not yet invoiced, grouped by buyer."""
    if not is_admin(update):
        return
    conn = db()
    rows = conn.execute(
        "SELECT user_id, user_name, title, price, qty FROM claims "
        "WHERE status = 'unpaid' ORDER BY user_id, id"
    ).fetchall()
    conn.close()
    if not rows:
        await update.message.reply_text("No open claims.")
        return
    buyers = {}
    for uid, name, title, price, qty in rows:
        b = buyers.setdefault(uid, {"name": name, "lines": [], "total": 0.0})
        b["lines"].append(f"  {qty}× {title} @ ${price:.2f}")
        b["total"] += price * qty
    parts = []
    for b in buyers.values():
        parts.append(f"👤 {b['name']} — ${b['total']:.2f}\n" + "\n".join(b["lines"]))
    text = "\n\n".join(parts)
    await update.message.reply_text(text[:4000])


async def cmd_sync(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/sync -> refresh quantities on recent channel posts from Shopify
    (use this if something sold on the website)"""
    if not is_admin(update):
        return
    cutoff = int(time.time()) - 14 * 86400
    conn = db()
    rows = conn.execute(
        "SELECT variant_id, channel_msg_id, title, price FROM posts WHERE posted_at > ?",
        (cutoff,),
    ).fetchall()
    conn.close()
    n = 0
    for variant_id, msg_id, title, price in rows:
        try:
            stock, _, _ = await get_stock(variant_id)
            await refresh_post(ctx.bot, msg_id, title, float(price), stock)
            n += 1
            await asyncio.sleep(0.5)
        except Exception:
            log.exception("Sync failed for %s", title)
    await update.message.reply_text(f"Synced {n} post(s).")


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

        # already handled (Telegram redelivery)?
        if conn.execute(
            "SELECT 1 FROM claims WHERE group_chat_id=? AND group_msg_id=?",
            (msg.chat_id, msg.message_id),
        ).fetchone():
            return

        user = msg.from_user
        first = user.first_name or "there"

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
            "user_name, group_chat_id, group_msg_id, claimed_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                product_id, variant_id, title, price, qty, user.id,
                user.full_name, msg.chat_id, msg.message_id, int(time.time()),
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
        f"💌 Tap the button below and press Start so I can send your invoice.",
        reply_markup=button,
    )
    await refresh_post(ctx.bot, channel_msg_id, title, price, remaining)


def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("myid", cmd_myid))
    app.add_handler(CommandHandler("post", cmd_post))
    app.add_handler(CommandHandler("claims", cmd_claims))
    app.add_handler(CommandHandler("sync", cmd_sync))
    app.add_handler(MessageHandler(filters.IS_AUTOMATIC_FORWARD, on_auto_forward), group=-1)
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, on_comment))
    log.info("Bot running")
    app.run_polling()


if __name__ == "__main__":
    main()
