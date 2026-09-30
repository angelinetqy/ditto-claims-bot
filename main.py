"""Ditto Deals claims bot - Stage 1: post Shopify items to the Telegram channel."""
import logging
import os
import sqlite3
import time

import httpx
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ditto")

# ---- config (set these as Railway environment variables) ----
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
SHOP = os.environ.get("SHOPIFY_STORE_DOMAIN", "dittodealsstore.myshopify.com")
CLIENT_ID = os.environ["SHOPIFY_CLIENT_ID"]
CLIENT_SECRET = os.environ["SHOPIFY_CLIENT_SECRET"]
CHANNEL = os.environ.get("CHANNEL_USERNAME", "@dittodeals")
ADMIN_IDS = {int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()}
DB_PATH = os.environ.get("DB_PATH", "/data/bot.db")
API_VERSION = os.environ.get("SHOPIFY_API_VERSION", "2026-01")
SALE_TAG = os.environ.get("SALE_TAG", "telegram")


# ---- tiny database: remembers which products were already posted ----
def db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS posts (
            product_id TEXT PRIMARY KEY,
            variant_id TEXT,
            channel_msg_id INTEGER,
            title TEXT,
            price TEXT,
            posted_at INTEGER
        )"""
    )
    return conn


# ---- Shopify client (Dev Dashboard apps: client-credentials token) ----
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


def make_caption(item) -> str:
    return (
        f"🎴 {item['title']}\n"
        f"Price: ${item['price']:.2f} SGD each\n"
        f"Quantity: {item['qty']}\n\n"
        f"💬 How to claim:\n"
        f'• Reply "claim" to accept the listed price'
    )


# ---- Telegram commands ----
def is_admin(update: Update) -> bool:
    return update.effective_user is not None and update.effective_user.id in ADMIN_IDS


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Hi! This is the Ditto Deals bot 💜")


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


def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("myid", cmd_myid))
    app.add_handler(CommandHandler("post", cmd_post))
    log.info("Bot running")
    app.run_polling()


if __name__ == "__main__":
    main()
