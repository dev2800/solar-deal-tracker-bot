
import os
import json
import csv
import asyncio
from datetime import datetime, timedelta, timezone
import discord
from discord.ext import commands

DATA_DIR = "./data"
os.makedirs(DATA_DIR, exist_ok=True)

DEALS_FILE = f"{DATA_DIR}/deals.json"
CONFIG_FILE = f"{DATA_DIR}/server_config.json"

LOSS_REASONS = {
    "1": "ghosted",
    "2": "one_legger",
    "3": "needs_thought",
    "4": "disqualified",
    "5": "other"
}

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)

# ------------------------
# Utilities
# ------------------------

def load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, "r") as f:
        return json.load(f)

def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def now():
    return datetime.now(timezone.utc).isoformat()

deals = load_json(DEALS_FILE, {})
config = load_json(CONFIG_FILE, {
    "revenue_enabled": False,
    "revenue_per_kw": 0,
    "ghl_enabled": False,
    "ghl_webhook": None
})

# ------------------------
# GHL Webhook
# ------------------------

async def send_ghl_event(event, payload):
    if not config.get("ghl_enabled") or not config.get("ghl_webhook"):
        return
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(config["ghl_webhook"], json={
                "event": event,
                **payload
            })
    except Exception as e:
        print("GHL webhook error:", e)

# ------------------------
# Bot Events
# ------------------------

@bot.event
async def on_ready():
    print(f"{bot.user} connected")

# ------------------------
# Commands
# ------------------------

@bot.command()
async def pin_commands(ctx):
    embed = discord.Embed(
        title="📌 Solar Tracker Commands",
        description=(
            "**IMPORTANT:** Run commands in a **general / sales channel**, not here.\n\n"
            "**Rep Commands**\n"
            "`#set First Last`\n"
            "`#sold First Last [kW]`\n\n"
            "**Manager/Admin**\n"
            "`#canceled First Last`\n\n"
            "**Stats**\n"
            "`!leaderboard`\n"
            "`!mystats`\n"
        ),
        color=0x00b894
    )

    msg = await ctx.send(embed=embed)

    pins = await ctx.channel.pins()
    for p in pins:
        if p.author == bot.user:
            await p.unpin()

    await msg.pin()
    await ctx.message.delete()

# ------------------------
# Message Listener
# ------------------------

@bot.event
async def on_message(message):
    if message.author.bot:
        return

    content = message.content.strip()

    # SET
    if content.lower().startswith("#set "):
        name = content[5:].strip()
        deal_id = f"{name.lower()}_{int(datetime.now().timestamp())}"
        deals[deal_id] = {
            "name": name,
            "setter": message.author.display_name,
            "closer": None,
            "kw": None,
            "status": "set",
            "loss_reason": None,
            "created_at": now()
        }
        save_json(DEALS_FILE, deals)
        await message.channel.send(f"✅ Appointment set for **{name}**")

    # SOLD
    elif content.lower().startswith("#sold "):
        parts = content.split()
        name = " ".join(parts[1:-1])
        kw = float(parts[-1]) if parts[-1].replace(".", "", 1).isdigit() else None

        for deal in deals.values():
            if deal["name"].lower() == name.lower():
                deal["status"] = "sold"
                deal["closer"] = message.author.display_name
                deal["kw"] = kw
                save_json(DEALS_FILE, deals)
                await message.channel.send(f"🎉 **{name}** sold!")
                break

    # CANCELED (guided)
    elif content.lower().startswith("#canceled "):
        name = content[10:].strip()
        for deal_id, deal in deals.items():
            if deal["name"].lower() == name.lower():
                deal["status"] = "canceled"
                await message.author.send(
                    "Why did this deal not close?\n"
                    "1️⃣ Ghosted\n"
                    "2️⃣ One-legger\n"
                    "3️⃣ Needs to think\n"
                    "4️⃣ Disqualified\n"
                    "5️⃣ Other"
                )

                def check(m):
                    return m.author == message.author and isinstance(m.channel, discord.DMChannel)

                reply = await bot.wait_for("message", check=check)
                reason = LOSS_REASONS.get(reply.content.strip(), "other")

                if reason == "other":
                    await message.author.send("Please type the reason:")
                    reply = await bot.wait_for("message", check=check)
                    reason = reply.content.strip()

                deal["loss_reason"] = reason
                save_json(DEALS_FILE, deals)
                await message.channel.send(f"❌ **{name}** marked canceled ({reason})")
                break

    await bot.process_commands(message)

# ------------------------
# Stats
# ------------------------

@bot.command()
async def mystats(ctx):
    total = 0
    closed = 0
    losses = {}

    for d in deals.values():
        if d["setter"] == ctx.author.display_name:
            total += 1
            if d["status"] == "sold":
                closed += 1
            elif d["status"] == "canceled":
                losses[d["loss_reason"]] = losses.get(d["loss_reason"], 0) + 1

    embed = discord.Embed(title="📊 Your Stats", color=0x0984e3)
    embed.add_field(name="Appointments", value=total)
    embed.add_field(name="Closed", value=closed)

    for reason, count in losses.items():
        embed.add_field(name=reason, value=f"{(count/total)*100:.1f}%")

    await ctx.send(embed=embed)

# ------------------------
# CSV Export
# ------------------------

@bot.command()
@commands.has_any_role("Admin", "Manager")
async def export_csv(ctx, period="all"):
    filename = f"/tmp/deals_{period}.csv"
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Name", "Setter", "Closer", "Status", "kW", "Loss Reason"])
        for d in deals.values():
            writer.writerow([
                d["name"],
                d["setter"],
                d.get("closer"),
                d["status"],
                d.get("kw"),
                d.get("loss_reason")
            ])
    await ctx.send(file=discord.File(filename))

# ------------------------
# Run
# ------------------------

TOKEN = os.getenv("DISCORD_BOT_TOKEN")
if not TOKEN:
    print("DISCORD_BOT_TOKEN not set")
else:
    bot.run(TOKEN)
