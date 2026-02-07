import os
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List

import discord
from discord.ext import commands, tasks

# -----------------------------
# Constants / File paths
# -----------------------------

DEALS_FILE = "deals_data.json"
LEADERBOARD_FILE = "leaderboard_data.json"
CONFIG_FILE = "server_config.json"

UTC = timezone.utc

# -----------------------------
# Intents & bot setup
# -----------------------------

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)

# -----------------------------
# Helpers: file I/O
# -----------------------------


def load_json(path: str, default: Any) -> Any:
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception:
            return default
    return default


def save_json(path: str, data: Any) -> None:
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# deals_data structure:
# {
#   "deals": {
#       "<normalized_customer_name>": {
#           "customer_name": "John Smith",
#           "setter_id": "123",
#           "setter_name": "Display Name",
#           "status": "set" | "sold" | "canceled",
#           "created_at": "...",
#           "closed_at": "...",
#           "closer_id": "...",
#           "closer_name": "...",
#           "kw_size": 8.5
#       },
#       ...
#   }
# }

deals_data = load_json(DEALS_FILE, {"deals": {}})


# leaderboard_data structure:
# {
#   "setters": {
#       "<user_id>": {
#           "username": "Display Name",
#           "appointments_set": int,
#           "appointments_closed": int,
#           "total_kw": float,
#           "deals": [ "<normalized_customer_name>", ... ]
#       }
#   },
#   "closers": {
#       "<user_id>": {
#           "username": "Display Name",
#           "deals_closed": int,
#           "total_kw": float,
#           "total_revenue": float,
#           "deals": [ "<normalized_customer_name>", ... ]
#       }
#   }
# }

leaderboard_data = load_json(
    LEADERBOARD_FILE,
    {"setters": {}, "closers": {}}
)

# server_config structure (per guild):
# {
#   "<guild_id>": {
#       "daily_channel_id": int | null,
#       "weekly_channel_id": int | null,
#       "monthly_channel_id": int | null,
#       "audit_channel_id": int | null,
#       "daily_summary_enabled": bool,
#       "revenue_enabled": bool,
#       "revenue_per_kw": float,
#       "pay_enabled": bool,
#       "pay_mode": "percent" | "flat",
#       "pay_setter_percent": float,
#       "pay_closer_percent": float,
#       "pay_setter_flat": float,
#       "pay_closer_flat": float
#   }
# }

server_config: Dict[str, Any] = load_json(CONFIG_FILE, {})


def save_deals() -> None:
    save_json(DEALS_FILE, deals_data)


def save_leaderboard() -> None:
    save_json(LEADERBOARD_FILE, leaderboard_data)


def save_config() -> None:
    save_json(CONFIG_FILE, server_config)


# -----------------------------
# Helpers: Guild config & channels
# -----------------------------


def get_guild_config(guild: discord.Guild) -> Dict[str, Any]:
    gid = str(guild.id)
    if gid not in server_config:
        server_config[gid] = {
            "daily_channel_id": None,
            "weekly_channel_id": None,
            "monthly_channel_id": None,
            "audit_channel_id": None,
            "daily_summary_enabled": True,
            "revenue_enabled": False,
            "revenue_per_kw": 0.0,
            "pay_enabled": False,
            "pay_mode": "percent",
            "pay_setter_percent": 50.0,
            "pay_closer_percent": 50.0,
            "pay_setter_flat": 0.0,
            "pay_closer_flat": 0.0,
        }
        save_config()
    return server_config[gid]


async def ensure_leaderboard_channels(guild: discord.Guild) -> None:
    """Create daily/weekly/monthly/audit channels if missing, and lock them to bot only."""
    cfg = get_guild_config(guild)

    async def get_or_create(name: str, purpose: str) -> Optional[int]:
        existing = discord.utils.get(guild.text_channels, name=name)
        if existing:
            channel = existing
        else:
            channel = await guild.create_text_channel(name, reason=f"Solar bot auto-created {purpose} channel")

        # Lock sending to everyone except bot
        overwrites = channel.overwrites
        everyone = guild.default_role
        bot_member = guild.me

        overwrites[everyone] = discord.PermissionOverwrite(send_messages=False, view_channel=True)
        overwrites[bot_member] = discord.PermissionOverwrite(send_messages=True, view_channel=True, manage_messages=True)
        await channel.edit(overwrites=overwrites)

        return channel.id

    # DAILY
    if not cfg.get("daily_channel_id"):
        cfg["daily_channel_id"] = await get_or_create("daily-leaderboard", "daily leaderboard")

    # WEEKLY
    if not cfg.get("weekly_channel_id"):
        cfg["weekly_channel_id"] = await get_or_create("weekly-leaderboard", "weekly leaderboard")

    # MONTHLY
    if not cfg.get("monthly_channel_id"):
        cfg["monthly_channel_id"] = await get_or_create("monthly-leaderboard", "monthly leaderboard")

    # AUDIT
    if not cfg.get("audit_channel_id"):
        cfg["audit_channel_id"] = await get_or_create("solar-audit-log", "audit log")

    save_config()


async def get_channel(guild: discord.Guild, channel_id: Optional[int]) -> Optional[discord.TextChannel]:
    if not channel_id:
        return None
    return guild.get_channel(channel_id)


async def send_audit(guild: discord.Guild, message: str) -> None:
    cfg = get_guild_config(guild)
    channel = await get_channel(guild, cfg.get("audit_channel_id"))
    if channel:
        await channel.send(message)


# -----------------------------
# Helpers: stats & names
# -----------------------------


def get_display_name(member: discord.Member) -> str:
    return member.display_name or member.name


def normalize_customer_name(raw: str) -> str:
    return " ".join(raw.split()).strip().lower()


def get_time_bounds(timeframe: str):
    now = datetime.now(UTC)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if timeframe == "today":
        return today_start
    if timeframe == "week":
        # Monday start
        return today_start - timedelta(days=today_start.weekday())
    if timeframe == "month":
        return today_start.replace(day=1)
    return None  # all-time


def update_setter_stats(user_id: str, username: str) -> None:
    setters = leaderboard_data["setters"]
    if user_id not in setters:
        setters[user_id] = {
            "username": username,
            "appointments_set": 0,
            "appointments_closed": 0,
            "total_kw": 0.0,
            "deals": [],
        }
    else:
        setters[user_id]["username"] = username


def update_closer_stats(user_id: str, username: str) -> None:
    closers = leaderboard_data["closers"]
    if user_id not in closers:
        closers[user_id] = {
            "username": username,
            "deals_closed": 0,
            "total_kw": 0.0,
            "total_revenue": 0.0,
            "deals": [],
        }
    else:
        closers[user_id]["username"] = username


def get_deal_by_customer_name(customer_name: str) -> Optional[Dict[str, Any]]:
    key = normalize_customer_name(customer_name)
    return deals_data["deals"].get(key)


def ensure_deal_record(customer_name: str) -> Dict[str, Any]:
    key = normalize_customer_name(customer_name)
    if key not in deals_data["deals"]:
        deals_data["deals"][key] = {
            "customer_name": customer_name,
            "setter_id": None,
            "setter_name": None,
            "status": "set",
            "created_at": datetime.now(UTC).isoformat(),
            "closed_at": None,
            "closer_id": None,
            "closer_name": None,
            "kw_size": None,
            "revenue": None,
        }
    return deals_data["deals"][key]


# -----------------------------
# Events
# -----------------------------


@bot.event
async def on_ready():
    print(f"{bot.user} has connected to Discord!")
    print(f"Bot is in {len(bot.guilds)} guild(s)")
    for guild in bot.guilds:
        await ensure_leaderboard_channels(guild)
    daily_summary_task.start()


@bot.event
async def on_guild_join(guild: discord.Guild):
    await ensure_leaderboard_channels(guild)
    await send_audit(guild, f"✅ Solar bot joined **{guild.name}** and created leaderboard channels.")


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not isinstance(message.author, discord.Member):
        return

    content_lower = message.content.lower()

    # -----------------------------
    # #set Name
    # -----------------------------
    if "#set" in content_lower:
        parts = message.content.split()
        try:
            idx = next(i for i, p in enumerate(parts) if p.lower() == "#set")
        except StopIteration:
            await bot.process_commands(message)
            return

        # Everything after #set is the customer name
        if idx + 1 >= len(parts):
            await message.channel.send("❌ Usage: `#set First Last` (customer name after #set)")
            await bot.process_commands(message)
            return

        raw_name = " ".join(parts[idx + 1:]).strip()
        if len(raw_name.split()) < 2:
            await message.channel.send("❌ Please use first and last name. Example: `#set John Smith`")
            await bot.process_commands(message)
            return

        member = message.author
        display_name = get_display_name(member)
        deal = ensure_deal_record(raw_name)

        deal["setter_id"] = str(member.id)
        deal["setter_name"] = display_name
        deal["status"] = "set"
        deal["created_at"] = datetime.now(UTC).isoformat()

        update_setter_stats(str(member.id), display_name)
        leaderboard_data["setters"][str(member.id)]["appointments_set"] += 1
        key = normalize_customer_name(raw_name)
        if key not in leaderboard_data["setters"][str(member.id)]["deals"]:
            leaderboard_data["setters"][str(member.id)]["deals"].append(key)

        save_deals()
        save_leaderboard()

        embed = discord.Embed(
            title="🎯 Appointment Set!",
            description=f"{member.mention} just set an appointment for **{deal['customer_name']}**",
            color=discord.Color.green(),
            timestamp=datetime.now(UTC),
        )
        embed.add_field(name="Customer", value=deal["customer_name"], inline=True)
        embed.add_field(name="Setter", value=display_name, inline=True)
        embed.add_field(name="Status", value="🔔 Pending Sale", inline=True)
        embed.set_footer(text="Close it later with: #sold First Last <kW>")

        await message.channel.send(embed=embed)
        await send_audit(message.guild, f"📝 #set by {display_name} for {deal['customer_name']}")

    # -----------------------------
    # #sold Name kW
    # -----------------------------
    elif "#sold" in content_lower:
        parts = message.content.split()
        try:
            idx = next(i for i, p in enumerate(parts) if p.lower() == "#sold")
        except StopIteration:
            await bot.process_commands(message)
            return

        if idx + 2 >= len(parts):
            await message.channel.send(
                "❌ Usage: `#sold First Last 8.5`\n"
                "Example: `#sold John Smith 8.5`"
            )
            await bot.process_commands(message)
            return

        # Name is everything between #sold and last token; last token is kW
        try:
            kw_size = float(parts[-1])
        except ValueError:
            await message.channel.send("❌ Last value must be the kW size. Example: `#sold John Smith 8.5`")
            await bot.process_commands(message)
            return

        raw_name = " ".join(parts[idx + 1:-1]).strip()
        deal = get_deal_by_customer_name(raw_name)
        if not deal:
            await message.channel.send(
                f"❌ I can't find a deal for **{raw_name}**.\n"
                "Make sure you set it first with `#set First Last`."
            )
            await bot.process_commands(message)
            return

        if deal["status"] == "sold":
            await message.channel.send(
                f"❌ This deal for **{deal['customer_name']}** is already marked as sold."
            )
            await bot.process_commands(message)
            return

        member = message.author
        display_name = get_display_name(member)

        deal["status"] = "sold"
        deal["closed_at"] = datetime.now(UTC).isoformat()
        deal["closer_id"] = str(member.id)
        deal["closer_name"] = display_name
        deal["kw_size"] = kw_size

        # Revenue logic per guild
        cfg = get_guild_config(message.guild)
        revenue = None
        if cfg.get("revenue_enabled") and cfg.get("revenue_per_kw", 0) > 0:
            revenue = kw_size * cfg["revenue_per_kw"]
        deal["revenue"] = revenue

        # Update closer stats
        update_closer_stats(str(member.id), display_name)
        closer_stats = leaderboard_data["closers"][str(member.id)]
        closer_stats["deals_closed"] += 1
        closer_stats["total_kw"] += kw_size
        if revenue:
            closer_stats["total_revenue"] += revenue
        key = normalize_customer_name(deal["customer_name"])
        if key not in closer_stats["deals"]:
            closer_stats["deals"].append(key)

        # Update setter stats
        if deal["setter_id"]:
            s_id = deal["setter_id"]
            update_setter_stats(s_id, deal["setter_name"] or "Unknown")
            setters = leaderboard_data["setters"][s_id]
            setters["appointments_closed"] += 1
            setters["total_kw"] += kw_size
            if key not in setters["deals"]:
                setters["deals"].append(key)

        save_deals()
        save_leaderboard()

        embed = discord.Embed(
            title="🎉 DEAL SOLD!",
            description=f"**{deal['customer_name']}** is now a closed deal!",
            color=discord.Color.gold(),
            timestamp=datetime.now(UTC),
        )
        embed.add_field(name="Customer", value=deal["customer_name"], inline=True)
        embed.add_field(name="System Size", value=f"{kw_size} kW", inline=True)
        if revenue:
            embed.add_field(name="Est. Revenue", value=f"${revenue:,.2f}", inline=True)
        embed.add_field(name="Setter", value=deal["setter_name"] or "Unknown", inline=False)
        embed.add_field(name="Closer", value=display_name, inline=False)

        await message.channel.send(embed=embed)
        await send_audit(
            message.guild,
            f"✅ #sold – {deal['customer_name']} | {kw_size} kW "
            f"(Setter: {deal['setter_name']}, Closer: {display_name})"
        )

    # -----------------------------
    # #canceled Name  (Manager/Admin only)
    # -----------------------------
    elif "#canceled" in content_lower:
        # Permissions check (Manager or Admin)
        roles = [r.name.lower() for r in message.author.roles]
        if "admin" not in roles and "manager" not in roles:
            await message.channel.send("❌ Only Admin or Manager can cancel deals.")
            await bot.process_commands(message)
            return

        parts = message.content.split()
        try:
            idx = next(i for i, p in enumerate(parts) if p.lower() == "#canceled")
        except StopIteration:
            await bot.process_commands(message)
            return

        if idx + 1 >= len(parts):
            await message.channel.send("❌ Usage: `#canceled First Last`")
            await bot.process_commands(message)
            return

        raw_name = " ".join(parts[idx + 1:]).strip()
        deal = get_deal_by_customer_name(raw_name)
        if not deal:
            await message.channel.send(f"❌ I can't find a deal for **{raw_name}**.")
            await bot.process_commands(message)
            return

        prev_status = deal["status"]
        key = normalize_customer_name(deal["customer_name"])

        # Roll back stats depending on current status
        if prev_status == "set":
            if deal["setter_id"] and deal["setter_id"] in leaderboard_data["setters"]:
                leaderboard_data["setters"][deal["setter_id"]]["appointments_set"] -= 1
        elif prev_status == "sold":
            kw = deal["kw_size"] or 0
            rev = deal["revenue"] or 0

            # Closer
            if deal["closer_id"] and deal["closer_id"] in leaderboard_data["closers"]:
                cstats = leaderboard_data["closers"][deal["closer_id"]]
                cstats["deals_closed"] -= 1
                cstats["total_kw"] -= kw
                cstats["total_revenue"] -= rev
                if key in cstats["deals"]:
                    cstats["deals"].remove(key)

            # Setter
            if deal["setter_id"] and deal["setter_id"] in leaderboard_data["setters"]:
                sstats = leaderboard_data["setters"][deal["setter_id"]]
                sstats["appointments_closed"] -= 1
                sstats["total_kw"] -= kw
                if key in sstats["deals"]:
                    sstats["deals"].remove(key)

        deal["status"] = "canceled"
        save_deals()
        save_leaderboard()

        embed = discord.Embed(
            title="🚫 Deal Canceled",
            description=f"Deal for **{deal['customer_name']}** has been canceled.",
            color=discord.Color.red(),
            timestamp=datetime.now(UTC),
        )
        embed.add_field(name="Previous Status", value=prev_status, inline=True)
        await message.channel.send(embed=embed)
        await send_audit(
            message.guild,
            f"🚫 #canceled – {deal['customer_name']} (previous status: {prev_status}) "
            f"by {get_display_name(message.author)}"
        )

    await bot.process_commands(message)


# -----------------------------
# Commands: Revenue & pay config (DM wizard)
# -----------------------------


@bot.command(name="configure_revenue", help="Admin/Manager only – configure revenue & pay (DM wizard)")
@commands.has_any_role("Admin", "Manager")
async def configure_revenue(ctx: commands.Context):
    """DM wizard to configure revenue + setter/closer pay."""
    guild = ctx.guild
    if guild is None:
        await ctx.reply("Run this command inside the server, not in DMs.")
        return

    cfg = get_guild_config(guild)

    try:
        dm = await ctx.author.create_dm()
    except discord.Forbidden:
        await ctx.reply("I can't DM you. Please enable DMs from server members and try again.")
        return

    await ctx.reply("📩 I just DMed you a configuration wizard for revenue & pay.")

    def check(m: discord.Message) -> bool:
        return m.author.id == ctx.author.id and m.channel.id == dm.id

    try:
        # 1) Enable revenue?
        await dm.send(
            "💰 **Revenue Tracking Setup**\n"
            "Do you want to enable revenue tracking on leaderboards? (yes/no)"
        )
        msg = await bot.wait_for("message", check=check, timeout=120)
        enable_rev = msg.content.strip().lower() in ["yes", "y"]

        cfg["revenue_enabled"] = enable_rev

        if enable_rev:
            await dm.send(
                "Great. How much revenue per kW?\n"
                "Example: if revenue is $400 per kW, reply with `400`."
            )
            while True:
                msg = await bot.wait_for("message", check=check, timeout=120)
                try:
                    per_kw = float(msg.content.strip())
                    if per_kw <= 0:
                        raise ValueError()
                    cfg["revenue_per_kw"] = per_kw
                    break
                except ValueError:
                    await dm.send("Please enter a positive number, like `400`.")

        else:
            cfg["revenue_per_kw"] = 0.0

        # 2) Enable pay?
        await dm.send(
            "👥 **Setter / Closer Pay Setup**\n"
            "Do you want me to calculate payouts for setter/closer on the manager leaderboard? (yes/no)"
        )
        msg = await bot.wait_for("message", check=check, timeout=120)
        enable_pay = msg.content.strip().lower() in ["yes", "y"]
        cfg["pay_enabled"] = enable_pay

        if enable_pay:
            await dm.send(
                "How do you want to define pay?\n"
                "- Type `percent` for % of revenue\n"
                "- Type `flat` for flat dollar amounts per deal"
            )
            while True:
                msg = await bot.wait_for("message", check=check, timeout=120)
                mode = msg.content.strip().lower()
                if mode in ["percent", "flat"]:
                    cfg["pay_mode"] = mode
                    break
                await dm.send("Please reply with `percent` or `flat`.")

            if cfg["pay_mode"] == "percent":
                await dm.send(
                    "Enter setter % and closer % of revenue.\n"
                    "Example: `40 60` (setter 40%, closer 60%)"
                )
                while True:
                    msg = await bot.wait_for("message", check=check, timeout=120)
                    parts = msg.content.replace(",", " ").split()
                    if len(parts) != 2:
                        await dm.send("Please enter two numbers, like `40 60`.")
                        continue
                    try:
                        s_pct = float(parts[0])
                        c_pct = float(parts[1])
                        if s_pct < 0 or c_pct < 0 or abs(s_pct + c_pct - 100) > 0.01:
                            await dm.send("Percents must be >= 0 and sum to 100. Try again, e.g. `40 60`.")
                            continue
                        cfg["pay_setter_percent"] = s_pct
                        cfg["pay_closer_percent"] = c_pct
                        break
                    except ValueError:
                        await dm.send("Please enter valid numbers like `40 60`.")
            else:
                await dm.send(
                    "Enter flat dollar amounts per deal for setter and closer.\n"
                    "Example: `100 300` (setter $100, closer $300)"
                )
                while True:
                    msg = await bot.wait_for("message", check=check, timeout=120)
                    parts = msg.content.replace(",", " ").split()
                    if len(parts) != 2:
                        await dm.send("Please enter two numbers, like `100 300`.")
                        continue
                    try:
                        s_flat = float(parts[0])
                        c_flat = float(parts[1])
                        if s_flat < 0 or c_flat < 0:
                            await dm.send("Values must be >= 0. Try again.")
                            continue
                        cfg["pay_setter_flat"] = s_flat
                        cfg["pay_closer_flat"] = c_flat
                        break
                    except ValueError:
                        await dm.send("Please enter valid numbers like `100 300`.")

        save_config()

        summary_lines = []
        summary_lines.append(f"Revenue enabled: **{cfg['revenue_enabled']}**")
        if cfg["revenue_enabled"]:
            summary_lines.append(f"Revenue per kW: **${cfg['revenue_per_kw']:.2f}**")

        summary_lines.append(f"Pay enabled: **{cfg['pay_enabled']}**")
        if cfg["pay_enabled"]:
            if cfg["pay_mode"] == "percent":
                summary_lines.append(
                    f"Pay mode: **percent** – Setter {cfg['pay_setter_percent']}%, "
                    f"Closer {cfg['pay_closer_percent']}%"
                )
            else:
                summary_lines.append(
                    f"Pay mode: **flat** – Setter ${cfg['pay_setter_flat']:.2f}, "
                    f"Closer ${cfg['pay_closer_flat']:.2f}"
                )

        await dm.send(
            "✅ Configuration saved.\n\n" + "\n".join(summary_lines)
        )
        await send_audit(guild, f"⚙️ Revenue/pay configuration updated by {get_display_name(ctx.author)}.")

    except Exception as e:
        await dm.send(f"❌ Wizard aborted or timed out.\nError: {e}")


# -----------------------------
# Commands: Leaderboards & stats
# -----------------------------


def _filter_deals_by_time(deal_keys: List[str], start_time: Optional[datetime]) -> List[str]:
    if start_time is None:
        return deal_keys
    out = []
    for key in deal_keys:
        deal = deals_data["deals"].get(key)
        if not deal or not deal.get("closed_at"):
            continue
        try:
            closed = datetime.fromisoformat(deal["closed_at"])
        except Exception:
            continue
        if closed >= start_time:
            out.append(key)
    return out


@bot.command(name="leaderboard", help="Show team leaderboard (no revenue)")
async def leaderboard(ctx: commands.Context, timeframe: str = "all"):
    timeframe = timeframe.lower()
    if timeframe not in ["all", "today", "week", "month"]:
        timeframe = "all"

    start_time = get_time_bounds(timeframe)

    embed = discord.Embed(
        title="🏆 Solar Sales Leaderboard",
        description=f"Timeframe: **{timeframe}**",
        color=discord.Color.gold(),
        timestamp=datetime.now(UTC),
    )

    # Closers
    closers_rows = []
    for uid, data in leaderboard_data["closers"].items():
        deal_keys = data["deals"]
        filtered = _filter_deals_by_time(deal_keys, start_time)
        deals_count = len(filtered)
        total_kw = 0.0
        for key in filtered:
            d = deals_data["deals"].get(key)
            if d and d.get("kw_size"):
                total_kw += d["kw_size"]
        closers_rows.append(
            {
                "name": data["username"],
                "deals": deals_count,
                "kw": total_kw,
            }
        )
    closers_rows.sort(key=lambda x: (x["deals"], x["kw"]), reverse=True)

    # Setters
    setters_rows = []
    for uid, data in leaderboard_data["setters"].items():
        deal_keys = data["deals"]
        filtered = _filter_deals_by_time(deal_keys, start_time)
        closed_count = len(filtered)

        if start_time is None:
            appts_set = data["appointments_set"]
        else:
            appts_set = 0
            for key, d in deals_data["deals"].items():
                if d.get("setter_id") == uid and d.get("created_at"):
                    try:
                        created = datetime.fromisoformat(d["created_at"])
                    except Exception:
                        continue
                    if created >= start_time:
                        appts_set += 1

        close_rate = (closed_count / appts_set * 100) if appts_set > 0 else 0.0
        setters_rows.append(
            {
                "name": data["username"],
                "appts_set": appts_set,
                "closed": closed_count,
                "close_rate": close_rate,
            }
        )
    setters_rows.sort(key=lambda x: (x["closed"], x["appts_set"]), reverse=True)

    # Closers field
    if closers_rows:
        medals = ["🥇", "🥈", "🥉"]
        text = ""
        for i, row in enumerate(closers_rows[:5]):
            medal = medals[i] if i < 3 else f"{i+1}."
            text += f"{medal} **{row['name']}** – {row['deals']} sold | {row['kw']:.1f} kW\n"
        embed.add_field(name="👔 Top Closers", value=text, inline=False)

    # Setters field
    if setters_rows:
        medals = ["🥇", "🥈", "🥉"]
        text = ""
        for i, row in enumerate(setters_rows[:5]):
            medal = medals[i] if i < 3 else f"{i+1}."
            text += (
                f"{medal} **{row['name']}** – {row['closed']} sold | "
                f"{row['appts_set']} set ({row['close_rate']:.0f}%)\n"
            )
        embed.add_field(name="📞 Top Setters", value=text, inline=False)

    # Overall stats (all time)
    total_deals = 0
    total_kw_all = 0.0
    for d in deals_data["deals"].values():
        if d.get("status") == "sold":
            total_deals += 1
            if d.get("kw_size"):
                total_kw_all += d["kw_size"]

    embed.add_field(
        name="📊 Company Stats (All Time)",
        value=f"💼 **Total Deals:** {total_deals}\n⚡ **Total kW:** {total_kw_all:.1f}",
        inline=False,
    )
    embed.set_footer(text="Timeframes: all, today, week, month | Example: !leaderboard week")

    await ctx.send(embed=embed)


@bot.command(name="managerboard", help="Manager/Admin – leaderboard with revenue & payouts")
@commands.has_any_role("Admin", "Manager")
async def managerboard(ctx: commands.Context, timeframe: str = "all"):
    guild = ctx.guild
    if guild is None:
        await ctx.reply("Run this in the server, not in DMs.")
        return

    cfg = get_guild_config(guild)
    timeframe = timeframe.lower()
    if timeframe not in ["all", "today", "week", "month"]:
        timeframe = "all"

    start_time = get_time_bounds(timeframe)

    embed = discord.Embed(
        title="📊 Manager Leaderboard (Revenue + Pay)",
        description=f"Timeframe: **{timeframe}**",
        color=discord.Color.blue(),
        timestamp=datetime.now(UTC),
    )

    # Build per-closer revenue
    rev_enabled = cfg.get("revenue_enabled", False) and cfg.get("revenue_per_kw", 0) > 0
    pay_enabled = cfg.get("pay_enabled", False)

    closer_rows = []
    for uid, data in leaderboard_data["closers"].items():
        deal_keys = _filter_deals_by_time(data["deals"], start_time)
        deals_count = len(deal_keys)
        total_kw = 0.0
        total_rev = 0.0
        setter_pay = 0.0
        closer_pay = 0.0

        for key in deal_keys:
            d = deals_data["deals"].get(key)
            if not d or d.get("status") != "sold":
                continue
            k = d.get("kw_size") or 0
            total_kw += k
            if rev_enabled:
                r = (d.get("revenue")
                     if d.get("revenue") is not None
                     else k * cfg["revenue_per_kw"])
                total_rev += r
                if pay_enabled:
                    if cfg["pay_mode"] == "percent":
                        setter_pay += r * (cfg["pay_setter_percent"] / 100.0)
                        closer_pay += r * (cfg["pay_closer_percent"] / 100.0)
                    else:
                        setter_pay += cfg["pay_setter_flat"]
                        closer_pay += cfg["pay_closer_flat"]

        closer_rows.append(
            {
                "name": data["username"],
                "deals": deals_count,
                "kw": total_kw,
                "revenue": total_rev,
                "setter_pay": setter_pay,
                "closer_pay": closer_pay,
            }
        )

    closer_rows.sort(key=lambda x: (x["deals"], x["kw"]), reverse=True)

    text = ""
    for i, row in enumerate(closer_rows[:10]):
        line = f"**{i+1}. {row['name']}** – {row['deals']} sold | {row['kw']:.1f} kW"
        if rev_enabled:
            line += f" | Rev: ${row['revenue']:,.0f}"
        if pay_enabled:
            line += (
                f" | Setter Pay: ${row['setter_pay']:,.0f}"
                f" | Closer Pay: ${row['closer_pay']:,.0f}"
            )
        text += line + "\n"

    if not text:
        text = "No data yet."

    embed.add_field(name="Managers View", value=text, inline=False)

    settings_lines = [
        f"Revenue enabled: **{cfg['revenue_enabled']}**",
        f"Revenue / kW: **${cfg['revenue_per_kw']:.2f}**" if cfg["revenue_enabled"] else "",
        f"Pay enabled: **{cfg['pay_enabled']}**",
    ]

    if cfg["pay_enabled"]:
        if cfg["pay_mode"] == "percent":
            settings_lines.append(
                f"Pay mode: **percent** – Setter {cfg['pay_setter_percent']}%, "
                f"Closer {cfg['pay_closer_percent']}%"
            )
        else:
            settings_lines.append(
                f"Pay mode: **flat** – Setter ${cfg['pay_setter_flat']:.2f}, "
                f"Closer ${cfg['pay_closer_flat']:.2f}"
            )

    embed.add_field(
        name="Config Snapshot",
        value="\n".join([line for line in settings_lines if line]),
        inline=False,
    )

    await ctx.send(embed=embed)


@bot.command(name="mystats", help="View your personal stats")
async def mystats(ctx: commands.Context):
    user_id = str(ctx.author.id)
    embed = discord.Embed(
        title=f"📊 Stats for {get_display_name(ctx.author)}",
        color=discord.Color.blue(),
        timestamp=datetime.now(UTC),
    )

    # Setter
    if user_id in leaderboard_data["setters"]:
        s = leaderboard_data["setters"][user_id]
        close_rate = (
            s["appointments_closed"] / s["appointments_set"] * 100
            if s["appointments_set"] > 0
            else 0.0
        )
        text = (
            f"📞 **Appointments Set:** {s['appointments_set']}\n"
            f"✅ **Closed (as setter):** {s['appointments_closed']}\n"
            f"📈 **Close Rate:** {close_rate:.1f}%\n"
            f"⚡ **Total kW:** {s['total_kw']:.1f}"
        )
        embed.add_field(name="Setter Performance", value=text, inline=False)

    # Closer
    if user_id in leaderboard_data["closers"]:
        c = leaderboard_data["closers"][user_id]
        avg_kw = c["total_kw"] / c["deals_closed"] if c["deals_closed"] > 0 else 0.0
        text = (
            f"🤝 **Deals Closed:** {c['deals_closed']}\n"
            f"⚡ **Total kW:** {c['total_kw']:.1f}\n"
            f"📊 **Avg System Size:** {avg_kw:.1f} kW"
        )
        embed.add_field(name="Closer Performance", value=text, inline=False)

    if (
        user_id not in leaderboard_data["setters"]
        and user_id not in leaderboard_data["closers"]
    ):
        embed.description = "No stats yet – start with `#set First Last`."

    await ctx.send(embed=embed)


@bot.command(name="todaystats", help="Show today's performance")
async def todaystats(ctx: commands.Context):
    today_start = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)

    today_set = 0
    today_sold = 0
    total_kw = 0.0

    for d in deals_data["deals"].values():
        if d.get("created_at"):
            try:
                created = datetime.fromisoformat(d["created_at"])
            except Exception:
                created = None
        else:
            created = None
        if created and created >= today_start:
            today_set += 1

        if d.get("status") == "sold" and d.get("closed_at"):
            try:
                closed = datetime.fromisoformat(d["closed_at"])
            except Exception:
                closed = None
            if closed and closed >= today_start:
                today_sold += 1
                if d.get("kw_size"):
                    total_kw += d["kw_size"]

    embed = discord.Embed(
        title="📅 Today's Performance",
        color=discord.Color.green(),
        timestamp=datetime.now(UTC),
    )
    embed.add_field(name="📞 Appointments Set", value=str(today_set), inline=True)
    embed.add_field(name="✅ Deals Sold", value=str(today_sold), inline=True)
    embed.add_field(name="⚡ Total kW", value=f"{total_kw:.1f}", inline=True)

    await ctx.send(embed=embed)


@bot.command(name="pendingdeals", help="Show all pending (set but not sold/canceled)")
async def pendingdeals(ctx: commands.Context):
    pending = [
        d for d in deals_data["deals"].values()
        if d.get("status") == "set"
    ]
    if not pending:
        await ctx.send("✅ No pending deals – everything is either sold or canceled.")
        return

    embed = discord.Embed(
        title="🔔 Pending Deals",
        description=f"{len(pending)} deal(s) set and not sold yet",
        color=discord.Color.orange(),
        timestamp=datetime.now(UTC),
    )

    for d in pending[:10]:
        created = d.get("created_at")
        created_str = ""
        if created:
            try:
                created_str = datetime.fromisoformat(created).strftime("%m/%d %H:%M")
            except Exception:
                created_str = created
        embed.add_field(
            name=d.get("customer_name", "Unknown"),
            value=f"Setter: {d.get('setter_name', 'Unknown')}\nCreated: {created_str}",
            inline=True,
        )

    if len(pending) > 10:
        embed.set_footer(text=f"Showing first 10 of {len(pending)} pending deals")

    await ctx.send(embed=embed)


# -----------------------------
# Pinned commands helper
# -----------------------------


@bot.command(name="pin_commands", help="Admin/Manager – pin command cheat sheet in this channel")
@commands.has_any_role("Admin", "Manager")
async def pin_commands(ctx: commands.Context):
    embed = discord.Embed(
        title="📌 Solar Tracker – How to Use Commands",
        description=(
            "**IMPORTANT:**\n"
            "👉 Run these commands in a **general or sales channel**, not in this leaderboard channel.\n\n"
            "**Rep Commands**\n"
            "• `#set First Last` – Log a new appointment\n"
            "• `#sold First Last 8.5` – Mark deal as sold\n\n"
            "**Manager / Admin**\n"
            "• `#canceled First Last` – Cancel a deal\n\n"
            "**Stats & Leaderboards**\n"
            "• `!leaderboard [timeframe]` – Team leaderboard (all/today/week/month)\n"
            "• `!mystats` – Your personal stats\n"
            "• `!todaystats` – Today's performance\n"
            "• `!managerboard [timeframe]` – Manager view (revenue + pay)\n\n"
            "⚠️ This channel is read-only for leaderboard updates."
        ),
        color=0x00B894,
    )

    msg = await ctx.send(embed=embed)

    # Unpin older bot pins to avoid clutter
    pins = await ctx.channel.pins()
    for p in pins:
        if p.author == bot.user:
            await p.unpin()

    await msg.pin()
    # Clean up the command message to keep channel spotless
    try:
        await ctx.message.delete()
    except discord.Forbidden:
        pass


# -----------------------------
# Daily summary task (lightweight)
# -----------------------------


@tasks.loop(hours=24)
async def daily_summary_task():
    # This could be expanded later; for now it's a no-op to avoid extra noise.
    # Keeping the task so we can easily add daily recap broadcasts if you want.
    return


# -----------------------------
# Help command override (solar-specific)
# -----------------------------


@bot.command(name="help_solar", help="Show solar tracking command guide")
async def help_solar(ctx: commands.Context):
    embed = discord.Embed(
        title="☀️ Solar Deal Tracker – Command Guide",
        description="Track appointments, sold deals, and leaderboards.",
        color=discord.Color.blue(),
    )
    embed.add_field(
        name="📝 Hashtags",
        value=(
            "`#set First Last` – Appointment set for customer\n"
            "`#sold First Last 8.5` – Deal sold with kW\n"
            "`#canceled First Last` – Cancel deal (Admin/Manager only)"
        ),
        inline=False,
    )
    embed.add_field(
        name="📊 Leaderboards & Stats",
        value=(
            "`!leaderboard [timeframe]` – Team leaderboard (all/today/week/month)\n"
            "`!mystats` – Your stats\n"
            "`!todaystats` – Today's performance\n"
            "`!pendingdeals` – Deals set but not sold\n"
            "`!managerboard [timeframe]` – Manager view (revenue + pay)"
        ),
        inline=False,
    )
    embed.add_field(
        name="⚙️ Config (Owners)",
        value=(
            "`!configure_revenue` – DM wizard for revenue & pay\n"
            "`!pin_commands` – Pin the commands cheat sheet in a leaderboard channel"
        ),
        inline=False,
    )
    embed.set_footer(text="Built for solar sales teams 🌞")
    await ctx.send(embed=embed)


# -----------------------------
# Admin: delete deal (optional)
# -----------------------------


@bot.command(name="deletedeal", help="Admin only – hard delete a deal by customer name")
@commands.has_permissions(administrator=True)
async def deletedeal(ctx: commands.Context, *, customer_name: str):
    key = normalize_customer_name(customer_name)
    deal = deals_data["deals"].get(key)
    if not deal:
        await ctx.send(f"❌ No deal found for **{customer_name}**.")
        return

    # light rollback similar to #canceled
    prev_status = deal["status"]
    kw = deal.get("kw_size") or 0
    rev = deal.get("revenue") or 0

    if prev_status == "set":
        if deal["setter_id"] and deal["setter_id"] in leaderboard_data["setters"]:
            leaderboard_data["setters"][deal["setter_id"]]["appointments_set"] -= 1
    elif prev_status == "sold":
        if deal["closer_id"] and deal["closer_id"] in leaderboard_data["closers"]:
            cstats = leaderboard_data["closers"][deal["closer_id"]]
            cstats["deals_closed"] -= 1
            cstats["total_kw"] -= kw
            cstats["total_revenue"] -= rev
            if key in cstats["deals"]:
                cstats["deals"].remove(key)
        if deal["setter_id"] and deal["setter_id"] in leaderboard_data["setters"]:
            sstats = leaderboard_data["setters"][deal["setter_id"]]
            sstats["appointments_closed"] -= 1
            sstats["total_kw"] -= kw
            if key in sstats["deals"]:
                sstats["deals"].remove(key)

    del deals_data["deals"][key]
    save_deals()
    save_leaderboard()

    await ctx.send(f"✅ Deal for **{customer_name}** has been deleted and stats updated.")
    await send_audit(
        ctx.guild,
        f"🗑️ deletedeal – {customer_name} removed by {get_display_name(ctx.author)}"
    )


# -----------------------------
# Run the bot
# -----------------------------

if __name__ == "__main__":
    TOKEN = os.getenv("DISCORD_BOT_TOKEN")
    if not TOKEN:
        print("Error: DISCORD_BOT_TOKEN environment variable not set!")
    else:
        bot.run(TOKEN)
