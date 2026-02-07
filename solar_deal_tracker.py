import os
import json
import csv
import asyncio
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

import discord
from discord.ext import commands

# ------------------------
# Paths & Data
# ------------------------

DATA_DIR = "./data"
os.makedirs(DATA_DIR, exist_ok=True)

DEALS_FILE = os.path.join(DATA_DIR, "deals.json")
CONFIG_FILE = os.path.join(DATA_DIR, "server_config.json")

# Loss reasons for no-sale
LOSS_REASONS = {
    "1": "ghosted",
    "2": "one_legger",
    "3": "needs_thought",
    "4": "disqualified",
    "5": "other",
}

LOSS_REASON_LABELS = {
    "ghosted": "Ghosted",
    "one_legger": "One-legger",
    "needs_thought": "Needs to think",
    "disqualified": "Disqualified",
    "other": "Other / Misc",
}

# Hashtag prefixes
INTENT_PREFIX_SET = "#set "
INTENT_PREFIX_SOLD = "#sold "
INTENT_PREFIX_NOSALE = "#nosale "
INTENT_PREFIX_CANCELED = "#canceled "

# Roles that can do admin actions
ADMIN_ROLES = {"Admin", "Manager"}

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)

# ------------------------
# Helpers
# ------------------------


def _load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso() -> str:
    return utc_now().isoformat()


# Load persisted data
deals: Dict[str, Dict[str, Any]] = _load_json(DEALS_FILE, {})
config: Dict[str, Any] = _load_json(
    CONFIG_FILE,
    {
        "revenue_enabled": False,
        "revenue_per_kw": 0.0,  # dollars per kW
        "ghl_enabled": False,
        "ghl_webhook": None,
        "next_internal_id": 1000,
    },
)

# Ensure next_internal_id exists (for older installs)
if "next_internal_id" not in config:
    max_existing = 999
    for d in deals.values():
        if isinstance(d.get("internal_id"), int):
            max_existing = max(max_existing, d["internal_id"])
    config["next_internal_id"] = max_existing + 1
    _save_json(CONFIG_FILE, config)


def _get_next_internal_id() -> int:
    internal_id = int(config.get("next_internal_id", 1000))
    config["next_internal_id"] = internal_id + 1
    _save_json(CONFIG_FILE, config)
    return internal_id


def _normalize_name(name: str) -> str:
    return " ".join(name.strip().split()).lower()


def _find_deal_by_name(
    name: str, preferred_statuses: Optional[List[str]] = None
) -> Optional[Dict[str, Any]]:
    """Return the most recently created deal for this customer name."""
    target = _normalize_name(name)
    candidates: List[Dict[str, Any]] = []
    for deal in deals.values():
        if _normalize_name(deal.get("name", "")) == target:
            if preferred_statuses is None or deal.get("status") in preferred_statuses:
                candidates.append(deal)
    if not candidates:
        return None
    candidates.sort(key=lambda d: d.get("created_at", ""), reverse=True)
    return candidates[0]


def _parse_kw(value: str) -> Optional[float]:
    try:
        cleaned = value.replace("k", "").replace("kw", "")
        return float(cleaned)
    except Exception:
        return None


def _compute_revenue(kw: Optional[float]) -> Optional[float]:
    if not kw:
        return None
    if not config.get("revenue_enabled"):
        return None
    per_kw = float(config.get("revenue_per_kw") or 0.0)
    if per_kw <= 0:
        return None
    return kw * per_kw


def _compute_closer_streak(closer_id: int) -> int:
    """Consecutive days (including today) this closer has at least one sold deal."""
    dates = set()
    for d in deals.values():
        if d.get("status") == "sold" and d.get("closer_id") == closer_id:
            closed_at = d.get("closed_at")
            if not closed_at:
                continue
            try:
                dt = datetime.fromisoformat(closed_at)
            except Exception:
                continue
            dates.add(dt.date())

    if not dates:
        return 0

    streak = 0
    current_day = utc_now().date()
    while current_day in dates:
        streak += 1
        current_day = current_day - timedelta(days=1)
    return streak


async def _send_ghl_event(event: str, payload: Dict[str, Any]) -> None:
    """Optional GHL webhook (uses stdlib only)."""
    if not config.get("ghl_enabled") or not config.get("ghl_webhook"):
        return
    try:
        body = json.dumps({"event": event, **payload}).encode("utf-8")
        req = urllib.request.Request(
            config["ghl_webhook"],
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        print("GHL webhook error:", e)


def _user_is_admin(member: discord.Member) -> bool:
    if member.guild_permissions.administrator:
        return True
    role_names = {r.name for r in member.roles}
    return not ADMIN_ROLES.isdisjoint(role_names)


# ------------------------
# Events
# ------------------------


@bot.event
async def on_ready():
    print(f"{bot.user} is online in {len(bot.guilds)} server(s)")


# ------------------------
# Pin commands
# ------------------------


@bot.command(name="pin_commands", help="Post & pin the Solar Tracker command cheat sheet")
async def pin_commands(ctx: commands.Context):
    embed = discord.Embed(
        title="📌 Solar Tracker – Command Cheat Sheet",
        description=(
            "Run these in your **general/sales channel** (not in DMs).\n\n"
            "**Hashtags (type in channel)**\n"
            "• `#set First Last`\n"
            "• `#sold First Last kW`\n"
            "• `#nosale First Last`\n"
            "• `#canceled First Last`  *(admin/manager only, after a signed deal cancels)*\n\n"
            "**Commands**\n"
            "• `!leaderboard [all|today|week|month]`\n"
            "• `!mystats`\n"
            "• `!todaystats`\n"
            "• `!pendingdeals`\n"
            "• `!export_csv [period]` *(admin/manager)*\n"
            "• `!help_solar` – full guide\n"
        ),
        color=0x00B894,
    )

    msg = await ctx.send(embed=embed)

    # Unpin previous bot messages
    pins = await ctx.channel.pins()
    for p in pins:
        if p.author == bot.user:
            await p.unpin()

    await msg.pin()
    try:
        await ctx.message.delete()
    except Exception:
        pass


# ------------------------
# Help command
# ------------------------


@bot.command(name="help_solar", help="Show Solar Tracker help")
async def help_solar(ctx: commands.Context):
    embed = discord.Embed(
        title="☀️ Solar Tracker – Command Guide",
        description="Track sets, deals, no-sales, cancellations, and leaderboards.",
        color=discord.Color.blurple(),
    )

    embed.add_field(
        name="📝 Hashtag Workflows (in channel)",
        value=(
            "**Set appointment**\n"
            "`#set First Last`\n"
            "Example: `#set John Smith`\n\n"
            "**Sold (signed)**\n"
            "`#sold First Last kW`\n"
            "Example: `#sold John Smith 8.5`\n\n"
            "**No-sale (didn’t close)**\n"
            "`#nosale First Last`\n"
            "Example: `#nosale John Smith`\n\n"
            "**Canceled after signing**\n"
            "`#canceled First Last`\n"
            "*(Admin/Manager only)*"
        ),
        inline=False,
    )

    embed.add_field(
        name="📊 Stats & Leaderboards",
        value=(
            "`!leaderboard [timeframe]` – Team rankings (all/today/week/month)\n"
            "`!mystats` – Your personal stats & loss breakdown\n"
            "`!todaystats` – Today’s team performance\n"
            "`!pendingdeals` – Appointments not closed yet"
        ),
        inline=False,
    )

    embed.add_field(
        name="🧰 Manager / Admin Tools",
        value=(
            "`!export_csv [period]` – Export deals (today/week/month/all)\n"
            "`!set_revenue off` – Hide revenue\n"
            "`!set_revenue 400` – Enable revenue at $400 per kW\n"
            "`!pin_commands` – Post & pin cheat sheet in this channel"
        ),
        inline=False,
    )

    embed.set_footer(text="Tip: Run `!pin_commands` once in your main sales channel.")
    await ctx.send(embed=embed)


# ------------------------
# Hashtag Listener
# ------------------------


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild:
        return

    content = message.content.strip()

    # 1) APPOINTMENT SET
    if content.lower().startswith(INTENT_PREFIX_SET):
        customer_name = content[len(INTENT_PREFIX_SET):].strip()
        if not customer_name:
            await message.channel.send("❌ Please include the customer's name. Example: `#set John Smith`")
        else:
            internal_id = _get_next_internal_id()
            deal_id = f"deal_{internal_id}"

            deals[deal_id] = {
                "deal_id": deal_id,
                "internal_id": internal_id,
                "name": customer_name,
                "setter": message.author.display_name,
                "setter_id": message.author.id,
                "closer": None,
                "closer_id": None,
                "kw": None,
                "status": "set",
                "loss_reason": None,
                "loss_reason_detail": None,
                "cancel_reason": None,
                "created_at": utc_iso(),
                "closed_at": None,
                "canceled_at": None,
                "no_sale_at": None,
            }
            _save_json(DEALS_FILE, deals)

            embed = discord.Embed(
                title="🎯 Appointment Set!",
                description=f"{message.author.mention} just set an appointment!",
                color=discord.Color.green(),
                timestamp=utc_now(),
            )
            embed.add_field(name="Customer", value=customer_name, inline=True)
            embed.add_field(name="Setter", value=message.author.display_name, inline=True)
            embed.add_field(name="Status", value="🟡 Pending Close", inline=True)
            embed.add_field(name="Internal Deal ID", value=f"#{internal_id}", inline=True)
            embed.add_field(
                name="How to close this later",
                value="Use: `#sold First Last kW`\nExample: `#sold John Smith 8.5`",
                inline=False,
            )
            embed.set_footer(text="Solar Tracker • Track your deals in real-time")

            await message.channel.send(embed=embed)

    # 2) DEAL SOLD
    elif content.lower().startswith(INTENT_PREFIX_SOLD):
        parts = content.split()
        if len(parts) < 3:
            await message.channel.send("❌ Format: `#sold First Last kW` (example: `#sold John Smith 7.2`)")
            await bot.process_commands(message)
            return

        kw = _parse_kw(parts[-1])
        if kw is None:
            await message.channel.send("❌ Could not read kW size. Example: `#sold John Smith 7.2`")
            await bot.process_commands(message)
            return

        customer_name = " ".join(parts[1:-1]).strip()
        if not customer_name:
            await message.channel.send("❌ Please include the customer's name. Example: `#sold John Smith 7.2`")
            await bot.process_commands(message)
            return

        deal = _find_deal_by_name(customer_name, preferred_statuses=["set", "sold"])
        if not deal:
            await message.channel.send(
                f"❌ No open deal found for **{customer_name}**. "
                "Make sure you used `#set` first."
            )
            await bot.process_commands(message)
            return

        deal["status"] = "sold"
        deal["closer"] = message.author.display_name
        deal["closer_id"] = message.author.id
        deal["kw"] = kw
        deal["closed_at"] = utc_iso()

        revenue = _compute_revenue(kw)
        _save_json(DEALS_FILE, deals)

        await _send_ghl_event(
            "deal_sold",
            {
                "customer_name": deal["name"],
                "kw": kw,
                "revenue": revenue,
                "setter": deal.get("setter"),
                "closer": deal.get("closer"),
                "internal_id": deal.get("internal_id"),
            },
        )

        streak_days = _compute_closer_streak(message.author.id)

        embed = discord.Embed(
            title="🎉 DEAL CLOSED!",
            description=f"Deal for **{deal['name']}** has been closed!",
            color=discord.Color.gold(),
            timestamp=utc_now(),
        )
        embed.add_field(name="⚡ System Size", value=f"{kw:.1f} kW", inline=True)
        if revenue is not None:
            embed.add_field(name="💰 Est. Revenue", value=f"${revenue:,.2f}", inline=True)
        embed.add_field(
            name="👤 Setter",
            value=deal.get("setter") or "N/A",
            inline=False,
        )
        embed.add_field(
            name="🤝 Closer",
            value=deal.get("closer") or message.author.display_name,
            inline=False,
        )
        embed.add_field(
            name="Internal Deal ID",
            value=f"#{deal.get('internal_id')}",
            inline=True,
        )
        if streak_days > 0:
            embed.add_field(
                name="🔥 Closer Streak",
                value=f"{streak_days} day(s) in a row",
                inline=True,
            )

        await message.channel.send(embed=embed)

    # 3) NO-SALE (didn't close)
    elif content.lower().startswith(INTENT_PREFIX_NOSALE):
        customer_name = content[len(INTENT_PREFIX_NOSALE):].strip()
        if not customer_name:
            await message.channel.send("❌ Please include the customer's name. Example: `#nosale John Smith`")
            await bot.process_commands(message)
            return

        deal = _find_deal_by_name(customer_name, preferred_statuses=["set"])
        if not deal:
            await message.channel.send(
                f"❌ No pending appointment found for **{customer_name}**. "
                "Make sure it was logged with `#set` first."
            )
            await bot.process_commands(message)
            return

        deal["status"] = "no_sale"
        deal["no_sale_at"] = utc_iso()
        _save_json(DEALS_FILE, deals)

        # DM reason selection
        try:
            prompt = (
                f"Why did **{deal['name']}** not close?\n"
                "Reply with a number:\n"
                "1️⃣ Ghosted\n"
                "2️⃣ One-legger (only one decision maker)\n"
                "3️⃣ Needs to think\n"
                "4️⃣ Disqualified\n"
                "5️⃣ Other"
            )
            await message.author.send(prompt)

            def check(m: discord.Message) -> bool:
                return m.author == message.author and isinstance(m.channel, discord.DMChannel)

            reply = await bot.wait_for("message", timeout=120, check=check)
            key = reply.content.strip()
            reason_code = LOSS_REASONS.get(key, "other")

            if reason_code == "other":
                await message.author.send("Please type a short reason:")
                reply2 = await bot.wait_for("message", timeout=180, check=check)
                reason_text = reply2.content.strip()
            else:
                reason_text = LOSS_REASON_LABELS.get(reason_code, reason_code.title())

            deal["loss_reason"] = reason_code
            deal["loss_reason_detail"] = reason_text
            _save_json(DEALS_FILE, deals)

            await message.channel.send(f"🚫 **{deal['name']}** marked as no-sale ({reason_text}).")
        except asyncio.TimeoutError:
            await message.channel.send(
                f"⏱️ No loss reason received for **{deal['name']}**. "
                "You can DM the bot later if you want this updated."
            )
        except discord.Forbidden:
            await message.channel.send(
                "⚠️ I couldn't DM you for the loss reason. Please enable DMs from server members."
            )

    # 4) CANCELED AFTER SIGNING (admin/manager)
    elif content.lower().startswith(INTENT_PREFIX_CANCELED):
        if not isinstance(message.author, discord.Member) or not _user_is_admin(message.author):
            await message.channel.send("❌ Only Admin/Manager can mark deals as canceled after signing.")
            await bot.process_commands(message)
            return

        customer_name = content[len(INTENT_PREFIX_CANCELED):].strip()
        if not customer_name:
            await message.channel.send("❌ Please include the customer's name. Example: `#canceled John Smith`")
            await bot.process_commands(message)
            return

        deal = _find_deal_by_name(customer_name, preferred_statuses=["sold"])
        if not deal:
            await message.channel.send(
                f"❌ No *sold* deal found for **{customer_name}**. "
                "Only signed deals can be marked as canceled."
            )
            await bot.process_commands(message)
            return

        deal["status"] = "canceled_after_sign"
        deal["canceled_at"] = utc_iso()
        _save_json(DEALS_FILE, deals)

        await message.channel.send(
            f"❌ Deal for **{deal['name']}** has been marked as **canceled after signing**."
        )

    # Let normal commands run
    await bot.process_commands(message)


# ------------------------
# Stats & Leaderboard
# ------------------------


def _deal_in_timeframe(deal: Dict[str, Any], start: Optional[datetime]) -> bool:
    if not start:
        return True
    ts = deal.get("closed_at") or deal.get("created_at")
    if not ts:
        return False
    try:
        dt = datetime.fromisoformat(ts)
    except Exception:
        return False
    return dt >= start


@bot.command(name="todaystats", help="Show today's performance summary")
async def today_stats(ctx: commands.Context):
    today_start = utc_now().replace(hour=0, minute=0, second=0, microsecond=0)

    sets = 0
    sold = 0
    total_kw = 0.0
    total_rev = 0.0

    for d in deals.values():
        if not _deal_in_timeframe(d, today_start):
            continue
        if d.get("status") in {"set", "no_sale"}:
            sets += 1
        if d.get("status") == "sold":
            sold += 1
            kw = float(d.get("kw") or 0)
            total_kw += kw
            rev = _compute_revenue(kw)
            if rev:
                total_rev += rev

    embed = discord.Embed(
        title="📅 Today's Performance",
        color=discord.Color.green(),
        timestamp=utc_now(),
    )
    embed.add_field(name="📞 Appointments Set", value=str(sets), inline=True)
    embed.add_field(name="✅ Deals Closed", value=str(sold), inline=True)
    embed.add_field(name="⚡ Total kW", value=f"{total_kw:.1f}", inline=True)
    if config.get("revenue_enabled"):
        embed.add_field(name="💰 Est. Revenue", value=f"${total_rev:,.2f}", inline=True)

    await ctx.send(embed=embed)


@bot.command(name="pendingdeals", help="Show all appointments that haven't been closed or no-saled yet")
async def pending_deals(ctx: commands.Context):
    pending = [d for d in deals.values() if d.get("status") == "set"]
    if not pending:
        await ctx.send("✅ No pending appointments!")
        return

    embed = discord.Embed(
        title="🔔 Pending Appointments",
        description=f"{len(pending)} appointment(s) waiting to be closed",
        color=discord.Color.orange(),
        timestamp=utc_now(),
    )

    for d in sorted(pending, key=lambda x: x.get("created_at", ""))[:10]:
        created = d.get("created_at")
        try:
            created_str = datetime.fromisoformat(created).strftime("%m/%d %H:%M")
        except Exception:
            created_str = created or "N/A"
        embed.add_field(
            name=f"{d.get('name', 'Unknown')}",
            value=f"Setter: {d.get('setter', 'Unknown')}\nCreated: {created_str}",
            inline=True,
        )

    if len(pending) > 10:
        embed.set_footer(text=f"Showing 10 of {len(pending)} pending deals")

    await ctx.send(embed=embed)


@bot.command(name="mystats", help="View your personal stats and loss breakdown")
async def my_stats(ctx: commands.Context):
    member = ctx.author
    setter_id = member.id
    closer_id = member.id

    appts_set = 0
    deals_sold = 0
    no_sales = 0
    canceled_after_sign = 0
    kw_total = 0.0
    revenue_total = 0.0
    loss_counts: Dict[str, int] = {}

    for d in deals.values():
        if d.get("setter_id") == setter_id:
            if d.get("status") in {"set", "no_sale", "sold", "canceled_after_sign"}:
                appts_set += 1

        if d.get("closer_id") == closer_id:
            if d.get("status") == "sold":
                deals_sold += 1
                kw = float(d.get("kw") or 0.0)
                kw_total += kw
                rev = _compute_revenue(kw)
                if rev:
                    revenue_total += rev
            elif d.get("status") == "no_sale":
                no_sales += 1
                code = d.get("loss_reason") or "other"
                loss_counts[code] = loss_counts.get(code, 0) + 1
            elif d.get("status") == "canceled_after_sign":
                canceled_after_sign += 1

    close_rate = (deals_sold / appts_set * 100) if appts_set else 0.0

    embed = discord.Embed(
        title=f"📊 Stats for {member.display_name}",
        color=discord.Color.blue(),
        timestamp=utc_now(),
    )

    embed.add_field(name="📞 Appointments Set", value=str(appts_set), inline=True)
    embed.add_field(name="✅ Deals Closed", value=str(deals_sold), inline=True)
    embed.add_field(name="📈 Close Rate", value=f"{close_rate:.1f}%", inline=True)

    embed.add_field(name="🚫 No-sale Deals", value=str(no_sales), inline=True)
    embed.add_field(
        name="❌ Canceled After Sign",
        value=str(canceled_after_sign),
        inline=True,
    )

    embed.add_field(name="⚡ Total kW (Closed)", value=f"{kw_total:.1f}", inline=True)
    if config.get("revenue_enabled"):
        embed.add_field(
            name="💰 Your Est. Revenue",
            value=f"${revenue_total:,.2f}",
            inline=True,
        )

    if loss_counts:
        breakdown_lines = []
        total_losses = sum(loss_counts.values())
        for code, count in loss_counts.items():
            label = LOSS_REASON_LABELS.get(code, code.title())
            pct = (count / total_losses) * 100 if total_losses else 0
            breakdown_lines.append(f"- **{label}** – {count} ({pct:.0f}%)")
        embed.add_field(
            name="🧠 No-sale Breakdown (as closer)",
            value="\n".join(breakdown_lines),
            inline=False,
        )

    await ctx.send(embed=embed)


@bot.command(name="leaderboard", help="Show team leaderboard")
async def leaderboard(ctx: commands.Context, timeframe: str = "all"):
    timeframe = timeframe.lower()
    now_dt = utc_now()
    start: Optional[datetime] = None

    if timeframe == "today":
        start = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif timeframe == "week":
        today_start = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        start = today_start - timedelta(days=today_start.weekday())
    elif timeframe == "month":
        start = now_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    closers: Dict[int, Dict[str, Any]] = {}
    setters: Dict[int, Dict[str, Any]] = {}

    for d in deals.values():
        if not _deal_in_timeframe(d, start):
            continue

        status = d.get("status")
        kw = float(d.get("kw") or 0.0)
        rev = _compute_revenue(kw) or 0.0

        # Closers
        if status == "sold" and d.get("closer_id"):
            cid = d["closer_id"]
            if cid not in closers:
                closers[cid] = {
                    "name": d.get("closer") or "Unknown",
                    "deals": 0,
                    "kw": 0.0,
                    "rev": 0.0,
                }
            closers[cid]["deals"] += 1
            closers[cid]["kw"] += kw
            closers[cid]["rev"] += rev

        # Setters
        if d.get("setter_id"):
            sid = d["setter_id"]
            if sid not in setters:
                setters[sid] = {
                    "name": d.get("setter") or "Unknown",
                    "appts_set": 0,
                    "closed": 0,
                }
            if status in {"set", "no_sale", "sold", "canceled_after_sign"}:
                setters[sid]["appts_set"] += 1
            if status == "sold":
                setters[sid]["closed"] += 1

    embed = discord.Embed(
        title="🏆 Solar Sales Leaderboard",
        description=f"Timeframe: **{timeframe.upper()}**",
        color=discord.Color.gold(),
        timestamp=utc_now(),
    )

    # Closers section
    if closers:
        closers_list = sorted(
            closers.values(),
            key=lambda x: (x["deals"], x["kw"]),
            reverse=True,
        )
        lines = []
        medals = ["🥇", "🥈", "🥉"]
        for i, c in enumerate(closers_list[:5]):
            medal = medals[i] if i < len(medals) else f"{i+1}."
            line = f"{medal} **{c['name']}** – {c['deals']} deal(s), {c['kw']:.1f} kW"
            if config.get("revenue_enabled"):
                line += f", ${c['rev']:,.0f}"
            lines.append(line)
        embed.add_field(name="👔 Top Closers", value="\n".join(lines), inline=False)

    # Setters section
    if setters:
        setters_list = sorted(
            setters.values(),
            key=lambda x: (x["closed"], x["appts_set"]),
            reverse=True,
        )
        lines = []
        medals = ["🥇", "🥈", "🥉"]
        for i, s in enumerate(setters_list[:5]):
            medal = medals[i] if i < len(medals) else f"{i+1}."
            close_rate = (
                s["closed"] / s["appts_set"] * 100 if s["appts_set"] else 0.0
            )
            lines.append(
                f"{medal} **{s['name']}** – {s['closed']} closed / {s['appts_set']} set "
                f"({close_rate:.0f}% close)"
            )
        embed.add_field(name="📞 Top Setters", value="\n".join(lines), inline=False)

    # Overall stats
    total_closed = 0
    total_kw = 0.0
    total_rev = 0.0

    for d in deals.values():
        if not _deal_in_timeframe(d, start):
            continue
        if d.get("status") == "sold":
            total_closed += 1
            kw = float(d.get("kw") or 0.0)
            total_kw += kw
            rev = _compute_revenue(kw)
            if rev:
                total_rev += rev

    embed.add_field(
        name="📊 Company Stats",
        value=(
            f"💼 Closed Deals: **{total_closed}**\n"
            f"⚡ Total kW: **{total_kw:.1f}**"
            + (
                f"\n💰 Est. Revenue: **${total_rev:,.2f}**"
                if config.get("revenue_enabled")
                else ""
            )
        ),
        inline=False,
    )

    embed.set_footer(text="Use: !leaderboard [all|today|week|month]")
    await ctx.send(embed=embed)


# ------------------------
# CSV Export & Revenue Config
# ------------------------


@bot.command(name="export_csv", help="Export deals to CSV (admin/manager only)")
@commands.has_any_role("Admin", "Manager")
async def export_csv(ctx: commands.Context, period: str = "all"):
    period = period.lower()
    now_dt = utc_now()
    start: Optional[datetime] = None

    if period == "today":
        start = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "week":
        today_start = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        start = today_start - timedelta(days=today_start.weekday())
    elif period == "month":
        start = now_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    filename = f"/tmp/deals_{period}_{int(now_dt.timestamp())}.csv"
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "Internal ID",
                "Customer",
                "Setter",
                "Closer",
                "Status",
                "kW",
                "Revenue",
                "Loss Reason Code",
                "Loss Reason Detail",
                "Created At",
                "Closed At",
                "Canceled At",
            ]
        )

        for d in deals.values():
            if not _deal_in_timeframe(d, start):
                continue
            kw = float(d.get("kw") or 0.0)
            rev = _compute_revenue(kw) or 0.0
            writer.writerow(
                [
                    d.get("internal_id"),
                    d.get("name"),
                    d.get("setter"),
                    d.get("closer"),
                    d.get("status"),
                    kw,
                    rev if rev else "",
                    d.get("loss_reason") or "",
                    d.get("loss_reason_detail") or "",
                    d.get("created_at") or "",
                    d.get("closed_at") or "",
                    d.get("canceled_at") or "",
                ]
            )

    await ctx.send(
        f"📁 Exported deals for **{period}**.",
        file=discord.File(filename, filename=os.path.basename(filename)),
    )


@bot.command(name="set_revenue", help="Enable/disable revenue display (admin/manager only)")
@commands.has_any_role("Admin", "Manager")
async def set_revenue(ctx: commands.Context, value: str):
    value = value.lower()
    if value in {"off", "0", "none"}:
        config["revenue_enabled"] = False
        config["revenue_per_kw"] = 0.0
        _save_json(CONFIG_FILE, config)
        await ctx.send("💸 Revenue display has been **disabled** for all future embeds and stats.")
        return

    try:
        kw_value = float(value)
    except ValueError:
        await ctx.send("❌ Usage: `!set_revenue off` or `!set_revenue 400` (meaning $400 per kW).")
        return

    config["revenue_enabled"] = True
    config["revenue_per_kw"] = kw_value
    _save_json(CONFIG_FILE, config)
    await ctx.send(f"💸 Revenue enabled at **${kw_value:.2f} per kW**.")


# ------------------------
# Run Bot
# ------------------------


def main():
    token = os.getenv("DISCORD_BOT_TOKEN")
    if not token:
        print("Error: DISCORD_BOT_TOKEN environment variable not set.")
        return
    bot.run(token)


if __name__ == "__main__":
    main()
