import os
import json
import csv
import asyncio
import urllib.request
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Optional

import discord
from discord.ext import commands

# ------------------------
# Timezone
# ------------------------

LOCAL_TZ = ZoneInfo("America/Chicago")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_local() -> datetime:
    return datetime.now(LOCAL_TZ)


def _utc_iso() -> str:
    return _now_utc().isoformat()


# ------------------------
# Paths / storage
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


def _load_deals():
    if not os.path.exists(DEALS_FILE):
        return {"next_id": 1, "deals": []}
    try:
        with open(DEALS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "next_id" not in data:
            data["next_id"] = 1
        if "deals" not in data:
            data["deals"] = []
        return data
    except Exception:
        return {"next_id": 1, "deals": []}


def _save_deals(data):
    tmp = DEALS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, DEALS_FILE)


def _load_config():
    if not os.path.exists(CONFIG_FILE):
        return {
            "revenue_enabled": False,
            "revenue_per_kw": 0.0,
            "ghl_enabled": False,
            "ghl_webhook": None,
        }
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {
            "revenue_enabled": False,
            "revenue_per_kw": 0.0,
            "ghl_enabled": False,
            "ghl_webhook": None,
        }


def _save_config(data):
    tmp = CONFIG_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, CONFIG_FILE)


DEALS_DATA = _load_deals()
CONFIG_DATA = _load_config()

# ------------------------
# Discord bot setup
# ------------------------

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

LEADERBOARD_CHANNELS = {
    "daily-leaderboard": "Daily sales leaderboard (read-only)",
    "weekly-leaderboard": "Weekly sales leaderboard (read-only)",
    "monthly-leaderboard": "Monthly sales leaderboard (read-only)",
}

# ------------------------
# Helpers
# ------------------------


def _deal_type(kw: float) -> str:
    return "battery_only" if kw == 0.0 else "standard"


def _deal_type_label(dtype: str) -> str:
    if dtype == "battery_only":
        return "Battery Only 🔋"
    return "Standard ⚡"


def _parse_date(date_str: str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        return None


def _get_guild_deals(guild_id: int):
    return [d for d in DEALS_DATA["deals"] if d.get("guild_id") == guild_id]


def _display_name(user_id: int | None, stored_name: str, use_mention: bool = False) -> str:
    """
    Return display string for a user.
    - use_mention=True AND user_id exists -> <@user_id> (clickable mention)
    - Otherwise -> just the plain display name (no ping)
    """
    if use_mention and user_id:
        return f"<@{user_id}>"
    return stored_name or "Unknown"


def _compute_revenue(kw: Optional[float]) -> Optional[float]:
    """Calculate revenue based on kW if enabled."""
    if not kw:
        return None
    if not CONFIG_DATA.get("revenue_enabled"):
        return None
    per_kw = float(CONFIG_DATA.get("revenue_per_kw") or 0.0)
    if per_kw <= 0:
        return None
    return kw * per_kw


def _compute_closer_streak(guild_id: int, closer_id: int) -> int:
    """Consecutive days (including today) this closer has at least one sold deal."""
    dates = set()
    for d in _get_guild_deals(guild_id):
        if d.get("status") == "sold" and d.get("closer_id") == closer_id:
            closed_at = d.get("closed_at") or d.get("created_at")
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
    current_day = _now_utc().date()
    while current_day in dates:
        streak += 1
        current_day = current_day - timedelta(days=1)
    return streak


async def _send_ghl_event(event: str, payload: Dict[str, Any]) -> None:
    """Optional GHL webhook (uses stdlib only)."""
    if not CONFIG_DATA.get("ghl_enabled") or not CONFIG_DATA.get("ghl_webhook"):
        return
    try:
        body = json.dumps({"event": event, **payload}).encode("utf-8")
        req = urllib.request.Request(
            CONFIG_DATA["ghl_webhook"],
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        print(f"GHL webhook error: {e}")


def _add_deal(
    guild_id: int,
    setter_id: int | None,
    setter_name: str | None,
    closer_id: int | None,
    closer_name: str | None,
    customer_name: str,
    kw: float | None,
    status: str = "sold",
):
    deal_id = DEALS_DATA.get("next_id", 1)
    DEALS_DATA["next_id"] = deal_id + 1

    deal = {
        "id": deal_id,
        "guild_id": guild_id,
        "setter_id": setter_id,
        "setter_name": setter_name,
        "closer_id": closer_id,
        "closer_name": closer_name,
        "customer_name": customer_name,
        "kw": float(kw) if kw is not None else None,
        "deal_type": _deal_type(float(kw)) if kw is not None else None,
        "status": status,
        "loss_reason": None,
        "loss_reason_detail": None,
        "created_at": _now_utc().isoformat(),
        "closed_at": _now_utc().isoformat() if status == "sold" else None,
        "no_sale_at": None,
        "canceled_at": None,
    }
    DEALS_DATA["deals"].append(deal)
    _save_deals(DEALS_DATA)
    return deal


def _find_deal_by_id(guild_id: int, deal_id: int):
    for d in _get_guild_deals(guild_id):
        if d.get("id") == deal_id:
            return d
    return None


def _find_latest_deal_by_customer(guild_id: int, customer_name: str, preferred_statuses: Optional[List[str]] = None):
    customer_lower = customer_name.strip().lower()
    candidates = []
    for d in _get_guild_deals(guild_id):
        if d.get("customer_name", "").strip().lower() == customer_lower:
            if preferred_statuses is None or d.get("status") in preferred_statuses:
                candidates.append(d)
    if not candidates:
        return None
    candidates.sort(key=lambda d: d.get("created_at") or "", reverse=True)
    return candidates[0]


def _filter_deals_period(
    guild_id: int,
    start_utc: datetime,
    end_utc: datetime,
    include_canceled: bool = False,
    status_filter: Optional[List[str]] = None,
):
    deals = _get_guild_deals(guild_id)
    result = []
    for d in deals:
        status = d.get("status", "sold")
        if status == "deleted":
            continue
        if not include_canceled and status in ("canceled", "canceled_after_sign"):
            continue
        if status_filter and status not in status_filter:
            continue
        # Use closed_at for sold deals, created_at for others
        ts = d.get("closed_at") or d.get("created_at")
        if not ts:
            continue
        try:
            created = datetime.fromisoformat(ts)
        except Exception:
            continue
        if start_utc <= created < end_utc:
            result.append(d)
    return result


def _get_user_deals(guild_id: int, user_id: int, user_name: str):
    """
    Get all deals where user is the closer OR the setter.
    Matches by ID first, then falls back to name matching for setters logged without @mention.
    """
    deals = []
    user_name_lower = user_name.lower().strip()
    
    for d in _get_guild_deals(guild_id):
        if d.get("status") in ("deleted",):
            continue
        
        # Check if user is the closer (by ID)
        if d.get("closer_id") == user_id:
            deals.append(d)
            continue
        
        # Check if user is the setter (by ID)
        if d.get("setter_id") == user_id:
            deals.append(d)
            continue
        
        # Fallback: check setter by name (for deals logged without @mention)
        setter_name = d.get("setter_name", "")
        if setter_name and setter_name.lower().strip() == user_name_lower:
            deals.append(d)
            continue
    
    return deals


def _get_user_deals_period(guild_id: int, user_id: int, user_name: str, start_utc, end_utc):
    """Get user's deals within a specific time period."""
    all_deals = _get_user_deals(guild_id, user_id, user_name)
    result = []
    for d in all_deals:
        ts = d.get("closed_at") or d.get("created_at")
        if not ts:
            continue
        try:
            created = datetime.fromisoformat(ts)
        except Exception:
            continue
        if start_utc <= created < end_utc:
            result.append(d)
    return result


def _aggregate_by_role(deals: list[dict], role: str):
    """
    Aggregate deals by closer or setter.
    role = 'closer' or 'setter'
    Returns list of {id, name, deals, kw} sorted by deals desc.
    """
    stats: dict[str, dict] = {}
    for d in deals:
        # Only count sold deals for aggregation
        if d.get("status") != "sold":
            continue
        uid = d.get(f"{role}_id")
        name = (d.get(f"{role}_name") or "").strip()
        if not name:
            continue
        # Use ID as key if available, else lowercase name
        key = str(uid) if uid else name.lower()
        if key not in stats:
            stats[key] = {
                "id": uid,
                "name": name,
                "deals": 0,
                "kw": 0.0,
            }
        stats[key]["deals"] += 1
        stats[key]["kw"] += float(d.get("kw") or 0.0)
    out = list(stats.values())
    out.sort(key=lambda x: (x["deals"], x["kw"]), reverse=True)
    return out


def _split_by_type(deals: list[dict]):
    """Split deals into standard and battery_only lists."""
    standard = []
    battery = []
    for d in deals:
        if d.get("status") != "sold":
            continue
        dtype = d.get("deal_type")
        if dtype is None:
            kw = d.get("kw")
            dtype = _deal_type(float(kw)) if kw is not None else "standard"
        if dtype == "battery_only":
            battery.append(d)
        else:
            standard.append(d)
    return standard, battery


def _period_bounds(kind: str, base_dt: datetime):
    kind = kind.lower()
    base_local = base_dt.astimezone(LOCAL_TZ)
    d = base_local.date()

    if kind in ("day", "today"):
        start_local = datetime(d.year, d.month, d.day, tzinfo=LOCAL_TZ)
        end_local = start_local + timedelta(days=1)
        pretty_kind = "Daily Blitz Scoreboard"
    elif kind in ("week", "thisweek"):
        monday = d - timedelta(days=d.weekday())
        start_local = datetime(monday.year, monday.month, monday.day, tzinfo=LOCAL_TZ)
        end_local = start_local + timedelta(days=7)
        pretty_kind = "Weekly Blitz Scoreboard"
    elif kind in ("month", "thismonth"):
        start_local = datetime(d.year, d.month, 1, tzinfo=LOCAL_TZ)
        if d.month == 12:
            end_local = datetime(d.year + 1, 1, 1, tzinfo=LOCAL_TZ)
        else:
            end_local = datetime(d.year, d.month + 1, 1, tzinfo=LOCAL_TZ)
        pretty_kind = "Monthly Blitz Scoreboard"
    else:
        start_local = datetime(d.year, d.month, d.day, tzinfo=LOCAL_TZ)
        end_local = start_local + timedelta(days=1)
        pretty_kind = "Blitz Scoreboard"

    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)
    return start_utc, end_utc, start_local, end_local, pretty_kind


# ---------------------------------------------------------------
# Build scoreboard  (plain-text for leaderboard channels - NO MENTIONS)
# ---------------------------------------------------------------

def _build_section_lines(deals: list[dict], role: str, show_kw: bool = True) -> list[str]:
    """
    Build 'Closer:' or 'Setter:' lines for a list of deals.
    NO @mentions - just plain names.
    Shows kW next to each person.
    """
    agg = _aggregate_by_role(deals, role)
    if not agg:
        return []
    lines = []
    label = "Closer :" if role == "closer" else "Setter :"
    lines.append(label)
    lines.append("")
    for row in agg:
        # Use plain name, NOT mention
        name = row["name"]
        if show_kw:
            lines.append(f"  {name} - {row['deals']} ({row['kw']:.1f} kW)")
        else:
            lines.append(f"  {name} - {row['deals']}")
    return lines


def _build_leaderboard_content(
    deals: list[dict],
    period_label: str,
    date_label: str,
) -> str:
    """
    Build a plain-text scoreboard for leaderboard channels.
    NO @mentions - just plain display names.
    Shows kW next to each person.
    """
    # Filter to only sold deals
    sold_deals = [d for d in deals if d.get("status") == "sold"]
    standard_deals, battery_deals = _split_by_type(sold_deals)

    lines = []
    lines.append(f"{period_label} ⚡")
    lines.append("")

    if not sold_deals:
        lines.append("_No deals yet — be the first to log a sale with `#sold`!_")
        return "\n".join(lines)

    # --- Standard section ---
    if standard_deals:
        lines.append("Standard ⚡")
        lines.append("")

        closer_lines = _build_section_lines(standard_deals, "closer", show_kw=True)
        if closer_lines:
            lines.extend(closer_lines)
            lines.append("")

        setter_lines = _build_section_lines(standard_deals, "setter", show_kw=True)
        if setter_lines:
            lines.extend(setter_lines)
            lines.append("")

    # --- Battery Only section ---
    if battery_deals:
        lines.append("Battery Only 🔋")
        lines.append("")

        closer_lines = _build_section_lines(battery_deals, "closer", show_kw=True)
        if closer_lines:
            lines.extend(closer_lines)
            lines.append("")

        setter_lines = _build_section_lines(battery_deals, "setter", show_kw=True)
        if setter_lines:
            lines.extend(setter_lines)
            lines.append("")

    # --- Totals ---
    total_deals = len(sold_deals)
    total_kw = sum(float(d.get("kw") or 0.0) for d in sold_deals)

    lines.append(f"**Total Transactions Sold:** {total_deals}")
    lines.append(f"**Total kW Sold:** {total_kw:.2f} kW")
    
    # Revenue if enabled
    if CONFIG_DATA.get("revenue_enabled"):
        total_rev = sum(_compute_revenue(float(d.get("kw") or 0.0)) or 0.0 for d in sold_deals)
        lines.append(f"**Est. Revenue:** ${total_rev:,.2f}")
    
    lines.append("")
    lines.append(
        f"_Commands: type `#sold @Setter kW` in your general chat. "
        f"Use `!mystats` to see your own numbers._"
    )

    return "\n".join(lines)


def _build_leaderboard_embed(
    guild: discord.Guild,
    deals: list[dict],
    period_label: str,
    date_label: str,
    use_mentions: bool = True,
):
    """
    Embed version used by the !leaderboard command.
    use_mentions=True for admin command, False otherwise.
    """
    embed = discord.Embed(
        title=f"🏆 {period_label}",
        description=date_label,
        color=0xf1c40f,
    )

    # Filter to only sold deals
    sold_deals = [d for d in deals if d.get("status") == "sold"]

    if not sold_deals:
        embed.add_field(
            name="No deals yet",
            value="Be the first to log a sale with `#sold`!",
            inline=False,
        )
        return embed

    standard_deals, battery_deals = _split_by_type(sold_deals)
    medals = ["🥇", "🥈", "🥉"]

    def _role_lines(deal_list, role):
        agg = _aggregate_by_role(deal_list, role)
        out = []
        for idx, row in enumerate(agg[:10]):
            icon = medals[idx] if idx < len(medals) else f"{idx+1}."
            display = _display_name(row["id"], row["name"], use_mention=use_mentions)
            line = f"{icon} {display} – {row['deals']} deal(s), {row['kw']:.1f} kW"
            if CONFIG_DATA.get("revenue_enabled"):
                rev = _compute_revenue(row["kw"]) or 0
                line += f", ${rev:,.0f}"
            out.append(line)
        return "\n".join(out)

    if standard_deals:
        cl = _role_lines(standard_deals, "closer")
        if cl:
            embed.add_field(name="⚡ Standard — Closers", value=cl, inline=False)
        sl = _role_lines(standard_deals, "setter")
        if sl:
            embed.add_field(name="⚡ Standard — Setters", value=sl, inline=False)

    if battery_deals:
        cl = _role_lines(battery_deals, "closer")
        if cl:
            embed.add_field(name="🔋 Battery Only — Closers", value=cl, inline=False)
        sl = _role_lines(battery_deals, "setter")
        if sl:
            embed.add_field(name="🔋 Battery Only — Setters", value=sl, inline=False)

    total_deals = len(sold_deals)
    total_kw = sum(float(d.get("kw") or 0.0) for d in sold_deals)
    
    totals_value = (
        f"💼 **Deals:** {total_deals}\n"
        f"⚡ **kW:** {total_kw:.1f}\n"
        f"Standard: {len(standard_deals)}  •  Battery Only: {len(battery_deals)}"
    )
    
    if CONFIG_DATA.get("revenue_enabled"):
        total_rev = sum(_compute_revenue(float(d.get("kw") or 0.0)) or 0.0 for d in sold_deals)
        totals_value += f"\n💰 **Est. Revenue:** ${total_rev:,.2f}"
    
    embed.add_field(name="Totals", value=totals_value, inline=False)
    embed.set_footer(text="Use !leaderboard [day|week|month] [YYYY-MM-DD] for history")
    return embed


# ---------------------------------------------------------------
# Channel management
# ---------------------------------------------------------------

async def ensure_leaderboard_channels(guild: discord.Guild):
    try:
        bot_member = guild.me
        if bot_member is None:
            return
        everyone = guild.default_role

        overwrites = {
            everyone: discord.PermissionOverwrite(
                view_channel=True,
                read_message_history=True,
                send_messages=False,
                add_reactions=False,
                create_public_threads=False,
                create_private_threads=False,
                create_forum_threads=False,
                send_messages_in_threads=False,
            ),
            bot_member: discord.PermissionOverwrite(
                view_channel=True,
                read_message_history=True,
                send_messages=True,
                embed_links=True,
                manage_messages=True,
                send_messages_in_threads=True,
            ),
        }

        for name, topic in LEADERBOARD_CHANNELS.items():
            chan = discord.utils.get(guild.text_channels, name=name)
            if chan is None:
                await guild.create_text_channel(name, topic=topic, overwrites=overwrites)
            else:
                await chan.edit(topic=topic, overwrites=overwrites)
    except discord.Forbidden:
        return
    except Exception as e:
        print(f"[ensure_leaderboard_channels] error in guild {guild.id}: {e}")


async def _post_today_leaderboards(guild: discord.Guild):
    """
    Post fresh scoreboards to all three leaderboard channels.
    NO @mentions - just plain text with names and kW.
    """
    now_local = _now_local()

    start_day_utc, end_day_utc, start_day_local, _, _ = _period_bounds("day", now_local)
    deals_day = _filter_deals_period(guild.id, start_day_utc, end_day_utc)

    start_week_utc, end_week_utc, start_week_local, end_week_local, _ = _period_bounds("week", now_local)
    deals_week = _filter_deals_period(guild.id, start_week_utc, end_week_utc)

    start_month_utc, end_month_utc, start_month_local, _, _ = _period_bounds("month", now_local)
    deals_month = _filter_deals_period(guild.id, start_month_utc, end_month_utc)

    channel_map = {}
    for name in LEADERBOARD_CHANNELS:
        chan = discord.utils.get(guild.text_channels, name=name)
        if chan:
            channel_map[name] = chan

    if "daily-leaderboard" in channel_map:
        content = _build_leaderboard_content(
            deals_day,
            "Daily Blitz Scoreboard",
            start_day_local.date().isoformat(),
        )
        await channel_map["daily-leaderboard"].send(content)

    if "weekly-leaderboard" in channel_map:
        week_label = (
            f"{start_week_local.date().isoformat()} → "
            f"{(end_week_local - timedelta(days=1)).date().isoformat()}"
        )
        content = _build_leaderboard_content(
            deals_week,
            "Weekly Blitz Scoreboard",
            week_label,
        )
        await channel_map["weekly-leaderboard"].send(content)

    if "monthly-leaderboard" in channel_map:
        content = _build_leaderboard_content(
            deals_month,
            "Monthly Blitz Scoreboard",
            start_month_local.date().strftime("%Y-%m"),
        )
        await channel_map["monthly-leaderboard"].send(content)


# ---------------------------------------------------------------
# Permission check helper
# ---------------------------------------------------------------

def _is_admin_or_manager(member: discord.Member) -> bool:
    if member.guild_permissions.administrator:
        return True
    return any(r.name.lower() in {"admin", "manager"} for r in getattr(member, "roles", []))


# ---------------------------------------------------------------
# Events
# ---------------------------------------------------------------


@bot.event
async def on_ready():
    print(f"{bot.user} has connected to Discord!")
    print(f"Guilds: {[g.name for g in bot.guilds]}")
    for guild in bot.guilds:
        await ensure_leaderboard_channels(guild)


@bot.event
async def on_guild_join(guild: discord.Guild):
    await ensure_leaderboard_channels(guild)


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    if (
        isinstance(message.channel, discord.TextChannel)
        and message.channel.name in LEADERBOARD_CHANNELS
    ):
        await bot.process_commands(message)
        return

    # Need a guild for # commands
    if not message.guild:
        await bot.process_commands(message)
        return

    content = message.content.strip()
    lower = content.lower()

    # ----------------------------------------------------------------
    # #set Customer Name - Log an appointment (setter)
    # ----------------------------------------------------------------
    if lower.startswith("#set "):
        customer_name = content[5:].strip()
        if not customer_name:
            await message.channel.send("❌ Please include the customer's name. Example: `#set John Smith`")
            await bot.process_commands(message)
            return

        deal = _add_deal(
            guild_id=message.guild.id,
            setter_id=message.author.id,
            setter_name=message.author.display_name,
            closer_id=None,
            closer_name=None,
            customer_name=customer_name,
            kw=None,
            status="set",
        )

        embed = discord.Embed(
            title="🎯 Appointment Set!",
            description=f"{message.author.mention} just set an appointment!",
            color=discord.Color.green(),
            timestamp=_now_utc(),
        )
        embed.add_field(name="Customer", value=customer_name, inline=True)
        embed.add_field(name="Setter", value=message.author.display_name, inline=True)
        embed.add_field(name="Status", value="🟡 Pending Close", inline=True)
        embed.add_field(name="Deal ID", value=f"#{deal['id']}", inline=True)
        embed.add_field(
            name="How to close this later",
            value="Use: `#sold Customer Name kW`\nExample: `#sold John Smith 8.5`",
            inline=False,
        )
        embed.set_footer(text="Deal Tracker • Track your deals in real-time")

        await message.channel.send(embed=embed)
        return

    # ----------------------------------------------------------------
    # #sold @Setter kW  OR  #sold Customer Name kW
    # ----------------------------------------------------------------
    if lower.startswith("#sold") and not lower.startswith("#soldfor"):
        try:
            parts = content.split()
            if len(parts) < 3:
                raise ValueError("Not enough parts")

            setter_member = message.mentions[0] if message.mentions else None
            setter_name = None
            setter_id = None
            kw = None
            customer_name = None

            if setter_member:
                # Format: #sold @Setter [Customer Name] kW
                mention_token = None
                for p in parts:
                    if p.startswith("<@") and p.endswith(">"):
                        mention_token = p
                        break
                if mention_token is None:
                    raise ValueError("No mention found")
                idx = parts.index(mention_token)
                if len(parts) - idx < 2:
                    raise ValueError("Missing kW")
                kw_token = parts[-1]
                kw = float(kw_token)
                customer_tokens = parts[idx + 1 : -1]
                customer_name = " ".join(customer_tokens) if customer_tokens else None
                setter_id = setter_member.id
                setter_name = setter_member.display_name
            else:
                # Format: #sold Customer Name kW (check if there's a pending deal)
                kw_token = parts[-1]
                kw = float(kw_token)
                customer_name = " ".join(parts[1:-1]).strip()
                
                # Try to find existing deal for this customer
                existing_deal = _find_latest_deal_by_customer(
                    message.guild.id, 
                    customer_name, 
                    preferred_statuses=["set"]
                )
                
                if existing_deal:
                    # Update existing deal
                    existing_deal["status"] = "sold"
                    existing_deal["closer"] = message.author.display_name
                    existing_deal["closer_id"] = message.author.id
                    existing_deal["closer_name"] = message.author.display_name
                    existing_deal["kw"] = kw
                    existing_deal["deal_type"] = _deal_type(kw)
                    existing_deal["closed_at"] = _now_utc().isoformat()
                    _save_deals(DEALS_DATA)
                    
                    setter_id = existing_deal.get("setter_id")
                    setter_name = existing_deal.get("setter_name")
                    
                    revenue = _compute_revenue(kw)
                    streak_days = _compute_closer_streak(message.guild.id, message.author.id)
                    
                    # Send GHL event
                    await _send_ghl_event("deal_sold", {
                        "customer_name": customer_name,
                        "kw": kw,
                        "revenue": revenue,
                        "setter": setter_name,
                        "closer": message.author.display_name,
                        "deal_id": existing_deal["id"],
                    })
                    
                    embed = discord.Embed(
                        title="🎉 DEAL CLOSED!",
                        description=f"Deal for **{customer_name}** has been closed!",
                        color=discord.Color.gold(),
                        timestamp=_now_utc(),
                    )
                    embed.add_field(name="⚡ System Size", value=f"{kw:.1f} kW", inline=True)
                    if revenue:
                        embed.add_field(name="💰 Est. Revenue", value=f"${revenue:,.2f}", inline=True)
                    embed.add_field(name="👤 Setter", value=setter_name or "N/A", inline=True)
                    embed.add_field(name="🤝 Closer", value=message.author.display_name, inline=True)
                    embed.add_field(name="Deal ID", value=f"#{existing_deal['id']}", inline=True)
                    if streak_days > 0:
                        embed.add_field(name="🔥 Streak", value=f"{streak_days} day(s)", inline=True)
                    
                    await message.channel.send(embed=embed)
                    await _post_today_leaderboards(message.guild)
                    return
                else:
                    # No existing deal - treat first word after #sold as setter name
                    setter_name = parts[1]
                    customer_tokens = parts[2:-1]
                    customer_name = " ".join(customer_tokens) if customer_tokens else "N/A"

            closer_member = message.author
            closer_name = closer_member.display_name

            deal = _add_deal(
                guild_id=message.guild.id,
                setter_id=setter_id,
                setter_name=setter_name,
                closer_id=closer_member.id,
                closer_name=closer_name,
                customer_name=customer_name or "N/A",
                kw=kw,
                status="sold",
            )

            revenue = _compute_revenue(kw)
            streak_days = _compute_closer_streak(message.guild.id, closer_member.id)
            dtype_label = _deal_type_label(deal["deal_type"])

            # Send GHL event
            await _send_ghl_event("deal_sold", {
                "customer_name": deal["customer_name"],
                "kw": kw,
                "revenue": revenue,
                "setter": setter_name,
                "closer": closer_name,
                "deal_id": deal["id"],
            })

            # Deal confirmation DOES use @mentions
            embed = discord.Embed(
                title="🎉 DEAL CLOSED!",
                color=0x2ecc71,
                description=(
                    f"Deal for {_display_name(setter_id, setter_name, use_mention=True)} has been logged!"
                ),
            )
            embed.add_field(
                name="💼 Closer",
                value=_display_name(closer_member.id, closer_name, use_mention=True),
                inline=True,
            )
            embed.add_field(
                name="Setter",
                value=_display_name(setter_id, setter_name, use_mention=True),
                inline=True,
            )
            embed.add_field(name="⚡ System Size", value=f"{deal['kw']:.1f} kW", inline=True)
            embed.add_field(name="Type", value=dtype_label, inline=True)
            if revenue:
                embed.add_field(name="💰 Est. Revenue", value=f"${revenue:,.2f}", inline=True)
            if customer_name and customer_name != "N/A":
                embed.add_field(name="Customer", value=deal["customer_name"], inline=True)
            if streak_days > 0:
                embed.add_field(name="🔥 Streak", value=f"{streak_days} day(s) in a row", inline=True)
            embed.set_footer(text=f"Deal #{deal['id']}")

            await message.channel.send(embed=embed)
            await _post_today_leaderboards(message.guild)

        except ValueError:
            await message.channel.send(
                "❌ Invalid `#sold` format.\n"
                "Use: `#sold @Setter kW` or `#sold Customer Name kW`\n"
                "Example: `#sold @Devin 6.5` or `#sold John Smith 6.5`"
            )
        except Exception as e:
            await message.channel.send(f"❌ Error processing sale: {e}")
        return

    # ----------------------------------------------------------------
    # #soldfor @Closer @Setter kW   (admin only)
    # ----------------------------------------------------------------
    if lower.startswith("#soldfor"):
        if not _is_admin_or_manager(message.author):
            await message.channel.send("⛔ Only admins or managers can use `#soldfor`.")
            return

        try:
            parts = content.split()
            if len(parts) < 4:
                raise ValueError("Not enough parts")

            mentions = message.mentions
            if len(mentions) < 2:
                raise ValueError("Need two @mentions: closer and setter")

            mention_tokens = [p for p in parts if p.startswith("<@") and p.endswith(">")]
            if len(mention_tokens) < 2:
                raise ValueError("Need two mentions")

            closer_member = mentions[0]
            setter_member = mentions[1]

            second_mention_idx = parts.index(mention_tokens[1])

            kw_token = parts[-1]
            kw = float(kw_token)

            customer_tokens = parts[second_mention_idx + 1 : -1]
            customer_name = " ".join(customer_tokens) if customer_tokens else None

            deal = _add_deal(
                guild_id=message.guild.id,
                setter_id=setter_member.id,
                setter_name=setter_member.display_name,
                closer_id=closer_member.id,
                closer_name=closer_member.display_name,
                customer_name=customer_name or "N/A",
                kw=kw,
                status="sold",
            )

            revenue = _compute_revenue(kw)
            dtype_label = _deal_type_label(deal["deal_type"])

            embed = discord.Embed(
                title="🎉 DEAL CLOSED! (logged by admin)",
                color=0x2ecc71,
                description=(
                    f"Deal logged by {message.author.display_name} "
                    f"for {_display_name(closer_member.id, closer_member.display_name, use_mention=True)}"
                ),
            )
            embed.add_field(
                name="💼 Closer",
                value=_display_name(closer_member.id, closer_member.display_name, use_mention=True),
                inline=True,
            )
            embed.add_field(
                name="Setter",
                value=_display_name(setter_member.id, setter_member.display_name, use_mention=True),
                inline=True,
            )
            embed.add_field(name="⚡ System Size", value=f"{deal['kw']:.1f} kW", inline=True)
            embed.add_field(name="Type", value=dtype_label, inline=True)
            if revenue:
                embed.add_field(name="💰 Est. Revenue", value=f"${revenue:,.2f}", inline=True)
            if customer_name and customer_name != "N/A":
                embed.add_field(name="Customer", value=deal["customer_name"], inline=True)
            embed.set_footer(text=f"Deal #{deal['id']}")

            await message.channel.send(embed=embed)
            await _post_today_leaderboards(message.guild)

        except ValueError:
            await message.channel.send(
                "❌ Invalid `#soldfor` format.\n"
                "Use: `#soldfor @Closer @Setter kW`\n"
                "Example: `#soldfor @Ethen @Devin 6.5`"
            )
        except Exception as e:
            await message.channel.send(f"❌ Error processing sale: {e}")
        return

    # ----------------------------------------------------------------
    # #nosale Customer Name - Mark a deal as no-sale with reason tracking
    # ----------------------------------------------------------------
    if lower.startswith("#nosale "):
        customer_name = content[8:].strip()
        if not customer_name:
            await message.channel.send("❌ Please include the customer's name. Example: `#nosale John Smith`")
            await bot.process_commands(message)
            return

        deal = _find_latest_deal_by_customer(message.guild.id, customer_name, preferred_statuses=["set"])
        if not deal:
            await message.channel.send(
                f"❌ No pending appointment found for **{customer_name}**. "
                "Make sure it was logged with `#set` first."
            )
            await bot.process_commands(message)
            return

        deal["status"] = "no_sale"
        deal["no_sale_at"] = _now_utc().isoformat()
        deal["closer_id"] = message.author.id
        deal["closer_name"] = message.author.display_name
        _save_deals(DEALS_DATA)

        # DM for loss reason
        try:
            prompt = (
                f"Why did **{deal['customer_name']}** not close?\n"
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
            _save_deals(DEALS_DATA)

            await message.channel.send(f"🚫 **{deal['customer_name']}** marked as no-sale ({reason_text}).")
        except asyncio.TimeoutError:
            await message.channel.send(
                f"⏱️ No loss reason received for **{deal['customer_name']}**. Marked as no-sale."
            )
        except discord.Forbidden:
            await message.channel.send(
                f"🚫 **{deal['customer_name']}** marked as no-sale. "
                "(Couldn't DM you for the loss reason - please enable DMs from server members.)"
            )
        return

    # ----------------------------------------------------------------
    # #cancel Customer Name - Mark deal as canceled
    # ----------------------------------------------------------------
    if lower.startswith("#cancel "):
        customer_name = content[8:].strip()
        if not customer_name:
            await message.channel.send("❌ Please include the customer's name. Example: `#cancel John Smith`")
            await bot.process_commands(message)
            return

        deal = _find_latest_deal_by_customer(message.guild.id, customer_name)
        if not deal:
            await message.channel.send(f"❌ No deal found for customer `{customer_name}`.")
            return

        if deal.get("status") in ("canceled", "canceled_after_sign"):
            await message.channel.send(f"ℹ️ Deal for `{customer_name}` is already canceled.")
            return

        old_status = deal.get("status")
        deal["status"] = "canceled_after_sign" if old_status == "sold" else "canceled"
        deal["canceled_at"] = _now_utc().isoformat()
        _save_deals(DEALS_DATA)

        embed = discord.Embed(
            title="⚠️ Deal Canceled",
            color=0xe67e22,
            description=f"Customer: **{deal['customer_name']}**",
        )
        embed.add_field(
            name="Closer",
            value=_display_name(deal.get("closer_id"), deal.get("closer_name", "Unknown")),
            inline=True,
        )
        if deal.get("setter_name"):
            embed.add_field(
                name="Setter",
                value=_display_name(deal.get("setter_id"), deal["setter_name"]),
                inline=True,
            )
        if deal.get("kw"):
            embed.add_field(name="System Size", value=f"{deal['kw']:.1f} kW", inline=True)
        embed.set_footer(text=f"Deal #{deal['id']}")
        await message.channel.send(embed=embed)
        await _post_today_leaderboards(message.guild)
        return

    # ----------------------------------------------------------------
    # #delete <ID> or #delete Customer Name   (admin/manager only)
    # ----------------------------------------------------------------
    if lower.startswith("#delete"):
        if not _is_admin_or_manager(message.author):
            await message.channel.send("⛔ Only admins or managers can delete deals.")
            return

        try:
            parts = content.split(maxsplit=1)
            if len(parts) < 2:
                raise ValueError("Missing target")
            target = parts[1].strip()

            deal = None
            try:
                deal_id = int(target)
                deal = _find_deal_by_id(message.guild.id, deal_id)
                if not deal:
                    await message.channel.send(f"❌ No deal found with ID `{deal_id}`.")
                    return
            except (ValueError, TypeError):
                deal = _find_latest_deal_by_customer(message.guild.id, target)
                if not deal:
                    await message.channel.send(f"❌ No deal found for `{target}`.")
                    return

            deal_info = (
                f"Deal #{deal['id']} — "
                f"Closer: {deal.get('closer_name', '?')}, "
                f"Setter: {deal.get('setter_name', '?')}, "
                f"{deal.get('kw', 0):.1f} kW"
            )

            DEALS_DATA["deals"] = [d for d in DEALS_DATA["deals"] if d["id"] != deal["id"]]
            _save_deals(DEALS_DATA)

            await message.channel.send(f"🗑️ Deleted: {deal_info}")
            await _post_today_leaderboards(message.guild)

        except ValueError:
            await message.channel.send("❌ Use: `#delete <DealID>` or `#delete Customer Name`")
        except Exception as e:
            await message.channel.send(f"❌ Error: {e}")
        return

    # ----------------------------------------------------------------
    # #clearleaderboard   (admin/manager only)
    # ----------------------------------------------------------------
    if lower.startswith("#clearleaderboard"):
        if not _is_admin_or_manager(message.author):
            await message.channel.send("⛔ Only admins or managers can clear the leaderboard.")
            return

        DEALS_DATA["deals"] = [d for d in DEALS_DATA["deals"] if d.get("guild_id") != message.guild.id]
        _save_deals(DEALS_DATA)
        await message.channel.send("🔥 All deals for this server have been cleared. Fresh start!")
        await _post_today_leaderboards(message.guild)
        return

    await bot.process_commands(message)


# ---------------------------------------------------------------
# ! Commands
# ---------------------------------------------------------------


@bot.command(name="deals")
async def deals_cmd(ctx: commands.Context, period: str = "day", date_str: str | None = None):
    """!deals [day|week|month|all] - List all deals with their IDs."""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return

    period = period.lower()
    if period not in {"day", "week", "month", "today", "thisweek", "thismonth", "all"}:
        await ctx.send("❌ Use: `!deals [day|week|month|all]`")
        return

    if period == "all":
        guild_deals = [
            d for d in _get_guild_deals(ctx.guild.id)
            if d.get("status") not in ("deleted",)
        ]
        date_label = "All Time"
        pretty = "All Deals"
    else:
        if date_str:
            base_date = _parse_date(date_str)
            if not base_date:
                await ctx.send("❌ Invalid date. Use `YYYY-MM-DD`.")
                return
            base_dt = datetime(base_date.year, base_date.month, base_date.day, tzinfo=LOCAL_TZ)
        else:
            base_dt = _now_local()

        start_utc, end_utc, start_local, end_local, pretty = _period_bounds(period, base_dt)
        guild_deals = _filter_deals_period(ctx.guild.id, start_utc, end_utc, include_canceled=True)
        if period in ("day", "today"):
            date_label = start_local.date().isoformat()
        elif period in ("month", "thismonth"):
            date_label = start_local.strftime("%Y-%m")
        else:
            date_label = f"{start_local.date()} → {(end_local - timedelta(days=1)).date()}"

    if not guild_deals:
        await ctx.send(f"No deals found for **{date_label}**.")
        return

    lines = [f"**{pretty}** — {date_label}\n"]
    lines.append("`ID  | Status   | Closer         | Setter         | kW    `")
    lines.append("`----|----------|----------------|----------------|-------`")

    for d in guild_deals:
        did = d["id"]
        status = d.get("status", "sold")
        status_short = {"sold": "✅ Sold", "set": "🟡 Set", "no_sale": "🚫 NoSale", 
                       "canceled": "❌ Cancel", "canceled_after_sign": "❌ Cancel"}.get(status, status)
        closer = (d.get("closer_name") or "?")[:14]
        setter = (d.get("setter_name") or "?")[:14]
        kw = f"{d.get('kw', 0):.1f}" if d.get("kw") else "-"
        lines.append(f"`{did:<4}| {status_short:<8} | {closer:<14} | {setter:<14} | {kw:<5}`")

    msg = "\n".join(lines)
    if len(msg) > 1900:
        chunk = ""
        for line in lines:
            if len(chunk) + len(line) + 1 > 1900:
                await ctx.send(chunk)
                chunk = ""
            chunk += line + "\n"
        if chunk:
            await ctx.send(chunk)
    else:
        await ctx.send(msg)


@bot.command(name="leaderboard")
async def leaderboard_cmd(ctx: commands.Context, period: str = "day", date_str: str | None = None):
    """!leaderboard [day|week|month] - Admin only, shows @mentions."""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return

    if not _is_admin_or_manager(ctx.author):
        await ctx.send("⛔ Only admins or managers can use `!leaderboard`.")
        return

    period = period.lower()
    if period not in {"day", "week", "month", "today", "thisweek", "thismonth"}:
        await ctx.send("❌ Invalid period. Use: `day`, `week`, `month`.")
        return

    if date_str:
        base_date = _parse_date(date_str)
        if not base_date:
            await ctx.send("❌ Invalid date. Use `YYYY-MM-DD`.")
            return
        base_dt = datetime(base_date.year, base_date.month, base_date.day, tzinfo=LOCAL_TZ)
    else:
        base_dt = _now_local()

    start_utc, end_utc, start_local, end_local, pretty = _period_bounds(period, base_dt)
    deals = _filter_deals_period(ctx.guild.id, start_utc, end_utc)

    if period in ("day", "today"):
        date_label = start_local.date().isoformat()
    elif period in ("month", "thismonth"):
        date_label = start_local.strftime("%Y-%m")
    else:
        date_label = f"{start_local.date()} → {(end_local - timedelta(days=1)).date()}"

    embed = _build_leaderboard_embed(ctx.guild, deals, pretty, date_label, use_mentions=True)
    await ctx.send(embed=embed)


@bot.command(name="mystats")
async def mystats_cmd(ctx: commands.Context, period: str = "alltime"):
    """!mystats [day|week|month|alltime] - Your personal stats."""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return

    period = period.lower()
    if period not in {"day", "today", "week", "thisweek", "month", "thismonth", "alltime", "all"}:
        await ctx.send("❌ Use: `!mystats [day|week|month|alltime]`")
        return

    user_id = ctx.author.id
    user_name = ctx.author.display_name

    if period in ("alltime", "all"):
        deals = _get_user_deals(ctx.guild.id, user_id, user_name)
        period_label = "All Time"
    else:
        base_dt = _now_local()
        start_utc, end_utc, start_local, end_local, _ = _period_bounds(period, base_dt)
        deals = _get_user_deals_period(ctx.guild.id, user_id, user_name, start_utc, end_utc)

        if period in ("day", "today"):
            period_label = f"Today ({start_local.date().isoformat()})"
        elif period in ("week", "thisweek"):
            period_label = f"This Week ({start_local.date()} → {(end_local - timedelta(days=1)).date()})"
        else:
            period_label = f"This Month ({start_local.strftime('%Y-%m')})"

    # Calculate stats
    sold_deals = [d for d in deals if d.get("status") == "sold"]
    set_deals = [d for d in deals if d.get("status") in ("set", "no_sale", "sold", "canceled_after_sign") and d.get("setter_id") == user_id]
    no_sale_deals = [d for d in deals if d.get("status") == "no_sale" and d.get("closer_id") == user_id]
    canceled_deals = [d for d in deals if d.get("status") == "canceled_after_sign" and d.get("closer_id") == user_id]
    
    closer_deals = [d for d in sold_deals if d.get("closer_id") == user_id]
    setter_deals = [d for d in sold_deals if d.get("setter_id") == user_id or
                   (d.get("setter_name", "").lower().strip() == user_name.lower().strip() and d.get("closer_id") != user_id)]

    total_kw = sum(float(d.get("kw") or 0.0) for d in closer_deals)
    total_rev = sum(_compute_revenue(float(d.get("kw") or 0.0)) or 0.0 for d in closer_deals)
    
    # Close rate
    appts_set = len(set_deals)
    close_rate = (len(closer_deals) / appts_set * 100) if appts_set > 0 else 0.0
    
    # Loss reason breakdown
    loss_counts: Dict[str, int] = {}
    for d in no_sale_deals:
        code = d.get("loss_reason") or "other"
        loss_counts[code] = loss_counts.get(code, 0) + 1

    embed = discord.Embed(
        title=f"📊 Stats for {ctx.author.display_name}",
        description=f"**{period_label}**",
        color=0x3498db,
    )
    
    embed.add_field(name="📞 Appointments Set", value=str(appts_set), inline=True)
    embed.add_field(name="✅ Deals Closed", value=str(len(closer_deals)), inline=True)
    embed.add_field(name="📈 Close Rate", value=f"{close_rate:.1f}%", inline=True)
    
    embed.add_field(name="🚫 No-sales", value=str(len(no_sale_deals)), inline=True)
    embed.add_field(name="❌ Canceled", value=str(len(canceled_deals)), inline=True)
    embed.add_field(name="⚡ Total kW", value=f"{total_kw:.1f}", inline=True)

    if CONFIG_DATA.get("revenue_enabled"):
        embed.add_field(name="💰 Est. Revenue", value=f"${total_rev:,.2f}", inline=True)
    
    if setter_deals:
        setter_kw = sum(float(d.get("kw") or 0.0) for d in setter_deals)
        embed.add_field(name="📋 As Setter (Sold)", value=f"{len(setter_deals)} deals ({setter_kw:.1f} kW)", inline=True)

    # Loss breakdown
    if loss_counts:
        breakdown_lines = []
        total_losses = sum(loss_counts.values())
        for code, count in loss_counts.items():
            label = LOSS_REASON_LABELS.get(code, code.title())
            pct = (count / total_losses) * 100 if total_losses else 0
            breakdown_lines.append(f"**{label}** – {count} ({pct:.0f}%)")
        embed.add_field(
            name="🧠 No-sale Breakdown",
            value="\n".join(breakdown_lines) if breakdown_lines else "None",
            inline=False,
        )

    embed.set_footer(text="Usage: !mystats [day|week|month|alltime]")
    await ctx.send(embed=embed)


@bot.command(name="todaystats")
async def today_stats_cmd(ctx: commands.Context):
    """!todaystats - Today's team performance."""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return

    now = _now_local()
    start_utc, end_utc, _, _, _ = _period_bounds("day", now)
    deals = _filter_deals_period(ctx.guild.id, start_utc, end_utc, include_canceled=True)

    sets = len([d for d in deals if d.get("status") in ("set", "no_sale")])
    sold = len([d for d in deals if d.get("status") == "sold"])
    total_kw = sum(float(d.get("kw") or 0.0) for d in deals if d.get("status") == "sold")
    total_rev = sum(_compute_revenue(float(d.get("kw") or 0.0)) or 0.0 for d in deals if d.get("status") == "sold")

    embed = discord.Embed(
        title="📅 Today's Performance",
        color=discord.Color.green(),
        timestamp=_now_utc(),
    )
    embed.add_field(name="📞 Appointments Set", value=str(sets), inline=True)
    embed.add_field(name="✅ Deals Closed", value=str(sold), inline=True)
    embed.add_field(name="⚡ Total kW", value=f"{total_kw:.1f}", inline=True)
    if CONFIG_DATA.get("revenue_enabled"):
        embed.add_field(name="💰 Est. Revenue", value=f"${total_rev:,.2f}", inline=True)

    await ctx.send(embed=embed)


@bot.command(name="pendingdeals")
async def pending_deals_cmd(ctx: commands.Context):
    """!pendingdeals - Show appointments waiting to be closed."""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return

    pending = [d for d in _get_guild_deals(ctx.guild.id) if d.get("status") == "set"]
    if not pending:
        await ctx.send("✅ No pending appointments!")
        return

    embed = discord.Embed(
        title="🔔 Pending Appointments",
        description=f"{len(pending)} appointment(s) waiting to be closed",
        color=discord.Color.orange(),
        timestamp=_now_utc(),
    )

    for d in sorted(pending, key=lambda x: x.get("created_at", ""))[:10]:
        created = d.get("created_at")
        try:
            created_str = datetime.fromisoformat(created).strftime("%m/%d %H:%M")
        except Exception:
            created_str = "N/A"
        embed.add_field(
            name=f"{d.get('customer_name', 'Unknown')}",
            value=f"Setter: {d.get('setter_name', 'Unknown')}\nCreated: {created_str}",
            inline=True,
        )

    if len(pending) > 10:
        embed.set_footer(text=f"Showing 10 of {len(pending)} pending deals")

    await ctx.send(embed=embed)


@bot.command(name="export_csv")
async def export_csv_cmd(ctx: commands.Context, period: str = "all"):
    """!export_csv [day|week|month|all] - Export deals to CSV (admin only)."""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return

    if not _is_admin_or_manager(ctx.author):
        await ctx.send("⛔ Only admins or managers can export data.")
        return

    period = period.lower()
    now = _now_local()
    
    if period == "all":
        guild_deals = _get_guild_deals(ctx.guild.id)
    else:
        start_utc, end_utc, _, _, _ = _period_bounds(period, now)
        guild_deals = _filter_deals_period(ctx.guild.id, start_utc, end_utc, include_canceled=True)

    filename = f"/tmp/deals_{period}_{int(_now_utc().timestamp())}.csv"
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Deal ID", "Customer", "Setter", "Closer", "Status", "kW",
            "Revenue", "Loss Reason", "Created At", "Closed At", "Canceled At"
        ])

        for d in guild_deals:
            kw = float(d.get("kw") or 0.0)
            rev = _compute_revenue(kw) or 0.0
            writer.writerow([
                d.get("id"),
                d.get("customer_name"),
                d.get("setter_name"),
                d.get("closer_name"),
                d.get("status"),
                kw if kw else "",
                rev if rev else "",
                d.get("loss_reason_detail") or d.get("loss_reason") or "",
                d.get("created_at") or "",
                d.get("closed_at") or "",
                d.get("canceled_at") or "",
            ])

    await ctx.send(
        f"📁 Exported {len(guild_deals)} deals for **{period}**.",
        file=discord.File(filename, filename=os.path.basename(filename)),
    )


@bot.command(name="set_revenue")
async def set_revenue_cmd(ctx: commands.Context, value: str = None):
    """!set_revenue [off|amount] - Enable/disable revenue tracking (admin only)."""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return

    if not _is_admin_or_manager(ctx.author):
        await ctx.send("⛔ Only admins or managers can configure revenue.")
        return

    if not value:
        status = "enabled" if CONFIG_DATA.get("revenue_enabled") else "disabled"
        per_kw = CONFIG_DATA.get("revenue_per_kw", 0)
        await ctx.send(f"💰 Revenue is currently **{status}** at ${per_kw:.2f} per kW.\n"
                      f"Use `!set_revenue off` or `!set_revenue 400` to change.")
        return

    value = value.lower()
    if value in {"off", "0", "none", "disable"}:
        CONFIG_DATA["revenue_enabled"] = False
        CONFIG_DATA["revenue_per_kw"] = 0.0
        _save_config(CONFIG_DATA)
        await ctx.send("💸 Revenue display has been **disabled**.")
        return

    try:
        kw_value = float(value)
    except ValueError:
        await ctx.send("❌ Usage: `!set_revenue off` or `!set_revenue 400` (meaning $400 per kW).")
        return

    CONFIG_DATA["revenue_enabled"] = True
    CONFIG_DATA["revenue_per_kw"] = kw_value
    _save_config(CONFIG_DATA)
    await ctx.send(f"💸 Revenue enabled at **${kw_value:.2f} per kW**.")


@bot.command(name="set_ghl")
async def set_ghl_cmd(ctx: commands.Context, webhook_url: str = None):
    """!set_ghl [webhook_url|off] - Configure GHL webhook (admin only)."""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return

    if not _is_admin_or_manager(ctx.author):
        await ctx.send("⛔ Only admins or managers can configure GHL.")
        return

    if not webhook_url:
        status = "enabled" if CONFIG_DATA.get("ghl_enabled") else "disabled"
        await ctx.send(f"🔗 GHL webhook is currently **{status}**.\n"
                      f"Use `!set_ghl <webhook_url>` or `!set_ghl off` to change.")
        return

    if webhook_url.lower() in {"off", "disable", "none"}:
        CONFIG_DATA["ghl_enabled"] = False
        CONFIG_DATA["ghl_webhook"] = None
        _save_config(CONFIG_DATA)
        await ctx.send("🔗 GHL webhook has been **disabled**.")
        return

    CONFIG_DATA["ghl_enabled"] = True
    CONFIG_DATA["ghl_webhook"] = webhook_url
    _save_config(CONFIG_DATA)
    await ctx.send("🔗 GHL webhook has been **enabled**. Events will be sent to your webhook.")


@bot.command(name="help")
async def help_cmd(ctx: commands.Context):
    embed = discord.Embed(
        title="📊 Deal Tracker – Command Guide",
        color=0x95a5a6,
        description=(
            "Track appointments, deals, no-sales, and leaderboards.\n"
            "_All times in Central Time._"
        ),
    )

    embed.add_field(
        name="📝 Hashtag Workflows",
        value=(
            "`#set Customer Name` — Log an appointment\n"
            "`#sold @Setter kW` — Log a sold deal\n"
            "`#sold Customer Name kW` — Close a pending appointment\n"
            "`#nosale Customer Name` — Mark as no-sale\n"
            "`#cancel Customer Name` — Cancel a deal\n"
            "`#delete <ID>` — Delete a deal (admin)"
        ),
        inline=False,
    )

    embed.add_field(
        name="📊 Stats & Leaderboards",
        value=(
            "`!mystats [day|week|month|alltime]` — Your stats\n"
            "`!todaystats` — Today's team performance\n"
            "`!pendingdeals` — Appointments waiting to close\n"
            "`!leaderboard [day|week|month]` — Team rankings (admin)"
        ),
        inline=False,
    )

    embed.add_field(
        name="🔧 Admin Tools",
        value=(
            "`!deals [day|week|month|all]` — List all deals with IDs\n"
            "`!export_csv [period]` — Export to spreadsheet\n"
            "`!set_revenue [off|amount]` — Configure $ per kW\n"
            "`!set_ghl [webhook_url|off]` — Configure GHL webhook\n"
            "`#soldfor @Closer @Setter kW` — Log for others\n"
            "`#clearleaderboard` — Wipe all deals"
        ),
        inline=False,
    )

    embed.set_footer(text="Leaderboard channels update automatically with each sale.")
    await ctx.send(embed=embed)


# ---------------------------------------------------------------
# Run
# ---------------------------------------------------------------

if __name__ == "__main__":
    token = os.getenv("DISCORD_BOT_TOKEN")
    if not token:
        print("Error: DISCORD_BOT_TOKEN environment variable is not set.")
    else:
        bot.run(token)
