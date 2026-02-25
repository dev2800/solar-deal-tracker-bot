import os
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import discord
from discord.ext import commands, tasks

# ------------------------
# Timezone
# ------------------------

LOCAL_TZ = ZoneInfo("America/Chicago")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_local() -> datetime:
    return datetime.now(LOCAL_TZ)


# ------------------------
# Paths / storage
# ------------------------

DATA_DIR = "./data"
os.makedirs(DATA_DIR, exist_ok=True)

DEALS_FILE = os.path.join(DATA_DIR, "deals.json")
SETTINGS_FILE = os.path.join(DATA_DIR, "settings.json")


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


DEALS_DATA = _load_deals()


# ---------------------------------------------------------------
# Per-guild settings  (feature toggles)
# ---------------------------------------------------------------
# Defaults — every toggle starts here.  Admins flip with !toggle.

DEFAULT_SETTINGS = {
    # Leaderboard behaviour
    "auto_post_daily": True,       # auto-update daily-leaderboard channel
    "auto_post_weekly": True,      # auto-update weekly-leaderboard channel
    "auto_post_monthly": True,     # auto-update monthly-leaderboard channel
    # What shows on the scoreboard
    "show_setters": True,          # show setter section on scoreboard
    "show_battery_section": True,  # split battery-only into its own section
    "show_kw_on_board": True,      # show kW next to names on scoreboard (ON now)
    # Permissions
    "setter_can_log": False,       # let setters use #sold  (off = closer/admin only)
    # Future hooks (off by default, ready to flip)
    "streaks_enabled": False,      # daily/weekly streak tracking
    "milestones_enabled": False,   # 🎯 celebrate deal milestones (10, 25, 50…)
    "mvp_announcement": False,     # post daily MVP at midnight
}


def _load_settings():
    if not os.path.exists(SETTINGS_FILE):
        return {}
    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_settings(data):
    tmp = SETTINGS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, SETTINGS_FILE)


SETTINGS_DATA = _load_settings()


def _guild_setting(guild_id: int, key: str):
    """Get a setting for a guild, falling back to defaults."""
    gs = SETTINGS_DATA.get(str(guild_id), {})
    return gs.get(key, DEFAULT_SETTINGS.get(key))


def _set_guild_setting(guild_id: int, key: str, value):
    gid = str(guild_id)
    if gid not in SETTINGS_DATA:
        SETTINGS_DATA[gid] = {}
    SETTINGS_DATA[gid][key] = value
    _save_settings(SETTINGS_DATA)


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

# Role names (case-insensitive matching)
ADMIN_ROLES = {"admin", "manager", "owner"}
CLOSER_ROLES = {"closer", "closers"}
SETTER_ROLES = {"setter", "setters"}

# ------------------------
# Permission helpers
# ------------------------


def _user_role_names(member: discord.Member) -> set[str]:
    """Return lowercase set of all role names for a member."""
    return {r.name.lower() for r in getattr(member, "roles", [])}


def _is_admin(member: discord.Member) -> bool:
    if member.guild_permissions.administrator:
        return True
    return bool(_user_role_names(member) & ADMIN_ROLES)


def _is_closer(member: discord.Member) -> bool:
    return bool(_user_role_names(member) & CLOSER_ROLES)


def _is_setter(member: discord.Member) -> bool:
    return bool(_user_role_names(member) & SETTER_ROLES)


def _can_log_sale(member: discord.Member) -> bool:
    """Closer or admin can always log.  Setter only if toggle is on."""
    if _is_admin(member) or _is_closer(member):
        return True
    if _is_setter(member) and _guild_setting(member.guild.id, "setter_can_log"):
        return True
    return False


# ------------------------
# Data helpers
# ------------------------


def _deal_type(kw: float) -> str:
    return "battery_only" if kw == 0.0 else "solar_battery"


def _deal_type_label(dtype: str) -> str:
    return "Battery Only 🔋" if dtype == "battery_only" else "Solar + Battery ☀️🔋"


def _parse_date(date_str: str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        return None


def _get_guild_deals(guild_id: int):
    return [d for d in DEALS_DATA["deals"] if d.get("guild_id") == guild_id]


def _display_name(
    user_id: int | None, stored_name: str, use_mention: bool = False
) -> str:
    """
    If use_mention=True AND we have an ID, return <@id> (clickable mention).
    Otherwise return plain display name (no ping).
    """
    if use_mention and user_id:
        return f"<@{user_id}>"
    return stored_name or "Unknown"


def _add_deal(
    guild_id: int,
    setter_id: int | None,
    setter_name: str | None,
    closer_id: int,
    closer_name: str,
    customer_name: str,
    kw: float,
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
        "kw": float(kw),
        "deal_type": _deal_type(float(kw)),
        "status": "closed",
        "created_at": _now_utc().isoformat(),
    }
    DEALS_DATA["deals"].append(deal)
    _save_deals(DEALS_DATA)
    return deal


def _find_deal_by_id(guild_id: int, deal_id: int):
    for d in _get_guild_deals(guild_id):
        if d.get("id") == deal_id:
            return d
    return None


def _find_latest_deal_by_customer(guild_id: int, customer_name: str):
    customer_lower = customer_name.strip().lower()
    candidates = [
        d
        for d in _get_guild_deals(guild_id)
        if d.get("customer_name", "").strip().lower() == customer_lower
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda d: d.get("created_at") or "", reverse=True)
    return candidates[0]


def _filter_deals_period(guild_id, start_utc, end_utc, include_canceled=False):
    result = []
    for d in _get_guild_deals(guild_id):
        status = d.get("status", "closed")
        if status == "deleted":
            continue
        if not include_canceled and status == "canceled":
            continue
        created_raw = d.get("created_at")
        if not created_raw:
            continue
        try:
            created = datetime.fromisoformat(created_raw)
        except Exception:
            continue
        if start_utc <= created < end_utc:
            result.append(d)
    return result


def _aggregate_by_role(deals, role):
    stats: dict[str, dict] = {}
    for d in deals:
        uid = d.get(f"{role}_id")
        name = (d.get(f"{role}_name") or "").strip()
        if not name:
            continue
        key = str(uid) if uid else name.lower()
        if key not in stats:
            stats[key] = {"id": uid, "name": name, "deals": 0, "kw": 0.0}
        stats[key]["deals"] += 1
        stats[key]["kw"] += float(d.get("kw") or 0.0)
    out = list(stats.values())
    out.sort(key=lambda x: (x["deals"], x["kw"]), reverse=True)
    return out


def _split_by_type(deals):
    solar, battery = [], []
    for d in deals:
        dtype = d.get("deal_type") or _deal_type(float(d.get("kw") or 0.0))
        (battery if dtype == "battery_only" else solar).append(d)
    return solar, battery


def _period_bounds(kind, base_dt):
    kind = kind.lower()
    base_local = base_dt.astimezone(LOCAL_TZ)
    d = base_local.date()

    if kind in ("day", "today"):
        s = datetime(d.year, d.month, d.day, tzinfo=LOCAL_TZ)
        e = s + timedelta(days=1)
        label = "Daily Blitz Scoreboard"
    elif kind in ("week", "thisweek"):
        monday = d - timedelta(days=d.weekday())
        s = datetime(monday.year, monday.month, monday.day, tzinfo=LOCAL_TZ)
        e = s + timedelta(days=7)
        label = "Weekly Blitz Scoreboard"
    elif kind in ("month", "thismonth"):
        s = datetime(d.year, d.month, 1, tzinfo=LOCAL_TZ)
        e = datetime(d.year + (d.month == 12), (d.month % 12) + 1, 1, tzinfo=LOCAL_TZ)
        label = "Monthly Blitz Scoreboard"
    else:
        s = datetime(d.year, d.month, d.day, tzinfo=LOCAL_TZ)
        e = s + timedelta(days=1)
        label = "Blitz Scoreboard"

    return s.astimezone(timezone.utc), e.astimezone(timezone.utc), s, e, label


# ---------------------------------------------------------------
# Scoreboard builders
# ---------------------------------------------------------------

def _section_lines(deals, role, guild_id, use_mention=False, show_kw=False):
    """Build Closer: or Setter: block."""
    agg = _aggregate_by_role(deals, role)
    if not agg:
        return []
    lines = ["Closer :" if role == "closer" else "Setter :", ""]
    for row in agg:
        name = _display_name(row["id"], row["name"], use_mention=use_mention)
        suffix = f" - {row['deals']}"
        if show_kw:
            suffix += f" ({row['kw']:.1f} kW)"
        lines.append(f"  {name}{suffix}")
    return lines


def _build_leaderboard_content(deals, period_label, guild_id):
    """
    Plain-text scoreboard for auto-posting to leaderboard channels.
    NO @mentions — just display names.
    """
    solar, battery = _split_by_type(deals)
    show_setters = _guild_setting(guild_id, "show_setters")
    show_battery = _guild_setting(guild_id, "show_battery_section")
    show_kw = _guild_setting(guild_id, "show_kw_on_board")

    lines = [f"{period_label} ⚡", ""]

    if not deals:
        lines.append("_No deals yet — be the first to log a sale with `#sold`!_")
        return "\n".join(lines)

    # Solar + Battery section
    if solar:
        lines.append("Solar + Battery ☀️🔋")
        lines.append("")
        lines.extend(_section_lines(solar, "closer", guild_id, show_kw=show_kw))
        lines.append("")
        if show_setters:
            sl = _section_lines(solar, "setter", guild_id, show_kw=show_kw)
            if sl:
                lines.extend(sl)
                lines.append("")

    # Battery Only section
    if battery and show_battery:
        lines.append("Battery Only 🔋")
        lines.append("")
        lines.extend(_section_lines(battery, "closer", guild_id, show_kw=show_kw))
        lines.append("")
        if show_setters:
            sl = _section_lines(battery, "setter", guild_id, show_kw=show_kw)
            if sl:
                lines.extend(sl)
                lines.append("")
    elif battery and not show_battery:
        # Battery deals exist but section is off — just fold into totals
        pass

    total = len(deals)
    total_kw = sum(float(d.get("kw") or 0.0) for d in deals)
    lines.append(f"**Total Transactions Sold:** {total}")
    lines.append(f"**Total kW Sold:** {total_kw:.2f} kW")
    lines.append("")
    lines.append(
        "_Commands: type `#sold @Setter kW` in your general chat. "
        "Use `!mystats` to see your own numbers._"
    )
    return "\n".join(lines)


def _build_leaderboard_embed(
    guild: discord.Guild,
    deals,
    period_label: str,
    date_label: str,
    mention_people: bool = True,
):
    """
    Embed version for !leaderboard and nightly main-chat posts.
    Uses @mentions only when mention_people=True.
    """
    embed = discord.Embed(title=f"🏆 {period_label}", description=date_label, color=0xf1c40f)

    if not deals:
        embed.add_field(name="No deals yet", value="Log a sale with `#sold`!", inline=False)
        return embed

    solar, battery = _split_by_type(deals)
    medals = ["🥇", "🥈", "🥉"]
    show_setters = _guild_setting(guild.id, "show_setters")

    def _role_embed_lines(deal_list, role):
        agg = _aggregate_by_role(deal_list, role)
        out = []
        for idx, row in enumerate(agg[:10]):
            icon = medals[idx] if idx < len(medals) else f"{idx+1}."
            mention = _display_name(row["id"], row["name"], use_mention=mention_people)
            out.append(f"{icon} {mention} – {row['deals']} deal(s), {row['kw']:.1f} kW")
        return "\n".join(out)

    if solar:
        cl = _role_embed_lines(solar, "closer")
        if cl:
            embed.add_field(name="☀️🔋 Solar+Battery — Closers", value=cl, inline=False)
        if show_setters:
            sl = _role_embed_lines(solar, "setter")
            if sl:
                embed.add_field(name="☀️🔋 Solar+Battery — Setters", value=sl, inline=False)

    if battery:
        cl = _role_embed_lines(battery, "closer")
        if cl:
            embed.add_field(name="🔋 Battery Only — Closers", value=cl, inline=False)
        if show_setters:
            sl = _role_embed_lines(battery, "setter")
            if sl:
                embed.add_field(name="🔋 Battery Only — Setters", value=sl, inline=False)

    total_kw = sum(float(d.get("kw") or 0.0) for d in deals)
    embed.add_field(
        name="Totals",
        value=(
            f"💼 **Deals:** {len(deals)}\n⚡ **kW:** {total_kw:.1f}\n"
            f"☀️🔋 Solar+Battery: {len(solar)}  •  🔋 Battery Only: {len(battery)}"
        ),
        inline=False,
    )
    embed.set_footer(text="!leaderboard [day|week|month] [YYYY-MM-DD]")
    return embed


# ---------------------------------------------------------------
# Channel + main-chat helpers
# ---------------------------------------------------------------

async def ensure_leaderboard_channels(guild):
    try:
        bot_member = guild.me
        if bot_member is None:
            return
        everyone = guild.default_role
        overwrites = {
            everyone: discord.PermissionOverwrite(
                view_channel=True, read_message_history=True,
                send_messages=False, add_reactions=False,
                create_public_threads=False, create_private_threads=False,
                create_forum_threads=False, send_messages_in_threads=False,
            ),
            bot_member: discord.PermissionOverwrite(
                view_channel=True, read_message_history=True,
                send_messages=True, embed_links=True,
                manage_messages=True, send_messages_in_threads=True,
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
        print(f"[ensure_leaderboard_channels] {guild.id}: {e}")


async def _post_today_leaderboards(guild):
    """Post fresh scoreboards to leaderboard channels (no pings)."""
    now = _now_local()
    gid = guild.id

    channel_map = {}
    for name in LEADERBOARD_CHANNELS:
        chan = discord.utils.get(guild.text_channels, name=name)
        if chan:
            channel_map[name] = chan

    # Daily
    if "daily-leaderboard" in channel_map and _guild_setting(gid, "auto_post_daily"):
        su, eu, sl, _, _ = _period_bounds("day", now)
        deals = _filter_deals_period(gid, su, eu)
        content = _build_leaderboard_content(deals, "Daily Blitz Scoreboard", gid)
        await channel_map["daily-leaderboard"].send(content)

    # Weekly
    if "weekly-leaderboard" in channel_map and _guild_setting(gid, "auto_post_weekly"):
        su, eu, sl, el, _ = _period_bounds("week", now)
        deals = _filter_deals_period(gid, su, eu)
        content = _build_leaderboard_content(deals, "Weekly Blitz Scoreboard", gid)
        await channel_map["weekly-leaderboard"].send(content)

    # Monthly
    if "monthly-leaderboard" in channel_map and _guild_setting(gid, "auto_post_monthly"):
        su, eu, sl, _, _ = _period_bounds("month", now)
        deals = _filter_deals_period(gid, su, eu)
        content = _build_leaderboard_content(deals, "Monthly Blitz Scoreboard", gid)
        await channel_map["monthly-leaderboard"].send(content)


def _get_main_text_channel(guild: discord.Guild) -> discord.TextChannel | None:
    """Best guess for the main chat channel."""
    preferred = ["general", "general-chat"]
    for name in preferred:
        chan = discord.utils.get(guild.text_channels, name=name)
        if chan:
            return chan
    for chan in guild.text_channels:
        if "general" in chan.name:
            return chan
    if guild.system_channel and isinstance(guild.system_channel, discord.TextChannel):
        return guild.system_channel
    return guild.text_channels[0] if guild.text_channels else None


async def _post_scheduled_leaderboards(guild: discord.Guild):
    """Post daily/weekly/monthly embeds with mentions into main chat."""
    channel = _get_main_text_channel(guild)
    if not channel:
        return

    now = _now_local()

    # Daily
    su, eu, sl, el, pretty = _period_bounds("day", now)
    deals_day = _filter_deals_period(guild.id, su, eu)
    day_label = sl.date().isoformat()
    embed_day = _build_leaderboard_embed(guild, deals_day, pretty, day_label, mention_people=True)
    await channel.send(embed=embed_day)

    # Weekly on Sunday
    if now.weekday() == 6:  # Monday=0
        su, eu, sl, el, pretty = _period_bounds("week", now)
        deals_week = _filter_deals_period(guild.id, su, eu)
        week_label = f"{sl.date()} → {(el - timedelta(days=1)).date()}"
        embed_week = _build_leaderboard_embed(guild, deals_week, pretty, week_label, mention_people=True)
        await channel.send(embed=embed_week)

    # Monthly on last day of month
    tomorrow = (now + timedelta(days=1)).date()
    if tomorrow.month != now.month:
        su, eu, sl, el, pretty = _period_bounds("month", now)
        deals_month = _filter_deals_period(guild.id, su, eu)
        month_label = sl.strftime("%Y-%m")
        embed_month = _build_leaderboard_embed(guild, deals_month, pretty, month_label, mention_people=True)
        await channel.send(embed=embed_month)


# ---------------------------------------------------------------
# Nightly scheduled task
# ---------------------------------------------------------------

@tasks.loop(minutes=1)
async def nightly_leaderboard_task():
    """Every minute, check if it's 23:59 local and post scheduled leaderboards."""
    now = _now_local()
    if now.hour == 23 and now.minute == 59:
        for guild in bot.guilds:
            try:
                await _post_scheduled_leaderboards(guild)
            except Exception as e:
                print(f"[nightly_leaderboard_task] {guild.id}: {e}")


@nightly_leaderboard_task.before_loop
async def _before_nightly_leaderboard_task():
    await bot.wait_until_ready()


# ---------------------------------------------------------------
# Events
# ---------------------------------------------------------------

@bot.event
async def on_ready():
    print(f"{bot.user} connected! Guilds: {[g.name for g in bot.guilds]}")
    for guild in bot.guilds:
        await ensure_leaderboard_channels(guild)
    if not nightly_leaderboard_task.is_running():
        nightly_leaderboard_task.start()


@bot.event
async def on_guild_join(guild):
    await ensure_leaderboard_channels(guild)


@bot.event
async def on_message(message):
    if message.author.bot:
        return
    if isinstance(message.channel, discord.TextChannel) and message.channel.name in LEADERBOARD_CHANNELS:
        await bot.process_commands(message)
        return

    content = message.content.strip()
    lower = content.lower()

    # ----------------------------------------------------------------
    # #sold @Setter kW            (closer / admin only, unless toggled)
    # ----------------------------------------------------------------
    if lower.startswith("#sold") and not lower.startswith("#soldfor"):
        if not _can_log_sale(message.author):
            await message.channel.send("⛔ Only **closers** or **admins** can log sales.")
            return

        try:
            parts = content.split()
            if len(parts) < 3:
                raise ValueError

            setter_member = message.mentions[0] if message.mentions else None
            setter_name = setter_id = kw = customer_name = None

            if setter_member:
                mention_token = next((p for p in parts if p.startswith("<@") and p.endswith(">")), None)
                if not mention_token:
                    raise ValueError
                idx = parts.index(mention_token)
                if len(parts) - idx < 2:
                    raise ValueError
                kw = float(parts[-1])
                tokens = parts[idx + 1 : -1]
                customer_name = " ".join(tokens) if tokens else None
                setter_id = setter_member.id
                setter_name = setter_member.display_name
            else:
                kw = float(parts[-1])
                setter_name = parts[1]
                tokens = parts[2:-1]
                customer_name = " ".join(tokens) if tokens else None

            deal = _add_deal(
                guild_id=message.guild.id,
                setter_id=setter_id, setter_name=setter_name,
                closer_id=message.author.id, closer_name=message.author.display_name,
                customer_name=customer_name or "N/A", kw=kw,
            )

            embed = discord.Embed(
                title="🎉 DEAL CLOSED!",
                color=0x2ecc71,
                description=f"Deal for {_display_name(setter_id, setter_name, use_mention=True)} has been logged!",
            )
            embed.add_field(
                name="💼 Closer",
                value=_display_name(message.author.id, message.author.display_name, use_mention=True),
                inline=True,
            )
            embed.add_field(
                name="Setter",
                value=_display_name(setter_id, setter_name, use_mention=True),
                inline=True,
            )
            embed.add_field(name="⚡ System Size", value=f"{deal['kw']:.1f} kW", inline=True)
            embed.add_field(name="Type", value=_deal_type_label(deal["deal_type"]), inline=True)
            if customer_name and customer_name != "N/A":
                embed.add_field(name="Customer", value=deal["customer_name"], inline=True)
            embed.set_footer(text=f"Deal #{deal['id']}")

            await message.channel.send(embed=embed)
            await _post_today_leaderboards(message.guild)

        except ValueError:
            await message.channel.send(
                "❌ Use: `#sold @Setter kW`\nExample: `#sold @Devin 6.5`\nBattery only: `#sold @Devin 0`"
            )
        except Exception as e:
            await message.channel.send(f"❌ Error: {e}")
        return

    # ----------------------------------------------------------------
    # #soldfor @Closer @Setter kW   (admin only)
    # ----------------------------------------------------------------
    if lower.startswith("#soldfor"):
        if not _is_admin(message.author):
            await message.channel.send("⛔ Only admins can use `#soldfor`.")
            return

        try:
            parts = content.split()
            if len(parts) < 4:
                raise ValueError
            mentions = message.mentions
            if len(mentions) < 2:
                raise ValueError("Need @Closer and @Setter")

            mention_tokens = [p for p in parts if p.startswith("<@") and p.endswith(">")]
            if len(mention_tokens) < 2:
                raise ValueError

            closer_member = mentions[0]
            setter_member = mentions[1]

            second_idx = parts.index(mention_tokens[1])
            kw = float(parts[-1])
            tokens = parts[second_idx + 1 : -1]
            customer_name = " ".join(tokens) if tokens else None

            deal = _add_deal(
                guild_id=message.guild.id,
                setter_id=setter_member.id, setter_name=setter_member.display_name,
                closer_id=closer_member.id, closer_name=closer_member.display_name,
                customer_name=customer_name or "N/A", kw=kw,
            )

            embed = discord.Embed(
                title="🎉 DEAL CLOSED! (admin)",
                color=0x2ecc71,
                description=f"Logged by {message.author.display_name}",
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
            embed.add_field(name="⚡ Size", value=f"{deal['kw']:.1f} kW", inline=True)
            embed.add_field(name="Type", value=_deal_type_label(deal["deal_type"]), inline=True)
            embed.set_footer(text=f"Deal #{deal['id']}")

            await message.channel.send(embed=embed)
            await _post_today_leaderboards(message.guild)

        except ValueError:
            await message.channel.send(
                "❌ Use: `#soldfor @Closer @Setter kW`\nExample: `#soldfor @Ethen @Devin 6.5`"
            )
        except Exception as e:
            await message.channel.send(f"❌ Error: {e}")
        return

    # ----------------------------------------------------------------
    # #cancel Customer Name   (admin only)
    # ----------------------------------------------------------------
    if lower.startswith("#cancel"):
        if not _is_admin(message.author):
            await message.channel.send("⛔ Only admins can cancel deals.")
            return
        try:
            parts = content.split(maxsplit=1)
            if len(parts) < 2:
                raise ValueError
            cust = parts[1].strip()
            deal = _find_latest_deal_by_customer(message.guild.id, cust)
            if not deal:
                await message.channel.send(f"❌ No deal found for `{cust}`.")
                return
            if deal.get("status") == "canceled":
                await message.channel.send("ℹ️ Already canceled.")
                return

            deal["status"] = "canceled"
            deal["canceled_at"] = _now_utc().isoformat()
            _save_deals(DEALS_DATA)

            embed = discord.Embed(
                title="⚠️ Deal Canceled",
                color=0xe67e22,
                description=f"Customer: **{deal['customer_name']}**",
            )
            embed.add_field(
                name="Closer",
                value=_display_name(deal.get("closer_id"), deal.get("closer_name", "?")),
                inline=True,
            )
            embed.add_field(name="Size", value=f"{deal['kw']:.1f} kW", inline=True)
            embed.set_footer(text=f"Deal #{deal['id']}")
            await message.channel.send(embed=embed)
            await _post_today_leaderboards(message.guild)

        except ValueError:
            await message.channel.send("❌ Use: `#cancel Customer Name`")
        except Exception as e:
            await message.channel.send(f"❌ Error: {e}")
        return

    # ----------------------------------------------------------------
    # #delete <ID> or #delete Customer Name   (admin only)
    # ----------------------------------------------------------------
    if lower.startswith("#delete"):
        if not _is_admin(message.author):
            await message.channel.send("⛔ Only admins can delete deals.")
            return
        try:
            parts = content.split(maxsplit=1)
            if len(parts) < 2:
                raise ValueError
            target = parts[1].strip()

            deal = None
            try:
                deal = _find_deal_by_id(message.guild.id, int(target))
                if not deal:
                    await message.channel.send(f"❌ No deal with ID `{target}`.")
                    return
            except (ValueError, TypeError):
                deal = _find_latest_deal_by_customer(message.guild.id, target)
                if not deal:
                    await message.channel.send(f"❌ No deal found for `{target}`.")
                    return

            info = (
                f"Deal #{deal['id']} — {deal.get('closer_name','?')} / "
                f"{deal.get('setter_name','?')} / {deal['kw']:.1f} kW"
            )
            DEALS_DATA["deals"] = [d for d in DEALS_DATA["deals"] if d["id"] != deal["id"]]
            _save_deals(DEALS_DATA)
            await message.channel.send(f"🗑️ Deleted: {info}")
            await _post_today_leaderboards(message.guild)

        except ValueError:
            await message.channel.send("❌ Use: `#delete <ID>` or `#delete Customer Name`")
        except Exception as e:
            await message.channel.send(f"❌ Error: {e}")
        return

    # ----------------------------------------------------------------
    # #clearleaderboard   (admin only)
    # ----------------------------------------------------------------
    if lower.startswith("#clearleaderboard"):
        if not _is_admin(message.author):
            await message.channel.send("⛔ Only admins can clear the leaderboard.")
            return
        DEALS_DATA["deals"] = [d for d in DEALS_DATA["deals"] if d.get("guild_id") != message.guild.id]
        _save_deals(DEALS_DATA)
        await message.channel.send("🔥 All deals cleared. Fresh start!")
        await _post_today_leaderboards(message.guild)
        return

    await bot.process_commands(message)


# ---------------------------------------------------------------
# ! Commands
# ---------------------------------------------------------------

@bot.command(name="deals")
async def deals_cmd(ctx, period: str = "day", date_str: str | None = None):
    """!deals [day|week|month|all] — admin only, list deals with IDs."""
    if not ctx.guild:
        return
    if not _is_admin(ctx.author):
        await ctx.send("⛔ Only admins can view the deal list.")
        return

    period = period.lower()
    if period not in {"day", "week", "month", "today", "thisweek", "thismonth", "all"}:
        await ctx.send("❌ Use: `!deals [day|week|month|all]`")
        return

    if period == "all":
        guild_deals = [d for d in _get_guild_deals(ctx.guild.id) if d.get("status") != "deleted"]
        date_label = "All Time"
        pretty = "All Deals"
    else:
        if date_str:
            bd = _parse_date(date_str)
            if not bd:
                await ctx.send("❌ Invalid date.")
                return
            base = datetime(bd.year, bd.month, bd.day, tzinfo=LOCAL_TZ)
        else:
            base = _now_local()
        su, eu, sl, el, pretty = _period_bounds(period, base)
        guild_deals = _filter_deals_period(ctx.guild.id, su, eu, include_canceled=True)
        if period in ("day", "today"):
            date_label = sl.date().isoformat()
        elif period in ("month", "thismonth"):
            date_label = sl.strftime("%Y-%m")
        else:
            date_label = f"{sl.date()} → {(el - timedelta(days=1)).date()}"

    if not guild_deals:
        await ctx.send(f"No deals for **{date_label}**.")
        return

    lines = [f"**{pretty}** — {date_label}\n"]
    lines.append("`ID  | Type   | Closer         | Setter         | kW    | St`")
    lines.append("`----|--------|----------------|----------------|-------|---`")
    for d in guild_deals:
        dtype = "Solar" if d.get("deal_type", "solar_battery") != "battery_only" else "Batt"
        c = (d.get("closer_name") or "?")[:14]
        s = (d.get("setter_name") or "?")[:14]
        kw = f"{d['kw']:.1f}"
        st = {"closed": "✅", "canceled": "❌"}.get(d.get("status", "closed"), "?")
        lines.append(f"`{d['id']:<4}| {dtype:<6} | {c:<14} | {s:<14} | {kw:<5} | {st}`")

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
async def leaderboard_cmd(ctx, period: str = "day", date_str: str | None = None):
    """
    !leaderboard [day|week|month] [YYYY-MM-DD]
    Admin = pings; non-admin = same board, no pings.
    """
    if not ctx.guild:
        return

    period = period.lower()
    if period not in {"day", "week", "month", "today", "thisweek", "thismonth"}:
        await ctx.send("❌ Use: `day`, `week`, or `month`.")
        return

    if date_str:
        bd = _parse_date(date_str)
        if not bd:
            await ctx.send("❌ Invalid date.")
            return
        base = datetime(bd.year, bd.month, bd.day, tzinfo=LOCAL_TZ)
    else:
        base = _now_local()

    su, eu, sl, el, pretty = _period_bounds(period, base)
    deals = _filter_deals_period(ctx.guild.id, su, eu)

    if period in ("day", "today"):
        dl = sl.date().isoformat()
    elif period in ("month", "thismonth"):
        dl = sl.strftime("%Y-%m")
    else:
        dl = f"{sl.date()} → {(el - timedelta(days=1)).date()}"

    mention_people = _is_admin(ctx.author)
    embed = _build_leaderboard_embed(ctx.guild, deals, pretty, dl, mention_people=mention_people)
    await ctx.send(embed=embed)


@bot.command(name="mystats")
async def mystats_cmd(ctx):
    """
    Show stats where the user is either the closer OR the setter.
    (So setters see their numbers too.)
    """
    if not ctx.guild:
        return
    deals = [
        d
        for d in _get_guild_deals(ctx.guild.id)
        if (d.get("closer_id") == ctx.author.id or d.get("setter_id") == ctx.author.id)
        and d.get("status") not in ("canceled", "deleted")
    ]
    total_kw = sum(float(d.get("kw") or 0.0) for d in deals)
    solar, battery = _split_by_type(deals)

    embed = discord.Embed(title=f"📊 Stats for {ctx.author.display_name}", color=0x3498db)
    embed.add_field(name="Deals (Closer + Setter)", value=str(len(deals)), inline=True)
    embed.add_field(name="Total kW", value=f"{total_kw:.1f}", inline=True)
    if solar:
        embed.add_field(name="☀️🔋 Solar+Batt", value=str(len(solar)), inline=True)
    if battery:
        embed.add_field(name="🔋 Battery Only", value=str(len(battery)), inline=True)
    await ctx.send(embed=embed)


# ---------------------------------------------------------------
# !toggle  (admin only — feature switches)
# ---------------------------------------------------------------

@bot.command(name="toggle")
async def toggle_cmd(ctx, feature: str | None = None):
    """
    !toggle              — show all settings
    !toggle <feature>    — flip a boolean setting on/off
    """
    if not ctx.guild:
        return
    if not _is_admin(ctx.author):
        await ctx.send("⛔ Only admins can manage settings.")
        return

    gid = ctx.guild.id

    if feature is None:
        # Show current settings
        lines = ["**⚙️ Server Settings**\n"]
        for key, default in DEFAULT_SETTINGS.items():
            current = _guild_setting(gid, key)
            status = "✅ ON" if current else "❌ OFF"
            lines.append(f"`{key}` — {status}")
        lines.append("\nFlip a setting: `!toggle <name>`")
        await ctx.send("\n".join(lines))
        return

    feature = feature.lower().replace("-", "_")
    if feature not in DEFAULT_SETTINGS:
        await ctx.send(f"❌ Unknown setting `{feature}`.\nUse `!toggle` to see all options.")
        return

    current = _guild_setting(gid, feature)
    if not isinstance(current, bool):
        await ctx.send(f"❌ `{feature}` is not a toggle.")
        return

    new_val = not current
    _set_guild_setting(gid, feature, new_val)
    status = "✅ ON" if new_val else "❌ OFF"
    await ctx.send(f"⚙️ `{feature}` is now {status}")


# ---------------------------------------------------------------
# !help  (role-aware)
# ---------------------------------------------------------------

@bot.command(name="help")
async def help_cmd(ctx):
    if not ctx.guild:
        return

    is_admin = _is_admin(ctx.author)
    is_closer = _is_closer(ctx.author)
    is_setter_role = _is_setter(ctx.author)

    embed = discord.Embed(
        title="☀️ Solar Leaderboard Bot",
        color=0x95a5a6,
        description="_All times in Central Time._",
    )

    # Everyone can see their stats
    embed.add_field(
        name="Your Stats",
        value="`!mystats` — your deals as closer or setter",
        inline=False,
    )

    # Closers (and admins) see how to log
    if is_closer or is_admin:
        embed.add_field(
            name="Log a Sale",
            value=(
                "`#sold @Setter kW` — Solar + Battery\n"
                "`#sold @Setter 0` — Battery Only\n"
                "• Example: `#sold @Devin 6.5`\n"
                "• With customer: `#sold @Devin John Smith 6.5`"
            ),
            inline=False,
        )

    # Setters see a note for now
    if is_setter_role and not is_closer and not is_admin:
        embed.add_field(
            name="Setter Info",
            value=(
                "Your set appointments show up on the scoreboard when a closer logs the deal.\n"
                "You can always see your stats with `!mystats`."
            ),
            inline=False,
        )

    # Admin sees everything
    if is_admin:
        embed.add_field(
            name="Admin: Log for Others",
            value=(
                "`#soldfor @Closer @Setter kW`\n"
                "• Example: `#soldfor @Ethen @Devin 6.5`"
            ),
            inline=False,
        )

        embed.add_field(
            name="Admin: Cancel / Delete",
            value=(
                "`#cancel Customer Name` — mark canceled\n"
                "`#delete <DealID>` — delete by ID\n"
                "`#delete Customer Name` — delete by name\n"
                "`!deals [day|week|month|all]` — list all deals with IDs"
            ),
            inline=False,
        )

        embed.add_field(
            name="Admin: Leaderboard & Settings",
            value=(
                "`!leaderboard [day|week|month] [YYYY-MM-DD]`\n"
                "`!toggle` — view/change feature toggles\n"
                "`#clearleaderboard` — wipe all deals"
            ),
            inline=False,
        )

    if not is_admin and not is_closer and not is_setter_role:
        embed.add_field(
            name="Getting Started",
            value="Ask your admin to assign you a **Closer** or **Setter** role to start logging deals!",
            inline=False,
        )

    embed.set_footer(text="Leaderboard channels update automatically — use #sold in general chat.")
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