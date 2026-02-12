import os
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import discord
from discord.ext import commands

# ------------------------
# Timezone
# ------------------------

# All "today / week / month" logic is based on Central Time
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

# ------------------------
# Discord bot setup
# ------------------------

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

# Names of the read-only leaderboard channels we manage
LEADERBOARD_CHANNELS = {
    "daily-leaderboard": "Daily sales leaderboard (read-only)",
    "weekly-leaderboard": "Weekly sales leaderboard (read-only)",
    "monthly-leaderboard": "Monthly sales leaderboard (read-only)",
}

# ------------------------
# Helpers
# ------------------------


def _deal_type(kw: float) -> str:
    """Return 'battery_only' if kW is 0, otherwise 'solar_battery'."""
    return "battery_only" if kw == 0.0 else "solar_battery"


def _deal_type_label(dtype: str) -> str:
    if dtype == "battery_only":
        return "Battery Only 🔋"
    return "Solar + Battery ☀️🔋"


def _parse_date(date_str: str):
    """Parse YYYY-MM-DD into a date object, or None if invalid."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        return None


def _get_guild_deals(guild_id: int):
    return [d for d in DEALS_DATA["deals"] if d.get("guild_id") == guild_id]


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


def _find_latest_deal_by_customer(guild_id: int, customer_name: str):
    """Return the most recent deal for this customer in this guild, or None."""
    customer_lower = customer_name.strip().lower()
    candidates = [
        d
        for d in _get_guild_deals(guild_id)
        if d.get("customer_name", "").strip().lower() == customer_lower
    ]
    if not candidates:
        return None
    candidates.sort(
        key=lambda d: d.get("created_at") or "",
        reverse=True,
    )
    return candidates[0]


def _filter_deals_period(
    guild_id: int,
    start_utc: datetime,
    end_utc: datetime,
    include_canceled: bool = False,
):
    deals = _get_guild_deals(guild_id)
    result = []
    for d in deals:
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


def _aggregate_by_closer(deals: list[dict]):
    """Return list of {name, deals, kw} sorted by deals then kw desc."""
    stats: dict[int | None, dict] = {}
    for d in deals:
        cid = d.get("closer_id")
        if cid not in stats:
            stats[cid] = {
                "name": d.get("closer_name", "Unknown"),
                "deals": 0,
                "kw": 0.0,
            }
        stats[cid]["deals"] += 1
        stats[cid]["kw"] += float(d.get("kw") or 0.0)
    out = list(stats.values())
    out.sort(key=lambda x: (x["deals"], x["kw"]), reverse=True)
    return out


def _aggregate_by_setter(deals: list[dict]):
    """Return list of {name, deals, kw} sorted by deals then kw desc for setters."""
    stats: dict[str, dict] = {}
    for d in deals:
        name = (d.get("setter_name") or "").strip()
        if not name:
            continue
        key = name.lower()
        if key not in stats:
            stats[key] = {
                "name": name,
                "deals": 0,
                "kw": 0.0,
            }
        stats[key]["deals"] += 1
        stats[key]["kw"] += float(d.get("kw") or 0.0)
    out = list(stats.values())
    out.sort(key=lambda x: (x["deals"], x["kw"]), reverse=True)
    return out


def _count_by_type(deals: list[dict]):
    """Count deals by type (solar_battery vs battery_only)."""
    solar_battery = 0
    battery_only = 0
    for d in deals:
        # Support old deals that don't have deal_type field
        dtype = d.get("deal_type")
        if dtype is None:
            dtype = _deal_type(float(d.get("kw") or 0.0))
        if dtype == "battery_only":
            battery_only += 1
        else:
            solar_battery += 1
    return solar_battery, battery_only


def _period_bounds(kind: str, base_dt: datetime):
    """
    Given kind in {"day","week","month"} and a timezone-aware datetime,
    treat it in LOCAL_TZ and return:
      (start_utc, end_utc, start_local, end_local, pretty_kind)
    where boundaries are midnight LOCAL_TZ.
    """
    kind = kind.lower()
    base_local = base_dt.astimezone(LOCAL_TZ)
    d = base_local.date()

    if kind in ("day", "today"):
        start_local = datetime(d.year, d.month, d.day, tzinfo=LOCAL_TZ)
        end_local = start_local + timedelta(days=1)
        pretty_kind = "Daily Leaderboard"
    elif kind in ("week", "thisweek"):
        monday = d - timedelta(days=d.weekday())
        start_local = datetime(monday.year, monday.month, monday.day, tzinfo=LOCAL_TZ)
        end_local = start_local + timedelta(days=7)
        pretty_kind = "Weekly Leaderboard"
    elif kind in ("month", "thismonth"):
        start_local = datetime(d.year, d.month, 1, tzinfo=LOCAL_TZ)
        if d.month == 12:
            end_local = datetime(d.year + 1, 1, 1, tzinfo=LOCAL_TZ)
        else:
            end_local = datetime(d.year, d.month + 1, 1, tzinfo=LOCAL_TZ)
        pretty_kind = "Monthly Leaderboard"
    else:
        start_local = datetime(d.year, d.month, d.day, tzinfo=LOCAL_TZ)
        end_local = start_local + timedelta(days=1)
        pretty_kind = "Leaderboard"

    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)
    return start_utc, end_utc, start_local, end_local, pretty_kind


# ---------------------------------------------------------------
# Build leaderboard content  (plain-text style matching screenshot)
# ---------------------------------------------------------------

def _build_leaderboard_content(
    deals: list[dict],
    period_label: str,
    date_label: str,
) -> str:
    """
    Build a plain-text leaderboard message that matches the
    'Weekly Blitz Scoreboard' screenshot format.
    """
    # Header
    lines = []
    lines.append(f"**{period_label}** ⚡")

    # Deal type breakdown
    solar_count, battery_count = _count_by_type(deals)
    type_parts = []
    if solar_count > 0:
        type_parts.append(f"Solar + Battery ☀️🔋")
    if battery_count > 0:
        type_parts.append(f"Battery Only 🔋")
    if type_parts:
        lines.append(" | ".join(type_parts))
    else:
        lines.append("Solar + Battery ☀️🔋")

    lines.append("")

    if not deals:
        lines.append("_No deals yet — be the first to log a sale with `#sold`!_")
        return "\n".join(lines)

    # Closer section
    by_closer = _aggregate_by_closer(deals)
    lines.append("**Closer:**")
    for row in by_closer:
        lines.append(f"{row['name']} - {row['deals']}")
    lines.append("")

    # Setter section
    by_setter = _aggregate_by_setter(deals)
    if by_setter:
        lines.append("**Setter:**")
        for row in by_setter:
            lines.append(f"{row['name']} - {row['deals']}")
        lines.append("")

    # Totals
    total_deals = len(deals)
    total_kw = sum(float(d.get("kw") or 0.0) for d in deals)

    lines.append(f"**Total Transactions Sold:** {total_deals}")
    lines.append(f"**Total kW Sold:** {total_kw:.2f} kW")

    # Battery breakdown if there are battery-only deals
    if battery_count > 0:
        lines.append(f"**Battery Only Deals:** {battery_count}")
    if solar_count > 0 and battery_count > 0:
        lines.append(f"**Solar + Battery Deals:** {solar_count}")

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
):
    """Build an embed-style leaderboard (used by !leaderboard command)."""
    embed = discord.Embed(
        title="🏆 Solar Sales Leaderboard",
        description=f"{period_label} • {date_label}",
        color=0xf1c40f,
    )

    if not deals:
        embed.add_field(
            name="No deals yet",
            value="Be the first to log a sale today with `#sold` in your general chat!",
            inline=False,
        )
        return embed

    # Closers
    by_closer = _aggregate_by_closer(deals)
    closer_lines = []
    medals = ["🥇", "🥈", "🥉"]
    for idx, row in enumerate(by_closer[:10]):
        icon = medals[idx] if idx < len(medals) else f"{idx+1}."
        closer_lines.append(
            f"{icon} **{row['name']}** – {row['deals']} deal(s), {row['kw']:.1f} kW"
        )
    embed.add_field(name="Top Closers", value="\n".join(closer_lines), inline=False)

    # Setters
    by_setter = _aggregate_by_setter(deals)
    if by_setter:
        setter_lines = []
        for idx, row in enumerate(by_setter[:10]):
            icon = medals[idx] if idx < len(medals) else f"{idx+1}."
            setter_lines.append(
                f"{icon} **{row['name']}** – {row['deals']} deal(s), {row['kw']:.1f} kW"
            )
        embed.add_field(name="Top Setters", value="\n".join(setter_lines), inline=False)

    # Type breakdown
    solar_count, battery_count = _count_by_type(deals)
    total_deals = len(deals)
    total_kw = sum(float(d.get("kw") or 0.0) for d in deals)

    totals_value = f"💼 **Deals:** {total_deals}\n⚡ **kW:** {total_kw:.1f}"
    if solar_count > 0:
        totals_value += f"\n☀️🔋 **Solar + Battery:** {solar_count}"
    if battery_count > 0:
        totals_value += f"\n🔋 **Battery Only:** {battery_count}"

    embed.add_field(
        name="Totals",
        value=totals_value,
        inline=False,
    )

    embed.set_footer(
        text="Use !leaderboard [day|week|month] [YYYY-MM-DD] for history • Times are in Central Time"
    )
    return embed


async def ensure_leaderboard_channels(guild: discord.Guild):
    """Create / fix the three read-only leaderboard channels."""
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
                await guild.create_text_channel(
                    name,
                    topic=topic,
                    overwrites=overwrites,
                )
            else:
                await chan.edit(topic=topic, overwrites=overwrites)
    except discord.Forbidden:
        return
    except Exception as e:
        print(f"[ensure_leaderboard_channels] error in guild {guild.id}: {e}")


async def _post_today_leaderboards(guild: discord.Guild):
    """Recalculate today/week/month and drop fresh messages in the three channels.

    Uses the plain-text 'Blitz Scoreboard' format for the channel posts
    (matching the screenshot style).
    """
    now_local = _now_local()

    # Day
    start_day_utc, end_day_utc, start_day_local, _, _ = _period_bounds("day", now_local)
    deals_day = _filter_deals_period(guild.id, start_day_utc, end_day_utc)

    # Week
    start_week_utc, end_week_utc, start_week_local, end_week_local, _ = _period_bounds(
        "week", now_local
    )
    deals_week = _filter_deals_period(guild.id, start_week_utc, end_week_utc)

    # Month
    (
        start_month_utc,
        end_month_utc,
        start_month_local,
        _,
        _,
    ) = _period_bounds("month", now_local)
    deals_month = _filter_deals_period(guild.id, start_month_utc, end_month_utc)

    channel_map = {}
    for name in LEADERBOARD_CHANNELS.keys():
        chan = discord.utils.get(guild.text_channels, name=name)
        if chan:
            channel_map[name] = chan

    # Daily — plain text scoreboard
    if "daily-leaderboard" in channel_map:
        content = _build_leaderboard_content(
            deals_day,
            "Daily Blitz Scoreboard",
            start_day_local.date().isoformat(),
        )
        await channel_map["daily-leaderboard"].send(content)

    # Weekly — plain text scoreboard
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

    # Monthly — plain text scoreboard
    if "monthly-leaderboard" in channel_map:
        month_label = start_month_local.date().strftime("%Y-%m")
        content = _build_leaderboard_content(
            deals_month,
            "Monthly Blitz Scoreboard",
            month_label,
        )
        await channel_map["monthly-leaderboard"].send(content)


# ------------------------
# Events
# ------------------------


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
    # Always ignore ourselves / bots
    if message.author.bot:
        return

    # Ignore hashtag logging commands in leaderboard channels
    if (
        isinstance(message.channel, discord.TextChannel)
        and message.channel.name in LEADERBOARD_CHANNELS
    ):
        await bot.process_commands(message)
        return

    content = message.content.strip()
    lower = content.lower()

    # ------------------------
    # #sold @Setter kW          (no customer name, just setter + kW)
    # #sold @Setter Customer kW (original format also still works)
    # ------------------------
    if lower.startswith("#sold"):
        try:
            parts = content.split()
            if len(parts) < 3:
                raise ValueError

            setter_member = message.mentions[0] if message.mentions else None
            setter_name = None
            setter_id = None
            kw = None
            customer_name = None

            if setter_member:
                mention_token = None
                for p in parts:
                    if p.startswith("<@") and p.endswith(">"):
                        mention_token = p
                        break
                if mention_token is None:
                    raise ValueError
                idx = parts.index(mention_token)
                if len(parts) - idx < 2:
                    raise ValueError
                kw_token = parts[-1]
                kw = float(kw_token)
                customer_tokens = parts[idx + 1 : -1]
                # Customer name is optional now
                customer_name = " ".join(customer_tokens) if customer_tokens else None
                setter_id = setter_member.id
                setter_name = setter_member.display_name
            else:
                # #sold SetterName kW  or  #sold SetterName Customer Name kW
                kw_token = parts[-1]
                kw = float(kw_token)
                setter_name = parts[1]
                setter_id = None
                customer_tokens = parts[2:-1]
                customer_name = " ".join(customer_tokens) if customer_tokens else None

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
            )

            # Determine deal type label
            dtype = _deal_type(kw)
            dtype_label = _deal_type_label(dtype)

            # Confirmation embed — matches the "DEAL CLOSED!" screenshot style
            embed = discord.Embed(
                title="🎉 DEAL CLOSED!",
                color=0x2ecc71,
                description=f"Deal for **{setter_name or 'N/A'}** has been logged!",
            )
            embed.add_field(name="💼 Closer", value=closer_name, inline=True)
            embed.add_field(name="Setter", value=setter_name or "N/A", inline=True)
            embed.add_field(name="⚡ System Size", value=f"{deal['kw']:.1f} kW", inline=True)
            embed.add_field(name="Type", value=dtype_label, inline=True)
            if customer_name and customer_name != "N/A":
                embed.add_field(name="Customer", value=deal["customer_name"], inline=True)
            embed.set_footer(text=f"Deal ID: {deal['id']} • Logged via #sold")

            await message.channel.send(embed=embed)
            await _post_today_leaderboards(message.guild)

        except ValueError:
            await message.channel.send(
                "❌ Invalid `#sold` format.\n"
                "Use: `#sold @Setter kW`\n"
                "Example: `#sold @Devin 6.5`\n"
                "For battery only: `#sold @Devin 0`"
            )
        except Exception as e:
            await message.channel.send(f"❌ Error processing sale: {e}")

        return

    # ------------------------
    # #cancel Customer Name  (marks last deal for that customer as canceled)
    # ------------------------
    if lower.startswith("#cancel"):
        try:
            parts = content.split(maxsplit=1)
            if len(parts) < 2:
                raise ValueError
            customer_name = parts[1].strip()
            deal = _find_latest_deal_by_customer(message.guild.id, customer_name)
            if not deal:
                await message.channel.send(
                    f"❌ No deal found for customer `{customer_name}`."
                )
                return

            if deal.get("status") == "canceled":
                await message.channel.send(
                    f"ℹ️ Latest deal for `{customer_name}` is already marked as canceled."
                )
                return

            deal["status"] = "canceled"
            deal["canceled_at"] = _now_utc().isoformat()
            _save_deals(DEALS_DATA)

            embed = discord.Embed(
                title="⚠️ Deal Canceled After Signing",
                color=0xe67e22,
                description=f"Customer: **{deal['customer_name']}**",
            )
            embed.add_field(
                name="Original Closer",
                value=deal.get("closer_name", "Unknown"),
                inline=True,
            )
            if deal.get("setter_name"):
                embed.add_field(
                    name="Setter", value=deal["setter_name"], inline=True
                )
            embed.add_field(
                name="System Size",
                value=f"{deal['kw']:.1f} kW",
                inline=True,
            )
            await message.channel.send(embed=embed)

            await _post_today_leaderboards(message.guild)

        except ValueError:
            await message.channel.send("❌ Use: `#cancel Customer Name`")
        except Exception as e:
            await message.channel.send(f"❌ Error marking canceled: {e}")
        return

    # ------------------------
    # #delete Customer Name  (admin/manager only)
    # ------------------------
    if lower.startswith("#delete"):
        perms = message.author.guild_permissions
        has_power_role = any(
            r.name.lower() in {"admin", "manager"}
            for r in getattr(message.author, "roles", [])
        )
        if not (perms.administrator or has_power_role):
            await message.channel.send(
                "⛔ Only admins or managers can delete deals."
            )
            return

        try:
            parts = content.split(maxsplit=1)
            if len(parts) < 2:
                raise ValueError
            customer_name = parts[1].strip()
            deal = _find_latest_deal_by_customer(message.guild.id, customer_name)
            if not deal:
                await message.channel.send(
                    f"❌ No deal found for customer `{customer_name}`."
                )
                return

            DEALS_DATA["deals"] = [
                d for d in DEALS_DATA["deals"] if d["id"] != deal["id"]
            ]
            _save_deals(DEALS_DATA)

            await message.channel.send(
                f"🗑️ Deleted latest deal for `{customer_name}` from stats."
            )
            await _post_today_leaderboards(message.guild)

        except ValueError:
            await message.channel.send("❌ Use: `#delete Customer Name`")
        except Exception as e:
            await message.channel.send(f"❌ Error deleting deal: {e}")
        return

    # ------------------------
    # #clearleaderboard  (admin/manager only, wipes all deals for this guild)
    # ------------------------
    if lower.startswith("#clearleaderboard"):
        perms = message.author.guild_permissions
        has_power_role = any(
            r.name.lower() in {"admin", "manager"}
            for r in getattr(message.author, "roles", [])
        )
        if not (perms.administrator or has_power_role):
            await message.channel.send(
                "⛔ Only admins or managers can clear the leaderboard."
            )
            return

        DEALS_DATA["deals"] = [
            d for d in DEALS_DATA["deals"] if d.get("guild_id") != message.guild.id
        ]
        _save_deals(DEALS_DATA)
        await message.channel.send(
            "🔥 All deals for this server have been cleared. Fresh start!"
        )
        await _post_today_leaderboards(message.guild)
        return

    # Let prefix commands (like !leaderboard, !help) still work
    await bot.process_commands(message)


# ------------------------
# Commands
# ------------------------


@bot.command(name="leaderboard")
async def leaderboard_cmd(
    ctx: commands.Context, period: str = "day", date_str: str | None = None
):
    """
    !leaderboard [day|week|month] [YYYY-MM-DD]
    If date is omitted, uses today in Central Time.
    """
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return

    period = period.lower()
    if period not in {
        "day",
        "week",
        "month",
        "today",
        "thisweek",
        "thismonth",
    }:
        await ctx.send(
            "❌ Invalid period. Use one of: `day`, `week`, `month`."
        )
        return

    if date_str:
        base_date = _parse_date(date_str)
        if not base_date:
            await ctx.send(
                "❌ Invalid date. Use format `YYYY-MM-DD` (example: `2026-02-06`)."
            )
            return
        base_dt_local = datetime(
            base_date.year,
            base_date.month,
            base_date.day,
            tzinfo=LOCAL_TZ,
        )
    else:
        base_dt_local = _now_local()

    (
        start_utc,
        end_utc,
        start_local,
        end_local,
        pretty_period,
    ) = _period_bounds(period, base_dt_local)

    deals = _filter_deals_period(ctx.guild.id, start_utc, end_utc)

    if period in {"day", "today"}:
        date_label = start_local.date().isoformat()
    elif period in {"month", "thismonth"}:
        date_label = start_local.date().strftime("%Y-%m")
    else:
        date_label = (
            f"{start_local.date().isoformat()} → "
            f"{(end_local - timedelta(days=1)).date().isoformat()}"
        )

    embed = _build_leaderboard_embed(
        ctx.guild,
        deals,
        pretty_period,
        date_label,
    )
    await ctx.send(embed=embed)


@bot.command(name="mystats")
async def mystats_cmd(ctx: commands.Context):
    """Very lightweight personal stats: how many deals you closed, and kW."""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return

    user_id = ctx.author.id
    deals = [
        d
        for d in _get_guild_deals(ctx.guild.id)
        if d.get("closer_id") == user_id and d.get("status") != "canceled"
    ]

    total_deals = len(deals)
    total_kw = sum(float(d.get("kw") or 0.0) for d in deals)
    solar_count, battery_count = _count_by_type(deals)

    embed = discord.Embed(
        title=f"📊 Stats for {ctx.author.display_name}",
        color=0x3498db,
    )
    embed.add_field(name="Deals Closed", value=str(total_deals), inline=True)
    embed.add_field(name="Total kW", value=f"{total_kw:.1f}", inline=True)
    if solar_count > 0:
        embed.add_field(name="☀️🔋 Solar + Battery", value=str(solar_count), inline=True)
    if battery_count > 0:
        embed.add_field(name="🔋 Battery Only", value=str(battery_count), inline=True)
    await ctx.send(embed=embed)


@bot.command(name="help", help="Show all solar leaderboard commands")
async def help_cmd(ctx: commands.Context):
    """Custom help command so reps can see all available actions."""
    embed = discord.Embed(
        title="☀️ Solar Leaderboard Bot – Commands",
        color=0x95a5a6,
        description=(
            "Log sales with hashtags in your **general chat**, "
            "use `!` commands for reports.\n"
            "_All leaderboards use Central Time._"
        ),
    )

    embed.add_field(
        name="Log a Sale (Solar + Battery)",
        value=(
            "`#sold @Setter kW`\n"
            "• Example: `#sold @Devin 6.5`\n"
            "• With customer: `#sold @Devin John Smith 6.5`"
        ),
        inline=False,
    )

    embed.add_field(
        name="Log a Battery-Only Sale",
        value=(
            "`#sold @Setter 0`\n"
            "• Use **0** for kW to mark as battery only\n"
            "• Example: `#sold @Devin 0`"
        ),
        inline=False,
    )

    embed.add_field(
        name="Cancel After Signing",
        value=(
            "`#cancel Customer Name`\n"
            "• Marks the **latest** deal for that customer as canceled\n"
            "• Canceled deals are **excluded** from leaderboards"
        ),
        inline=False,
    )

    embed.add_field(
        name="Delete a Deal (Admin/Manager)",
        value=(
            "`#delete Customer Name`\n"
            "• Hard-deletes the **latest** deal for that customer from stats"
        ),
        inline=False,
    )

    embed.add_field(
        name="Reset This Server's Stats (Admin/Manager)",
        value=(
            "`#clearleaderboard`\n"
            "• Wipes all deals for this server\n"
            "• Useful for fresh contests / new months"
        ),
        inline=False,
    )

    embed.add_field(
        name="View Leaderboards",
        value=(
            "`!leaderboard [day|week|month] [YYYY-MM-DD]`\n"
            "• `!leaderboard` → today\n"
            "• `!leaderboard week` → this week\n"
            "• `!leaderboard month` → this month\n"
            "• `!leaderboard day 2026-02-01` → specific past day\n"
            "• `!leaderboard week 2026-02-01` → week containing that date"
        ),
        inline=False,
    )

    embed.add_field(
        name="Your Personal Stats",
        value="`!mystats` – shows how many deals you closed and total kW.",
        inline=False,
    )

    embed.set_footer(
        text="Daily/weekly/monthly leaderboard channels are read-only – use #sold in your normal chat."
    )
    await ctx.send(embed=embed)


# ------------------------
# Run
# ------------------------

if __name__ == "__main__":
    token = os.getenv("DISCORD_BOT_TOKEN")
    if not token:
        print("Error: DISCORD_BOT_TOKEN environment variable is not set.")
    else:
        bot.run(token)
