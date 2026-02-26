import os
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

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

LEADERBOARD_CHANNELS = {
    "daily-leaderboard": "Daily sales leaderboard (read-only)",
    "weekly-leaderboard": "Weekly sales leaderboard (read-only)",
    "monthly-leaderboard": "Monthly sales leaderboard (read-only)",
}

# ------------------------
# Helpers
# ------------------------


def _deal_type(kw: float) -> str:
    return "battery_only" if kw == 0.0 else "solar_battery"


def _deal_type_label(dtype: str) -> str:
    if dtype == "battery_only":
        return "Battery Only 🔋"
    return "Solar + Battery ☀️🔋"


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


def _get_user_deals(guild_id: int, user_id: int, user_name: str):
    """
    Get all deals where user is the closer OR the setter.
    Matches by ID first, then falls back to name matching for setters logged without @mention.
    """
    deals = []
    user_name_lower = user_name.lower().strip()
    
    for d in _get_guild_deals(guild_id):
        if d.get("status") in ("canceled", "deleted"):
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


def _aggregate_by_role(deals: list[dict], role: str):
    """
    Aggregate deals by closer or setter.
    role = 'closer' or 'setter'
    Returns list of {id, name, deals, kw} sorted by deals desc.
    """
    stats: dict[str, dict] = {}
    for d in deals:
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
    """Split deals into solar_battery and battery_only lists."""
    solar = []
    battery = []
    for d in deals:
        dtype = d.get("deal_type")
        if dtype is None:
            dtype = _deal_type(float(d.get("kw") or 0.0))
        if dtype == "battery_only":
            battery.append(d)
        else:
            solar.append(d)
    return solar, battery


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
    solar_deals, battery_deals = _split_by_type(deals)

    lines = []
    lines.append(f"{period_label} ⚡")
    lines.append("")

    if not deals:
        lines.append("_No deals yet — be the first to log a sale with `#sold`!_")
        return "\n".join(lines)

    # --- Solar + Battery section ---
    if solar_deals:
        lines.append("Solar + Battery ☀️🔋")
        lines.append("")

        closer_lines = _build_section_lines(solar_deals, "closer", show_kw=True)
        if closer_lines:
            lines.extend(closer_lines)
            lines.append("")

        setter_lines = _build_section_lines(solar_deals, "setter", show_kw=True)
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
    total_deals = len(deals)
    total_kw = sum(float(d.get("kw") or 0.0) for d in deals)

    lines.append(f"**Total Transactions Sold:** {total_deals}")
    lines.append(f"**Total kW Sold:** {total_kw:.2f} kW")
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

    if not deals:
        embed.add_field(
            name="No deals yet",
            value="Be the first to log a sale with `#sold`!",
            inline=False,
        )
        return embed

    solar_deals, battery_deals = _split_by_type(deals)
    medals = ["🥇", "🥈", "🥉"]

    def _role_lines(deal_list, role):
        agg = _aggregate_by_role(deal_list, role)
        out = []
        for idx, row in enumerate(agg[:10]):
            icon = medals[idx] if idx < len(medals) else f"{idx+1}."
            display = _display_name(row["id"], row["name"], use_mention=use_mentions)
            out.append(f"{icon} {display} – {row['deals']} deal(s), {row['kw']:.1f} kW")
        return "\n".join(out)

    if solar_deals:
        cl = _role_lines(solar_deals, "closer")
        if cl:
            embed.add_field(name="☀️🔋 Solar+Battery — Closers", value=cl, inline=False)
        sl = _role_lines(solar_deals, "setter")
        if sl:
            embed.add_field(name="☀️🔋 Solar+Battery — Setters", value=sl, inline=False)

    if battery_deals:
        cl = _role_lines(battery_deals, "closer")
        if cl:
            embed.add_field(name="🔋 Battery Only — Closers", value=cl, inline=False)
        sl = _role_lines(battery_deals, "setter")
        if sl:
            embed.add_field(name="🔋 Battery Only — Setters", value=sl, inline=False)

    total_deals = len(deals)
    total_kw = sum(float(d.get("kw") or 0.0) for d in deals)
    embed.add_field(
        name="Totals",
        value=(
            f"💼 **Deals:** {total_deals}\n"
            f"⚡ **kW:** {total_kw:.1f}\n"
            f"☀️🔋 Solar+Battery: {len(solar_deals)}  •  🔋 Battery Only: {len(battery_deals)}"
        ),
        inline=False,
    )
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
    # #sold @Setter kW
    # #sold @Setter Customer Name kW
    # ----------------------------------------------------------------
    if lower.startswith("#sold") and not lower.startswith("#soldfor"):
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
                customer_name = " ".join(customer_tokens) if customer_tokens else None
                setter_id = setter_member.id
                setter_name = setter_member.display_name
            else:
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

            dtype_label = _deal_type_label(deal["deal_type"])

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
            if customer_name and customer_name != "N/A":
                embed.add_field(name="Customer", value=deal["customer_name"], inline=True)
            embed.set_footer(text=f"Deal #{deal['id']}")

            await message.channel.send(embed=embed)
            await _post_today_leaderboards(message.guild)

        except ValueError:
            await message.channel.send(
                "❌ Invalid `#sold` format.\n"
                "Use: `#sold @Setter kW`\n"
                "Example: `#sold @Devin 6.5`\n"
                "Battery only: `#sold @Devin 0`"
            )
        except Exception as e:
            await message.channel.send(f"❌ Error processing sale: {e}")
        return

    # ----------------------------------------------------------------
    # #soldfor @Closer @Setter kW   (admin only — log deal for someone else)
    # #soldfor @Closer @Setter Customer Name kW
    # ----------------------------------------------------------------
    if lower.startswith("#soldfor"):
        if not _is_admin_or_manager(message.author):
            await message.channel.send("⛔ Only admins or managers can use `#soldfor`.")
            return

        try:
            parts = content.split()
            # Need at least: #soldfor @Closer @Setter kW
            if len(parts) < 4:
                raise ValueError

            mentions = message.mentions
            if len(mentions) < 2:
                raise ValueError("Need two @mentions: closer and setter")

            # Find the mention tokens in order
            mention_tokens = [p for p in parts if p.startswith("<@") and p.endswith(">")]
            if len(mention_tokens) < 2:
                raise ValueError

            closer_member = mentions[0]
            setter_member = mentions[1]

            # Find position after second mention
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
            )

            dtype_label = _deal_type_label(deal["deal_type"])

            # Deal confirmation DOES use @mentions
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
            if customer_name and customer_name != "N/A":
                embed.add_field(name="Customer", value=deal["customer_name"], inline=True)
            embed.set_footer(text=f"Deal #{deal['id']}")

            await message.channel.send(embed=embed)
            await _post_today_leaderboards(message.guild)

        except ValueError:
            await message.channel.send(
                "❌ Invalid `#soldfor` format.\n"
                "Use: `#soldfor @Closer @Setter kW`\n"
                "Example: `#soldfor @Ethen @Devin 6.5`\n"
                "With customer: `#soldfor @Ethen @Devin John Smith 6.5`\n"
                "Battery only: `#soldfor @Ethen @Devin 0`"
            )
        except Exception as e:
            await message.channel.send(f"❌ Error processing sale: {e}")
        return

    # ----------------------------------------------------------------
    # #cancel Customer Name
    # ----------------------------------------------------------------
    if lower.startswith("#cancel"):
        try:
            parts = content.split(maxsplit=1)
            if len(parts) < 2:
                raise ValueError
            customer_name = parts[1].strip()
            deal = _find_latest_deal_by_customer(message.guild.id, customer_name)
            if not deal:
                await message.channel.send(f"❌ No deal found for customer `{customer_name}`.")
                return

            if deal.get("status") == "canceled":
                await message.channel.send(f"ℹ️ Latest deal for `{customer_name}` is already canceled.")
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
                value=_display_name(deal.get("closer_id"), deal.get("closer_name", "Unknown")),
                inline=True,
            )
            if deal.get("setter_name"):
                embed.add_field(
                    name="Setter",
                    value=_display_name(deal.get("setter_id"), deal["setter_name"]),
                    inline=True,
                )
            embed.add_field(name="System Size", value=f"{deal['kw']:.1f} kW", inline=True)
            embed.set_footer(text=f"Deal #{deal['id']}")
            await message.channel.send(embed=embed)
            await _post_today_leaderboards(message.guild)

        except ValueError:
            await message.channel.send("❌ Use: `#cancel Customer Name`")
        except Exception as e:
            await message.channel.send(f"❌ Error: {e}")
        return

    # ----------------------------------------------------------------
    # #delete <ID>  or  #delete Customer Name   (admin/manager only)
    # ----------------------------------------------------------------
    if lower.startswith("#delete"):
        if not _is_admin_or_manager(message.author):
            await message.channel.send("⛔ Only admins or managers can delete deals.")
            return

        try:
            parts = content.split(maxsplit=1)
            if len(parts) < 2:
                raise ValueError
            target = parts[1].strip()

            deal = None
            # Try to parse as deal ID first
            try:
                deal_id = int(target)
                deal = _find_deal_by_id(message.guild.id, deal_id)
                if not deal:
                    await message.channel.send(f"❌ No deal found with ID `{deal_id}`.")
                    return
            except (ValueError, TypeError):
                # Not a number — treat as customer name
                deal = _find_latest_deal_by_customer(message.guild.id, target)
                if not deal:
                    await message.channel.send(f"❌ No deal found for `{target}`.")
                    return

            deal_info = (
                f"Deal #{deal['id']} — "
                f"Closer: {deal.get('closer_name', '?')}, "
                f"Setter: {deal.get('setter_name', '?')}, "
                f"{deal['kw']:.1f} kW"
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
# Slash-style ! commands
# ---------------------------------------------------------------


@bot.command(name="deals")
async def deals_cmd(ctx: commands.Context, period: str = "day", date_str: str | None = None):
    """
    !deals [day|week|month] [YYYY-MM-DD]
    List all deals with their IDs so admins can reference/delete them.
    """
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

    # Build a compact table
    lines = [f"**{pretty}** — {date_label}\n"]
    lines.append("`ID  | Type     | Closer         | Setter         | kW    | Status`")
    lines.append("`----|----------|----------------|----------------|-------|--------`")

    for d in guild_deals:
        did = d["id"]
        dtype = "Solar" if d.get("deal_type", "solar_battery") == "solar_battery" else "Batt"
        closer = (d.get("closer_name") or "?")[:14]
        setter = (d.get("setter_name") or "?")[:14]
        kw = f"{d['kw']:.1f}"
        status = d.get("status", "closed")
        status_short = {"closed": "✅", "canceled": "❌", "deleted": "🗑️"}.get(status, status)
        lines.append(f"`{did:<4}| {dtype:<8} | {closer:<14} | {setter:<14} | {kw:<5} | {status_short}`")

    # Discord messages have a 2000 char limit
    msg = "\n".join(lines)
    if len(msg) > 1900:
        # Send in chunks
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
    """
    !leaderboard [day|week|month] [YYYY-MM-DD]
    Admin only - shows @mentions for everyone on the board.
    """
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return

    # Admin only
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

    # Always use @mentions since only admins can use this command
    embed = _build_leaderboard_embed(ctx.guild, deals, pretty, date_label, use_mentions=True)
    await ctx.send(embed=embed)


@bot.command(name="mystats")
async def mystats_cmd(ctx: commands.Context, period: str = "alltime"):
    """
    !mystats [day|week|month|alltime]
    Shows stats where user is closer OR setter.
    Works for both closers and setters!
    """
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
        # All time stats
        deals = _get_user_deals(ctx.guild.id, user_id, user_name)
        period_label = "All Time"
    else:
        # Period-based stats
        base_dt = _now_local()
        start_utc, end_utc, start_local, end_local, _ = _period_bounds(period, base_dt)
        deals = _get_user_deals_period(ctx.guild.id, user_id, user_name, start_utc, end_utc)

        if period in ("day", "today"):
            period_label = f"Today ({start_local.date().isoformat()})"
        elif period in ("week", "thisweek"):
            period_label = f"This Week ({start_local.date()} → {(end_local - timedelta(days=1)).date()})"
        else:  # month
            period_label = f"This Month ({start_local.strftime('%Y-%m')})"

    total_deals = len(deals)
    total_kw = sum(float(d.get("kw") or 0.0) for d in deals)
    solar_deals, battery_deals = _split_by_type(deals)

    # Count deals where user was closer vs setter
    closer_deals = [d for d in deals if d.get("closer_id") == user_id]
    setter_deals = [d for d in deals if d.get("setter_id") == user_id or
                   (d.get("setter_name", "").lower().strip() == user_name.lower().strip() and d.get("closer_id") != user_id)]

    embed = discord.Embed(
        title=f"📊 Stats for {ctx.author.display_name}",
        description=f"**{period_label}**",
        color=0x3498db,
    )
    embed.add_field(name="Total Deals", value=str(total_deals), inline=True)
    embed.add_field(name="Total kW", value=f"{total_kw:.1f}", inline=True)
    embed.add_field(name="\u200b", value="\u200b", inline=True)  # Spacer

    if closer_deals:
        closer_kw = sum(float(d.get("kw") or 0.0) for d in closer_deals)
        embed.add_field(name="💼 As Closer", value=f"{len(closer_deals)} deals ({closer_kw:.1f} kW)", inline=True)

    if setter_deals:
        setter_kw = sum(float(d.get("kw") or 0.0) for d in setter_deals)
        embed.add_field(name="📋 As Setter", value=f"{len(setter_deals)} deals ({setter_kw:.1f} kW)", inline=True)

    if solar_deals:
        embed.add_field(name="☀️🔋 Solar+Battery", value=str(len(solar_deals)), inline=True)
    if battery_deals:
        embed.add_field(name="🔋 Battery Only", value=str(len(battery_deals)), inline=True)

    embed.set_footer(text="Usage: !mystats [day|week|month|alltime]")
    await ctx.send(embed=embed)


@bot.command(name="help")
async def help_cmd(ctx: commands.Context):
    embed = discord.Embed(
        title="☀️ Solar Leaderboard Bot – Commands",
        color=0x95a5a6,
        description=(
            "Log sales with hashtags in **general chat**, "
            "use `!` commands for reports.\n"
            "_All times in Central Time._"
        ),
    )

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

    embed.add_field(
        name="Admin: Log for Someone Else",
        value=(
            "`#soldfor @Closer @Setter kW`\n"
            "• Example: `#soldfor @Ethen @Devin 6.5`"
        ),
        inline=False,
    )

    embed.add_field(
        name="Cancel / Delete",
        value=(
            "`#cancel Customer Name` — mark as canceled\n"
            "`#delete <DealID>` — delete by deal number\n"
            "`#delete Customer Name` — delete by customer name\n"
            "• Use `!deals` to see all deal IDs"
        ),
        inline=False,
    )

    embed.add_field(
        name="View Deals & Leaderboards",
        value=(
            "`!deals [day|week|month|all]` — list deals with IDs\n"
            "`!leaderboard [day|week|month] [YYYY-MM-DD]`\n"
            "`!mystats` — your all-time stats\n"
            "`!mystats day` — today's stats\n"
            "`!mystats week` — this week's stats\n"
            "`!mystats month` — this month's stats"
        ),
        inline=False,
    )

    embed.add_field(
        name="Admin: Reset",
        value="`#clearleaderboard` — wipes all deals for this server",
        inline=False,
    )

    embed.set_footer(text="Leaderboard channels are read-only – use #sold in your normal chat.")
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
