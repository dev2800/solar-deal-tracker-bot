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
    """Return current time in LOCAL_TZ."""
    return _now_utc().astimezone(LOCAL_TZ)


# ------------------------
# Data storage helpers
# ------------------------

DATA_FILE = "data/deals.json"


def _load_deals() -> list[dict]:
    """Load all deals from disk, oldest to newest."""
    if not os.path.exists(DATA_FILE):
        return []
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            deals = json.load(f)
    except json.JSONDecodeError:
        deals = []
    # Normalize: ensure timestamp is stored as ISO string
    # (We won't parse on load except when needed.)
    return deals


def _save_deals(deals: list[dict]) -> None:
    """Persist deals to disk."""
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(deals, f, ensure_ascii=False, indent=2)


def _add_deal(deal: dict) -> None:
    """
    Append a single deal to the data file.
    Deal should already contain a UTC ISO8601 `timestamp`.
    """
    deals = _load_deals()
    deals.append(deal)
    _save_deals(deals)


# ------------------------
# Deal helpers
# ------------------------


def _deal_type(kw: float) -> str:
    """
    Return 'battery_only' if kw == 0, otherwise 'solar_battery'.
    """
    return "battery_only" if kw == 0 else "solar_battery"


def _aggregate_by_person(deals: list[dict], key: str):
    """
    Aggregate deals by given key ("closer" or "setter").
    Returns a list of dicts sorted by deal_count desc, then name asc:
      [{"name": str, "deals": int, "kw": float}, ...]
    """
    from collections import defaultdict

    counts = defaultdict(lambda: {"deals": 0, "kw": 0.0})
    for d in deals:
        name = d.get(key)
        if not name:
            continue
        counts[name]["deals"] += 1
        try:
            kw = float(d.get("kw") or 0.0)
        except (TypeError, ValueError):
            kw = 0.0
        counts[name]["kw"] += kw

    rows = []
    for name, agg in counts.items():
        rows.append({"name": name, "deals": agg["deals"], "kw": agg["kw"]})
    rows.sort(key=lambda r: (-r["deals"], r["name"].lower()))
    return rows


def _aggregate_by_closer(deals: list[dict]):
    return _aggregate_by_person(deals, "closer")


def _aggregate_by_setter(deals: list[dict]):
    return _aggregate_by_person(deals, "setter")


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


def _split_deals_by_type(deals: list[dict]):
    """Split deals into (solar_battery_deals, battery_only_deals)."""
    solar_deals = []
    battery_deals = []
    for d in deals:
        dtype = d.get("deal_type")
        if dtype is None:
            dtype = _deal_type(float(d.get("kw") or 0.0))
        if dtype == "battery_only":
            battery_deals.append(d)
        else:
            solar_deals.append(d)
    return solar_deals, battery_deals


def _period_bounds(kind: str, base_dt: datetime):
    """
    Given kind in {"day","week","month"} and a timezone-aware datetime,
    treat it in LOCAL_TZ and return:
      (start_utc, end_utc, start_local, end_local, pretty_kind)
    where boundaries are midnight LOCAL_TZ.
    """
    kind = kind.lower()
    local = base_dt.astimezone(LOCAL_TZ)
    if kind == "day":
        start_local = local.replace(hour=0, minute=0, second=0, microsecond=0)
        end_local = start_local + timedelta(days=1)
        pretty = "Daily Blitz Scoreboard"
    elif kind == "week":
        # Monday start-of-week
        start_local = local - timedelta(days=local.weekday())
        start_local = start_local.replace(hour=0, minute=0, second=0, microsecond=0)
        end_local = start_local + timedelta(days=7)
        pretty = "Weekly Blitz Scoreboard"
    elif kind == "month":
        start_local = local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        # Next month
        if start_local.month == 12:
            end_local = start_local.replace(year=start_local.year + 1, month=1)
        else:
            end_local = start_local.replace(month=start_local.month + 1)
        pretty = "Monthly Blitz Scoreboard"
    else:
        raise ValueError(f"Unknown period kind: {kind}")

    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)
    return start_utc, end_utc, start_local, end_local, pretty


def _filter_deals_by_period(deals: list[dict], kind: str, base_dt: datetime):
    """
    Return only deals with timestamp in the given period, plus labels.
    """
    start_utc, end_utc, start_local, end_local, pretty = _period_bounds(kind, base_dt)

    def _parse_ts(d):
        ts = d.get("timestamp")
        if not ts:
            return None
        try:
            return datetime.fromisoformat(ts)
        except Exception:
            return None

    filtered = []
    for d in deals:
        dt = _parse_ts(d)
        if dt is None:
            continue
        if start_utc <= dt < end_utc:
            filtered.append(d)

    # For date label we show local period
    if kind == "day":
        label = start_local.strftime("%Y-%m-%d")
    elif kind == "week":
        label = f"{start_local:%Y-%m-%d} to {end_local - timedelta(days=1):%Y-%m-%d}"
    else:  # month
        label = start_local.strftime("%Y-%m")

    return filtered, pretty, label


# ------------------------
# Discord bot setup
# ------------------------

intents = discord.Intents.default()
intents.message_content = True
intents.members = True  # If we need to resolve mentions to names later

bot = commands.Bot(command_prefix="!", intents=intents)


# ------------------------
# Formatting helpers
# ------------------------


def _format_user_display(user: discord.abc.User) -> str:
    """
    Return a display string for a user, preferring nicknames where possible.
    """
    if isinstance(user, discord.Member) and user.nick:
        return user.nick
    return user.name


# ------------------------
# Leaderboard content (plain text message)
# ------------------------


def _build_leaderboard_content(
    deals: list[dict],
    period_label: str,
    date_label: str,
) -> str:
    """
    Build a plain-text leaderboard message with two sections:
    1) Solar + Battery
    2) Battery Only
    matching the 'ideal leaderboard' format.
    """
    lines: list[str] = []

    # Header
    lines.append(f"**{period_label}** ⚡")
    # If you want the date visible, uncomment:
    # lines.append(f"_Period: {date_label}_")
    lines.append("")

    if not deals:
        lines.append("_No deals yet — be the first to log a sale with `#sold`!_")
        return "\n".join(lines)

    # Split deals into two buckets
    solar_deals, battery_deals = _split_deals_by_type(deals)
    solar_count = len(solar_deals)
    battery_count = len(battery_deals)

    # --- Solar + Battery section ---
    if solar_count > 0:
        lines.append("Solar + Battery ☀️🔋")
        lines.append("")
        lines.append("**Closer:**")
        by_closer_solar = _aggregate_by_closer(solar_deals)
        for row in by_closer_solar:
            lines.append(f"{row['name']} - {row['deals']}")
        lines.append("")

        by_setter_solar = _aggregate_by_setter(solar_deals)
        if by_setter_solar:
            lines.append("**Setter:**")
            for row in by_setter_solar:
                lines.append(f"{row['name']} - {row['deals']}")
            lines.append("")

    # --- Battery Only section ---
    if battery_count > 0:
        if solar_count > 0:
            lines.append("")  # blank line between sections
        lines.append("Battery Only 🟩")
        lines.append("")
        lines.append("**Closer:**")
        by_closer_battery = _aggregate_by_closer(battery_deals)
        for row in by_closer_battery:
            lines.append(f"{row['name']} - {row['deals']}")
        lines.append("")

        by_setter_battery = _aggregate_by_setter(battery_deals)
        if by_setter_battery:
            lines.append("**Setter:**")
            for row in by_setter_battery:
                lines.append(f"{row['name']} - {row['deals']}")
            lines.append("")

    # --- Totals ---
    total_deals = len(deals)
    total_kw = sum(float(d.get("kw") or 0.0) for d in deals)

    lines.append(f"**Total Transactions Sold:** {total_deals}")
    lines.append(f"**Total kW Sold:** {total_kw:.2f} kW")

    if battery_count > 0:
        lines.append(f"**Battery Only Deals:** {battery_count}")
    if solar_count > 0:
        lines.append(f"**Solar + Battery Deals:** {solar_count}")

    lines.append("")
    lines.append(
        "_Commands: type `#sold @Setter kW` in your general chat. "
        "Use `!mystats` to see your own numbers._"
    )

    return "\n".join(lines)


def _build_leaderboard_embed(
    deals: list[dict],
    period_label: str,
    date_label: str,
    channel_name: str,
) -> discord.Embed:
    """
    Optional richer embed for an admin-only command if we want it.
    (Right now your main scoreboard is plain text in a specific channel,
    but this can be used for DM summaries or an admin dashboard.)
    """
    total_deals = len(deals)
    total_kw = sum(float(d.get("kw") or 0.0) for d in deals)
    solar_count, battery_count = _count_by_type(deals)

    desc = [
        f"**Period:** {date_label}",
        f"**Total Transactions Sold:** {total_deals}",
        f"**Total kW Sold:** {total_kw:.2f} kW",
        "",
        f"Solar + Battery Deals: {solar_count}",
        f"Battery Only Deals: {battery_count}",
    ]
    embed = discord.Embed(
        title=f"{period_label} – {channel_name}",
        description="\n".join(desc),
        color=discord.Color.gold(),
    )

    medals = ["🥇", "🥈", "🥉"]

    # Closers
    by_closer = _aggregate_by_closer(deals)
    if by_closer:
        closer_lines = []
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

    return embed


# ------------------------
# Bot commands
# ------------------------


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")
    print("------")


@bot.command(name="sold", help="Log a closed deal. Usage: #sold @Setter kW [Customer Name (optional)]")
async def sold(ctx: commands.Context, setter: discord.Member, kw: float = 0.0, *, customer: str = ""):
    """
    Command to log a closed deal.
    Example:
      #sold @SetterName 6.5 John Smith
      #sold @SetterName 0 John Smith    (battery-only)
    """
    closer_name = _format_user_display(ctx.author)
    setter_name = _format_user_display(setter)
    deal_id = None

    # Build deal record
    deal = {
        "id": None,  # Will assign after load
        "timestamp": _now_utc().isoformat(),
        "closer": closer_name,
        "setter": setter_name,
        "kw": kw,
        "deal_type": _deal_type(kw),
        "customer": customer,
        "channel_id": ctx.channel.id,
        "guild_id": ctx.guild.id if ctx.guild else None,
    }

    # Persist
    deals = _load_deals()
    deal_id = len(deals) + 1
    deal["id"] = deal_id
    deals.append(deal)
    _save_deals(deals)

    # Build confirmation message
    embed = discord.Embed(
        title="🎉 DEAL CLOSED!",
        description=f"Deal for {setter.mention} has been logged!",
        color=discord.Color.green(),
    )
    embed.add_field(name="Closer", value=closer_name, inline=True)
    embed.add_field(name="Setter", value=setter_name, inline=True)
    embed.add_field(name="⚡ System Size", value=f"{kw:.1f} kW", inline=True)

    deal_type_label = "Battery Only 🟩" if kw == 0 else "Solar + Battery ☀️🔋"
    embed.add_field(name="Type", value=deal_type_label, inline=True)
    embed.add_field(
        name="Customer",
        value=customer or "_(no name given)_",
        inline=True,
    )
    embed.set_footer(text=f"Deal ID: {deal_id} · Logged via #sold")

    await ctx.send(embed=embed)


@bot.command(name="mystats", help="Show your own deal stats for today, this week, and this month.")
async def mystats(ctx: commands.Context):
    user_name = _format_user_display(ctx.author)
    deals = _load_deals()

    now = _now_utc()
    periods = ["day", "week", "month"]
    lines = [f"**Stats for {user_name}:**"]

    for kind in periods:
        period_deals, pretty, label = _filter_deals_by_period(deals, kind, now)
        my_deals = [d for d in period_deals if d.get("closer") == user_name or d.get("setter") == user_name]
        total = len(my_deals)
        if total == 0:
            lines.append(f"- {pretty} ({label}): 0 deals")
            continue
        kw_total = sum(float(d.get("kw") or 0.0) for d in my_deals)
        lines.append(
            f"- {pretty} ({label}): {total} deal(s), {kw_total:.1f} kW"
        )

    await ctx.send("\n".join(lines))


@bot.command(
    name="leaderboard",
    help="Post the daily/weekly/monthly blitz scoreboard into the configured channels.",
)
@commands.has_permissions(administrator=True)
async def leaderboard(ctx: commands.Context):
    """
    Admin-only command that:
      - Calculates daily / weekly / monthly stats
      - Posts them to specific channels (if they exist)
    """
    guild = ctx.guild
    if guild is None:
        await ctx.send("This command can only be used in a server.")
        return

    deals = _load_deals()
    now = _now_utc()

    # Mapping of period -> channel_name
    targets = {
        "day": "daily-leaderboard",
        "week": "weekly-leaderboard",
        "month": "monthly-leaderboard",
    }

    for kind, channel_name in targets.items():
        channel = discord.utils.get(guild.text_channels, name=channel_name)
        if not channel:
            continue

        period_deals, pretty, label = _filter_deals_by_period(deals, kind, now)
        content = _build_leaderboard_content(period_deals, pretty, label)

        # Clear previous bot messages in that channel (optional/soft)
        # We won't delete in this simple version; just append.
        await channel.send(content)

    await ctx.send("Leaderboards posted.")


@bot.command(
    name="setleaderboards",
    help="(Optional) Explain to reps how to use the #sold and !mystats commands.",
)
async def setleaderboards(ctx: commands.Context):
    """
    Simple helper message you can pin in your general chat.
    """
    embed = discord.Embed(
        title="How to Use the Solar Tracker Bot",
        description=(
            "• Log a closed deal with: `#sold @Setter kW [Customer Name]`\n"
            "  - Use `0` for kW if it's **battery only**.\n"
            "• See your stats with: `!mystats`\n"
            "• Leaderboards will auto-update when an admin runs `!leaderboard`."
        ),
        color=discord.Color.blurple(),
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
