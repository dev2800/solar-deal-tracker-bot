import os
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import discord
from discord.ext import commands

# --------------- Config -----------------

STATS_FILE = "sales_stats.json"

INTENTS = discord.Intents.default()
INTENTS.message_content = True
INTENTS.members = True
INTENTS.guilds = True

bot = commands.Bot(command_prefix="!", intents=INTENTS)

# --------------- Data layer -------------


@dataclass
class Sale:
    id: int
    guild_id: int
    timestamp: str  # ISO
    closer_id: int
    setter_id: Optional[int]  # may be None if they didn't @ mention
    setter_name: str
    kw: float


@dataclass
class SalesStore:
    next_id: int
    sales: List[Sale]


def _load_raw() -> Dict:
    if os.path.exists(STATS_FILE):
        try:
            with open(STATS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"next_id": 1, "sales": []}
    return {"next_id": 1, "sales": []}


def _save_raw(data: Dict) -> None:
    tmp = STATS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, STATS_FILE)


def load_store() -> SalesStore:
    raw = _load_raw()
    sales = [
        Sale(
            id=s["id"],
            guild_id=s["guild_id"],
            timestamp=s["timestamp"],
            closer_id=s["closer_id"],
            setter_id=s.get("setter_id"),
            setter_name=s["setter_name"],
            kw=float(s["kw"]),
        )
        for s in raw.get("sales", [])
    ]
    return SalesStore(next_id=raw.get("next_id", 1), sales=sales)


def save_store(store: SalesStore) -> None:
    raw = {
        "next_id": store.next_id,
        "sales": [asdict(s) for s in store.sales],
    }
    _save_raw(raw)


store = load_store()

# --------------- Helpers ----------------


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _start_of_today_utc() -> datetime:
    now = _now_utc()
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


def _start_of_week_utc() -> datetime:
    today = _start_of_today_utc()
    return today - timedelta(days=today.weekday())


def _start_of_month_utc() -> datetime:
    today = _start_of_today_utc()
    return today.replace(day=1)


async def ensure_leaderboard_channels(
    guild: discord.Guild,
) -> Dict[str, discord.TextChannel]:
    desired = {
        "daily-leaderboard": "Daily sales scoreboard",
        "weekly-leaderboard": "Weekly sales scoreboard",
        "monthly-leaderboard": "Monthly sales scoreboard",
    }

    existing = {ch.name: ch for ch in guild.text_channels}

    result: Dict[str, discord.TextChannel] = {}
    for name, topic in desired.items():
        if name in existing:
            result[name] = existing[name]
            continue

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(
                send_messages=False, view_channel=True
            ),
            guild.me: discord.PermissionOverwrite(
                send_messages=True,
                manage_messages=True,
                view_channel=True,
                embed_links=True,
            ),
        }

        try:
            ch = await guild.create_text_channel(
                name=name, overwrites=overwrites, topic=topic
            )
        except discord.Forbidden:
            # fallback: use system channel or any channel we can write in
            ch = guild.system_channel or next(
                (c for c in guild.text_channels
                 if c.permissions_for(guild.me).send_messages),
                None,
            )  # type: ignore

        if ch:
            result[name] = ch

    return result


def _aggregate_stats(
    guild: discord.Guild, since: Optional[datetime] = None
) -> Tuple[Dict[int, Dict], Dict[int, Dict], float, float]:
    """Return (closers, setters, total_deals, total_kw)."""
    global store
    closers: Dict[int, Dict] = {}
    setters: Dict[int, Dict] = {}
    total_deals = 0
    total_kw = 0.0

    for sale in store.sales:
        if sale.guild_id != guild.id:
            continue
        ts = datetime.fromisoformat(sale.timestamp)
        if since and ts < since:
            continue

        total_deals += 1
        total_kw += sale.kw

        # closer
        cstats = closers.setdefault(
            sale.closer_id,
            {"count": 0, "kw": 0.0},
        )
        cstats["count"] += 1
        cstats["kw"] += sale.kw

        # setter
        sstats = setters.setdefault(
            sale.setter_id or -1,
            {
                "count": 0,
                "kw": 0.0,
                "setter_name": sale.setter_name,
                "setter_id": sale.setter_id,
            },
        )
        sstats["count"] += 1
        sstats["kw"] += sale.kw

    return closers, setters, total_deals, total_kw


async def _get_user_mention(
    guild: discord.Guild, user_id: Optional[int], fallback_name: str
) -> str:
    if user_id is None or user_id == -1:
        return fallback_name
    member = guild.get_member(user_id)
    if member is None:
        try:
            member = await guild.fetch_member(user_id)
        except discord.NotFound:
            return fallback_name
    return member.mention


async def post_or_edit_scoreboard(
    channel: discord.TextChannel, content: str
) -> None:
    """Edit last scoreboard message from the bot, or send a new one."""
    last_bot_message: Optional[discord.Message] = None
    async for msg in channel.history(limit=20):
        if msg.author == bot.user and "Scoreboard" in (msg.content or ""):
            last_bot_message = msg
            break

    if last_bot_message:
        await last_bot_message.edit(content=content)
        msg = last_bot_message
    else:
        msg = await channel.send(content)

    # Try to pin once
    try:
        if not msg.pinned:
            await msg.pin(reason="Solar scoreboard auto-pin")
    except discord.Forbidden:
        pass


async def update_all_scoreboards(guild: discord.Guild) -> None:
    channels = await ensure_leaderboard_channels(guild)

    ranges = {
        "daily": _start_of_today_utc(),
        "weekly": _start_of_week_utc(),
        "monthly": _start_of_month_utc(),
    }

    for key, since in ranges.items():
        ch = channels.get(f"{key}-leaderboard")
        if not ch:
            continue

        closers, setters, total_deals, total_kw = _aggregate_stats(guild, since)
        if total_deals == 0:
            text = (
                f"**{key.title()} Scoreboard 📊**\n"
                "No sales logged yet. Log one with `#sold @Setter kW` in your general channel."
            )
            await post_or_edit_scoreboard(ch, text)
            continue

        closer_lines: List[str] = []
        for uid, data in sorted(
            closers.items(), key=lambda kv: (-kv[1]["count"], -kv[1]["kw"])
        ):
            member = guild.get_member(uid)
            name = member.mention if member else f"<@{uid}>"
            closer_lines.append(f"{name} - {data['count']}")

        setter_lines: List[str] = []
        for uid, data in sorted(
            setters.items(), key=lambda kv: (-kv[1]["count"], -kv[1]["kw"])
        ):
            mention = await _get_user_mention(
                guild, data["setter_id"], data["setter_name"]
            )
            setter_lines.append(f"{mention} - {data['count']}")

        text = (
            f"**{key.title()} Blitz Scoreboard ⚡**\n"
            "Solar + Battery ☀️🔋\n\n"
            "**Closer:**\n"
            + ("\n".join(closer_lines) or "No data")
            + "\n\n**Setter:**\n"
            + ("\n".join(setter_lines) or "No data")
            + f"\n\n**Total Transactions Sold:** {int(total_deals)}"
            + f"\n**Total kW Sold:** {total_kw:.2f} kW"
            + "\n\n_Commands: type `#sold @Setter kW` in your general chat. "
            + "Use `!mystats` to see your own numbers._"
        )

        await post_or_edit_scoreboard(ch, text)


# --------------- Bot events & commands ---------


@bot.event
async def on_ready():
    print(f"{bot.user} has connected to Discord!")
    print(f"Bot is in {len(bot.guilds)} guild(s)")
    for guild in bot.guilds:
        try:
            await ensure_leaderboard_channels(guild)
            await update_all_scoreboards(guild)
        except Exception as e:
            print(f"Error updating scoreboards for guild {guild.id}: {e}")


@bot.event
async def on_guild_join(guild: discord.Guild):
    await ensure_leaderboard_channels(guild)
    await update_all_scoreboards(guild)


def parse_sold_message(content: str) -> Optional[Tuple[str, float]]:
    """Parse '#sold Setter Name 5.2' returning (setter_str, kw)."""
    lowered = content.lower()
    if "#sold" not in lowered:
        return None

    parts = content.split()
    try:
        sold_index = next(
            i for i, p in enumerate(parts) if p.lower().startswith("#sold")
        )
    except StopIteration:
        return None

    if len(parts) - sold_index < 3:
        return None

    try:
        kw = float(parts[-1])
    except ValueError:
        return None

    setter_tokens = parts[sold_index + 1 : -1]
    if not setter_tokens:
        return None
    setter_str = " ".join(setter_tokens)
    return setter_str, kw


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    parsed = parse_sold_message(message.content)
    if parsed:
        setter_str, kw = parsed

        # Try to resolve setter mention
        setter_member: Optional[discord.Member] = None
        if message.mentions:
            setter_member = message.mentions[0]
            setter_name_for_storage = setter_member.display_name
            setter_id_for_storage: Optional[int] = setter_member.id
        else:
            setter_name_for_storage = setter_str
            setter_id_for_storage = None

        global store
        sale = Sale(
            id=store.next_id,
            guild_id=message.guild.id if message.guild else 0,
            timestamp=_now_utc().isoformat(),
            closer_id=message.author.id,
            setter_id=setter_id_for_storage,
            setter_name=setter_name_for_storage,
            kw=kw,
        )
        store.sales.append(sale)
        store.next_id += 1
        save_store(store)

        closer_mention = message.author.mention
        setter_display = (
            setter_member.mention if setter_member else setter_name_for_storage
        )

        embed = discord.Embed(
            title="🎉 DEAL CLOSED!",
            description=f"Deal for **{setter_display}** has been logged!",
            color=discord.Color.gold(),
            timestamp=_now_utc(),
        )
        embed.add_field(name="💼 Closer", value=closer_mention, inline=True)
        embed.add_field(name="📞 Setter", value=setter_display, inline=True)
        embed.add_field(name="⚡ System Size", value=f"{kw} kW", inline=True)

        await message.channel.send(embed=embed)

        if message.guild:
            await update_all_scoreboards(message.guild)

    # Let normal commands run too
    await bot.process_commands(message)


@bot.command(name="mystats", help="Show your personal stats as closer and setter")
async def my_stats(ctx: commands.Context):
    user_id = ctx.author.id
    guild = ctx.guild
    if not guild:
        await ctx.send("Run this in a server, not in DMs.")
        return

    total_closer = 0
    kw_closer = 0.0
    total_setter = 0
    kw_setter = 0.0

    for sale in store.sales:
        if sale.guild_id != guild.id:
            continue
        if sale.closer_id == user_id:
            total_closer += 1
            kw_closer += sale.kw
        if sale.setter_id == user_id:
            total_setter += 1
            kw_setter += sale.kw

    if total_closer == 0 and total_setter == 0:
        await ctx.send("No stats yet. Log a sale with `#sold @Setter kW`.")
        return

    lines: List[str] = [f"Stats for **{ctx.author.display_name}**"]
    if total_closer:
        lines.append(
            f"• As **Closer**: {total_closer} deals | {kw_closer:.2f} kW"
        )
    if total_setter:
        lines.append(
            f"• As **Setter**: {total_setter} deals | {kw_setter:.2f} kW"
        )

    await ctx.send("\n".join(lines))


@bot.command(name="help_solar", help="Show commands for the solar scoreboard bot")
async def help_solar(ctx: commands.Context):
    text = (
        "**Solar Scoreboard Bot – Commands**\n\n"
        "**Log a sale**\n"
        "`#sold @Setter kW` – example: `#sold @Devin 5.8`\n"
        "Type this in your standard sales/general chat. The bot will:\n"
        "• Log the sale (closer = who typed it)\n"
        "• Update daily / weekly / monthly scoreboards\n"
        "• Keep totals for kW and transactions\n\n"
        "**View your stats**\n"
        "`!mystats` – see your own deals as closer / setter.\n\n"
        "Scoreboards live in: `#daily-leaderboard`, `#weekly-leaderboard`, `#monthly-leaderboard`."
    )
    await ctx.send(text)


# --------------- Entrypoint --------------------


if __name__ == "__main__":
    token = os.getenv("DISCORD_BOT_TOKEN")
    if not token:
        print("Error: DISCORD_BOT_TOKEN environment variable not set!")
    else:
        bot.run(token)
