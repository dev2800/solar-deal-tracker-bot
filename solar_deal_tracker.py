import discord
from discord.ext import commands, tasks
import json
import os
from datetime import datetime, timedelta
from types import SimpleNamespace  # for fake ctx when posting leaderboards

# Bot setup with intents
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix='!', intents=intents)

# File to store deal data
DEALS_FILE = 'deals_data.json'
LEADERBOARD_FILE = 'leaderboard_data.json'


# ---------- DATA LOAD / SAVE ----------

def load_deals():
    if os.path.exists(DEALS_FILE):
        with open(DEALS_FILE, 'r') as f:
            return json.load(f)
    return {'deals': {}, 'next_id': 1000}


def load_leaderboard():
    if os.path.exists(LEADERBOARD_FILE):
        with open(LEADERBOARD_FILE, 'r') as f:
            return json.load(f)
    return {'setters': {}, 'closers': {}}


def save_deals(data):
    with open(DEALS_FILE, 'w') as f:
        json.dump(data, f, indent=2)


def save_leaderboard(data):
    with open(LEADERBOARD_FILE, 'w') as f:
        json.dump(data, f, indent=2)


# Initialize data
deals_data = load_deals()
leaderboard_data = load_leaderboard()


def normalize_customer_name(text: str) -> str:
    """Normalize customer name for consistent lookup."""
    return " ".join(text.split()).strip().title()


def generate_deal_id():
    """Generate unique internal deal ID"""
    deal_id = deals_data['next_id']
    deals_data['next_id'] += 1
    save_deals(deals_data)
    return deal_id


def get_display_name(user: discord.abc.User) -> str:
    """Get a human-friendly display name (nickname if available)."""
    return getattr(user, "display_name", user.name)


def update_setter_stats(user_id, username):
    """Update or create setter stats"""
    if user_id not in leaderboard_data['setters']:
        leaderboard_data['setters'][user_id] = {
            'username': username,
            'appointments_set': 0,
            'appointments_closed': 0,
            'total_kw': 0.0,
            'deals': []
        }
    else:
        leaderboard_data['setters'][user_id]['username'] = username


def update_closer_stats(user_id, username):
    """Update or create closer stats, including streak fields."""
    if user_id not in leaderboard_data['closers']:
        leaderboard_data['closers'][user_id] = {
            'username': username,
            'deals_closed': 0,
            'total_kw': 0.0,
            'total_revenue': 0.0,
            'deals': [],
            'current_streak_days': 0,
            'best_streak_days': 0,
            'last_closed_date': None
        }
    else:
        leaderboard_data['closers'][user_id]['username'] = username
        # Ensure streak fields exist for older data
        closer = leaderboard_data['closers'][user_id]
        closer.setdefault('current_streak_days', 0)
        closer.setdefault('best_streak_days', 0)
        closer.setdefault('last_closed_date', None)


async def safe_dm(user: discord.abc.User, content: str):
    """Safely DM a user; ignore if DMs are closed."""
    if not user:
        return
    try:
        await user.send(content)
    except discord.Forbidden:
        pass
    except Exception as e:
        print(f"DM error: {e}")


# ---------- LEADERBOARD CHANNEL HELPERS ----------

async def setup_leaderboard_channels(guild: discord.Guild):
    """
    Create daily/weekly/monthly leaderboard channels with locked perms
    if they don't already exist.
    """
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=False,
            add_reactions=False,
        ),
        guild.me: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            add_reactions=True,
            manage_messages=True,
        ),
    }

    async def get_or_create(name: str):
        channel = discord.utils.get(guild.text_channels, name=name)
        if channel is None:
            channel = await guild.create_text_channel(
                name,
                overwrites=overwrites,
                reason="Create solar leaderboard channel",
            )
        return channel

    await get_or_create("daily-leaderboard")
    await get_or_create("weekly-leaderboard")
    await get_or_create("monthly-leaderboard")


def get_leaderboard_channels(guild: discord.Guild):
    """Return the daily/weekly/monthly leaderboard channels (may be None)."""
    daily = discord.utils.get(guild.text_channels, name="daily-leaderboard")
    weekly = discord.utils.get(guild.text_channels, name="weekly-leaderboard")
    monthly = discord.utils.get(guild.text_channels, name="monthly-leaderboard")
    return daily, weekly, monthly


async def post_leaderboard_to_channel(channel: discord.TextChannel, timeframe: str):
    """
    Reuse the existing !leaderboard logic to post into any channel
    by faking a minimal ctx object.
    """
    dummy_ctx = SimpleNamespace(
        channel=channel,
        author=channel.guild.me,
        send=channel.send,
    )
    await show_leaderboard(dummy_ctx, timeframe=timeframe)


async def update_leaderboards_for_guild(guild: discord.Guild):
    """
    Refresh daily/weekly/monthly leaderboard channels for a guild.
    Called after sets/closes and when joining a new guild.
    """
    daily, weekly, monthly = get_leaderboard_channels(guild)

    async def clear_and_post(channel, timeframe):
        try:
            await channel.purge(limit=10)
        except discord.Forbidden:
            print(f"No permission to purge messages in {channel.name}")
        except discord.HTTPException as e:
            print(f"Failed to purge messages in {channel.name}: {e}")
        await post_leaderboard_to_channel(channel, timeframe)

    if daily:
        await clear_and_post(daily, "today")
    if weekly:
        await clear_and_post(weekly, "week")
    if monthly:
        await clear_and_post(monthly, "month")


# ---------- EVENTS ----------

@bot.event
async def on_ready():
    print(f'{bot.user} has connected to Discord!')
    print(f'Bot is in {len(bot.guilds)} guild(s)')

    # Ensure every guild has the leaderboard channels and initial boards
    for guild in bot.guilds:
        try:
            await setup_leaderboard_channels(guild)
            await update_leaderboards_for_guild(guild)
        except Exception as e:
            print(f"Error setting up leaderboards for guild {guild.name}: {e}")

    # Start the daily reset task (currently a no-op)
    if not daily_stats_reset.is_running():
        daily_stats_reset.start()


@bot.event
async def on_guild_join(guild):
    """When bot is invited to a new server, set up the channels automatically."""
    print(f"Joined new guild: {guild.name} ({guild.id})")
    try:
        await setup_leaderboard_channels(guild)
        await update_leaderboards_for_guild(guild)
    except Exception as e:
        print(f"Error during guild join setup for {guild.name}: {e}")


@bot.event
async def on_message(message):
    # Ignore bot's own messages
    if message.author.bot:
        return

    content_lower = message.content.lower()

    # APPOINTMENT SET workflow - using customer name and optional assigned closer
    if '#appointmentset' in content_lower or '#appointment set' in content_lower:
        raw_parts = message.content.split()
        customer_name_tokens = []

        # Build a set of mention tokens so we don't treat them as name words
        mention_tokens = set()
        for m in message.mentions:
            mention_tokens.add(f"<@{m.id}>")
            mention_tokens.add(f"<@!{m.id}>")

        # Find hashtag, then everything after it (excluding mentions / other hashtags) is name
        for i, part in enumerate(raw_parts):
            p = part.lower()
            if p.startswith('#appointmentset') or (p.startswith('#appointment') and 'set' in p):
                j = i + 1
                while j < len(raw_parts):
                    token = raw_parts[j]
                    if token in mention_tokens:
                        # skip mention from name
                        j += 1
                        continue
                    if token.startswith('#'):
                        break
                    customer_name_tokens.append(token)
                    j += 1
                break

        if not customer_name_tokens:
            await message.channel.send(
                "❌ I couldn't find the customer name.\n"
                "Use: `#appointmentset First Last [@CloserOptional]`\n"
                "Example: `#appointmentset John Smith @Mike`"
            )
            await bot.process_commands(message)
            return

        customer_raw = " ".join(customer_name_tokens)
        customer_key = normalize_customer_name(customer_raw)

        # Optional assigned closer = first non-bot user mention
        assigned_closer = None
        for m in message.mentions:
            if not m.bot:
                assigned_closer = m
                break

        # Check for existing open deal for this customer
        existing = deals_data['deals'].get(customer_key)
        if existing and existing['status'] == 'appointment_set':
            await message.channel.send(
                f"⚠️ There's already a pending appointment for **{customer_key}**.\n"
                "If you need to log another one, close or delete the first deal first."
            )
            await bot.process_commands(message)
            return

        # Generate unique internal deal ID
        internal_id = generate_deal_id()
        user_id = str(message.author.id)
        username = get_display_name(message.author)

        # Update setter stats
        update_setter_stats(user_id, username)
        leaderboard_data['setters'][user_id]['appointments_set'] += 1

        # Create deal record (keyed by customer name)
        deals_data['deals'][customer_key] = {
            'deal_id': internal_id,          # internal numeric ID
            'customer_name': customer_key,   # normalized customer name
            'setter_id': user_id,
            'setter_name': username,
            'assigned_closer_id': str(assigned_closer.id) if assigned_closer else None,
            'assigned_closer_name': get_display_name(assigned_closer) if assigned_closer else None,
            'status': 'appointment_set',
            'created_at': datetime.now().isoformat(),
            'closed_at': None,
            'closer_id': None,
            'closer_name': None,
            'kw_size': None,
            'revenue': None
        }

        save_deals(deals_data)
        save_leaderboard(leaderboard_data)

        # Send confirmation
        embed = discord.Embed(
            title='🎯 Appointment Set!',
            description=f'{message.author.mention} just set an appointment!',
            color=discord.Color.green(),
            timestamp=datetime.utcnow()
        )
        embed.add_field(name='Customer', value=customer_key, inline=True)
        embed.add_field(name='Setter', value=username, inline=True)
        embed.add_field(name='Status', value='🔔 Pending Close', inline=True)
        if assigned_closer:
            embed.add_field(name='Assigned Closer', value=assigned_closer.mention, inline=True)
        embed.set_footer(
            text='To close this deal later, use: #closeddeal First Last kW\n'
                 'Example: #closeddeal John Smith 8.5'
        )

        await message.channel.send(embed=embed)

        # DM assigned closer, if any
        if assigned_closer:
            guild_name = message.guild.name if message.guild else "your server"
            dm_text = (
                f"📅 New appointment assigned to you in **{guild_name}**\n"
                f"Customer: **{customer_key}**\n"
                f"Set by: **{username}**"
            )
            await safe_dm(assigned_closer, dm_text)

        # Update the leaderboard channels for this guild
        if message.guild:
            await update_leaderboards_for_guild(message.guild)

    # CLOSED DEAL workflow - using customer name
    elif '#closeddeal' in content_lower or '#closed deal' in content_lower:
        # Parse: #closeddeal First Last 8.5
        raw_parts = message.content.split()

        try:
            customer_name_tokens = []
            kw_size = None

            for i, part in enumerate(raw_parts):
                p = part.lower()
                if p.startswith('#closeddeal') or (p.startswith('#closed') and 'deal' in p):
                    remaining = raw_parts[i + 1:]
                    if len(remaining) < 2:
                        raise ValueError("Not enough parts after #closeddeal")
                    # Last token should be kW, everything before is customer name
                    kw_token = remaining[-1]
                    kw_size = float(kw_token)
                    customer_name_tokens = remaining[:-1]
                    break

            if not customer_name_tokens or kw_size is None:
                await message.channel.send(
                    '❌ Invalid format! Use: `#closeddeal First Last kW`\n'
                    'Example: `#closeddeal John Smith 8.5`'
                )
                await bot.process_commands(message)
                return

            customer_raw = " ".join(customer_name_tokens)
            customer_key = normalize_customer_name(customer_raw)

            # Check if deal exists
            if customer_key not in deals_data['deals']:
                await message.channel.send(
                    f'❌ No deal found for customer **{customer_key}**!\n'
                    'Make sure the appointment was set first with `#appointmentset First Last`.'
                )
                await bot.process_commands(message)
                return

            deal = deals_data['deals'][customer_key]

            # Check if already closed
            if deal['status'] == 'closed':
                await message.channel.send(
                    f'❌ Deal for **{customer_key}** was already closed by {deal["closer_name"]}!'
                )
                await bot.process_commands(message)
                return

            # Update deal
            closer_id = str(message.author.id)
            closer_name = get_display_name(message.author)

            deal['status'] = 'closed'
            deal['closed_at'] = datetime.now().isoformat()
            deal['closer_id'] = closer_id
            deal['closer_name'] = closer_name
            deal['kw_size'] = kw_size
            deal['revenue'] = kw_size * 3.50  # Assuming $3.50/watt average

            # Update closer stats (including streak)
            update_closer_stats(closer_id, closer_name)
            closer_data = leaderboard_data['closers'][closer_id]
            closer_data['deals_closed'] += 1
            closer_data['total_kw'] += kw_size
            closer_data['total_revenue'] += deal['revenue']
            closer_data['deals'].append(customer_key)

            # Streak logic
            today_date = datetime.now().date()
            last_str = closer_data.get('last_closed_date')
            if last_str:
                last_date = datetime.fromisoformat(last_str).date()
                delta = (today_date - last_date).days
                if delta == 0:
                    # same day, keep streak as is (at least 1)
                    if closer_data['current_streak_days'] == 0:
                        closer_data['current_streak_days'] = 1
                elif delta == 1:
                    closer_data['current_streak_days'] = closer_data.get('current_streak_days', 0) + 1
                else:
                    closer_data['current_streak_days'] = 1
            else:
                closer_data['current_streak_days'] = 1

            closer_data['best_streak_days'] = max(
                closer_data.get('best_streak_days', 0),
                closer_data['current_streak_days']
            )
            closer_data['last_closed_date'] = today_date.isoformat()

            # Update setter stats
            setter_id = deal['setter_id']
            update_setter_stats(setter_id, deal['setter_name'])
            setter_data = leaderboard_data['setters'][setter_id]
            setter_data['appointments_closed'] += 1
            setter_data['total_kw'] += kw_size
            setter_data['deals'].append(customer_key)

            save_deals(deals_data)
            save_leaderboard(leaderboard_data)

            # Send celebration message
            embed = discord.Embed(
                title='🎉 DEAL CLOSED!',
                description=f'Deal for **{customer_key}** has been closed!',
                color=discord.Color.gold(),
                timestamp=datetime.utcnow()
            )
            embed.add_field(name='💰 System Size', value=f'{kw_size} kW', inline=True)
            embed.add_field(name='📊 Est. Revenue', value=f'${deal["revenue"]:,.2f}', inline=True)
            embed.add_field(name='🎯 Setter', value=deal['setter_name'], inline=False)
            embed.add_field(name='🤝 Closer', value=closer_name, inline=False)
            embed.add_field(name='Internal Deal ID', value=f'#{deal["deal_id"]}', inline=True)

            # Streak field
            streak_text = f"{closer_data['current_streak_days']} day(s)"
            streak_text += f" | Best: {closer_data['best_streak_days']} day(s)"
            embed.add_field(name='🔥 Closer Streak', value=streak_text, inline=True)

            # If originally assigned, show it
            if deal.get('assigned_closer_name') and str(deal.get('assigned_closer_id')) != closer_id:
                embed.add_field(
                    name='Original Assignee',
                    value=deal['assigned_closer_name'],
                    inline=False
                )

            await message.channel.send(embed=embed)

            # DM setter + closer
            guild = message.guild
            guild_name = guild.name if guild else "your server"

            setter_member = guild.get_member(int(setter_id)) if guild else None
            closer_member = guild.get_member(int(closer_id)) if guild else None

            setter_dm = (
                f"✅ Your appointment for **{customer_key}** just closed in **{guild_name}**!\n"
                f"Closer: **{closer_name}**\n"
                f"System Size: {kw_size} kW\n"
                f"Est. Revenue: ${deal['revenue']:,.2f}"
            )
            closer_dm = (
                f"🎉 You just closed **{customer_key}** in **{guild_name}**!\n"
                f"System Size: {kw_size} kW\n"
                f"Est. Revenue: ${deal['revenue']:,.2f}\n"
                f"Current streak: {closer_data['current_streak_days']} day(s) "
                f"(Best: {closer_data['best_streak_days']} day(s))"
            )

            await safe_dm(setter_member, setter_dm)
            await safe_dm(closer_member, closer_dm)

            # Update the leaderboard channels for this guild
            if guild:
                await update_leaderboards_for_guild(guild)

        except ValueError:
            await message.channel.send(
                '❌ Invalid kW size! Make sure it\'s a number.\n'
                'Example: `#closeddeal John Smith 8.5`'
            )
        except Exception as e:
            await message.channel.send(f'❌ Error processing deal: {str(e)}')

    # Allow other commands to process
    await bot.process_commands(message)


# ---------- COMMANDS ----------

@bot.command(name='leaderboard', help='Display comprehensive sales leaderboard')
async def show_leaderboard(ctx, timeframe: str = 'all'):
    """Display the sales leaderboard with filtering options"""

    # Calculate date filters
    now = datetime.now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = today_start - timedelta(days=today_start.weekday())
    month_start = today_start.replace(day=1)

    # Filter deals based on timeframe
    def filter_deals_by_time(deals_list, start_date):
        filtered = []
        for key in deals_list:
            if key in deals_data['deals']:
                deal = deals_data['deals'][key]
                if deal['closed_at'] and deal['status'] == 'closed':
                    deal_date = datetime.fromisoformat(deal['closed_at'])
                    if deal_date >= start_date:
                        filtered.append(key)
        return filtered

    # Create main embed
    embed = discord.Embed(
        title='🏆 Solar Sales Leaderboard',
        description=f'Performance Overview - {timeframe.upper()}',
        color=discord.Color.gold(),
        timestamp=datetime.utcnow()
    )

    # CLOSERS LEADERBOARD
    closers_list = []
    for closer_id, data in leaderboard_data['closers'].items():
        if timeframe == 'today':
            deals = filter_deals_by_time(data['deals'], today_start)
        elif timeframe == 'week':
            deals = filter_deals_by_time(data['deals'], week_start)
        elif timeframe == 'month':
            deals = filter_deals_by_time(data['deals'], month_start)
        else:  # all time
            deals = [d for d in data['deals'] if d in deals_data['deals'] and deals_data['deals'][d]['status'] == 'closed']

        # Calculate stats for timeframe
        total_kw = sum(
            deals_data['deals'][d]['kw_size']
            for d in deals
            if d in deals_data['deals'] and deals_data['deals'][d]['kw_size']
        )

        closers_list.append({
            'name': data['username'],
            'deals': len(deals),
            'kw': total_kw
        })

    closers_list.sort(key=lambda x: (x['deals'], x['kw']), reverse=True)

    # SETTERS LEADERBOARD
    setters_list = []
    for setter_id, data in leaderboard_data['setters'].items():
        if timeframe == 'today':
            deals = filter_deals_by_time(data['deals'], today_start)
        elif timeframe == 'week':
            deals = filter_deals_by_time(data['deals'], week_start)
        elif timeframe == 'month':
            deals = filter_deals_by_time(data['deals'], month_start)
        else:  # all time
            deals = [d for d in data['deals'] if d in deals_data['deals'] and deals_data['deals'][d]['status'] == 'closed']

        closed_deals = len(deals)

        # Count appointments set in timeframe
        if timeframe == 'all':
            appts_set = data['appointments_set']
        else:
            appts_set = sum(
                1 for key, deal in deals_data['deals'].items()
                if deal['setter_id'] == setter_id and
                datetime.fromisoformat(deal['created_at']) >= (
                    today_start if timeframe == 'today' else
                    week_start if timeframe == 'week' else
                    month_start
                )
            )

        setters_list.append({
            'name': data['username'],
            'appts_set': appts_set,
            'closed': closed_deals,
            'close_rate': (closed_deals / appts_set * 100) if appts_set > 0 else 0
        })

    setters_list.sort(key=lambda x: (x['closed'], x['appts_set']), reverse=True)

    # Add Closers to embed
    if closers_list:
        closers_text = ""
        medals = ['🥇', '🥈', '🥉']
        for i, closer in enumerate(closers_list[:5]):
            medal = medals[i] if i < 3 else f'{i+1}.'
            closers_text += f'{medal} **{closer["name"]}** - {closer["deals"]} deals | {closer["kw"]:.1f} kW\n'
        embed.add_field(name='👔 Top Closers', value=closers_text or 'No data', inline=False)

    # Add Setters to embed
    if setters_list:
        setters_text = ""
        medals = ['🥇', '🥈', '🥉']
        for i, setter in enumerate(setters_list[:5]):
            medal = medals[i] if i < 3 else f'{i+1}.'
            setters_text += f'{medal} **{setter["name"]}** - {setter["closed"]} closed | {setter["appts_set"]} set ({setter["close_rate"]:.0f}%)\n'
        embed.add_field(name='📞 Top Setters', value=setters_text or 'No data', inline=False)

    # Overall Stats (closed only, all time)
    total_deals = len([d for d in deals_data['deals'].values() if d['status'] == 'closed'])
    total_kw = sum(d['kw_size'] for d in deals_data['deals'].values() if d['status'] == 'closed' and d['kw_size'])
    total_revenue = sum(d['revenue'] for d in deals_data['deals'].values() if d['status'] == 'closed' and d['revenue'])

    stats_text = f"💼 **Total Closed Deals:** {total_deals}\n"
    stats_text += f"⚡ **Total kW (Closed):** {total_kw:.1f}\n"
    stats_text += f"💰 **Est. Revenue (Closed):** ${total_revenue:,.2f}"

    embed.add_field(name='📊 Company Stats (All Time)', value=stats_text, inline=False)
    embed.set_footer(text='Timeframes: all, today, week, month | Use: !leaderboard [timeframe]')

    await ctx.send(embed=embed)


@bot.command(name='mystats', help='View your personal stats')
async def my_stats(ctx):
    """Show personal statistics"""
    user_id = str(ctx.author.id)

    display_name = get_display_name(ctx.author)

    embed = discord.Embed(
        title=f'📊 Stats for {display_name}',
        color=discord.Color.blue(),
        timestamp=datetime.utcnow()
    )

    # Setter stats
    if user_id in leaderboard_data['setters']:
        setter_data = leaderboard_data['setters'][user_id]
        close_rate = (setter_data['appointments_closed'] / setter_data['appointments_set'] * 100) if setter_data['appointments_set'] > 0 else 0

        setter_text = f"📞 **Appointments Set:** {setter_data['appointments_set']}\n"
        setter_text += f"✅ **Closed:** {setter_data['appointments_closed']}\n"
        setter_text += f"📈 **Close Rate:** {close_rate:.1f}%\n"
        setter_text += f"⚡ **Total kW:** {setter_data['total_kw']:.1f}"

        embed.add_field(name='Setter Performance', value=setter_text, inline=False)

    # Closer stats
    if user_id in leaderboard_data['closers']:
        closer_data = leaderboard_data['closers'][user_id]
        avg_kw = closer_data['total_kw'] / closer_data['deals_closed'] if closer_data['deals_closed'] > 0 else 0

        closer_text = f"🤝 **Deals Closed:** {closer_data['deals_closed']}\n"
        closer_text += f"⚡ **Total kW:** {closer_data['total_kw']:.1f}\n"
        closer_text += f"📊 **Avg System Size:** {avg_kw:.1f} kW\n"
        closer_text += f"💰 **Est. Revenue:** ${closer_data['total_revenue']:,.2f}\n"
        closer_text += f"🔥 **Current Streak:** {closer_data.get('current_streak_days', 0)} day(s)\n"
        closer_text += f"🏅 **Best Streak:** {closer_data.get('best_streak_days', 0)} day(s)"

        embed.add_field(name='Closer Performance', value=closer_text, inline=False)

    if user_id not in leaderboard_data['setters'] and user_id not in leaderboard_data['closers']:
        embed.description = "No stats yet! Start setting appointments with #appointmentset"

    await ctx.send(embed=embed)


@bot.command(name='dealinfo', help='Get information about a specific deal by customer name')
async def deal_info(ctx, *, customer_name: str):
    """Show details about a specific deal (by customer name)"""
    customer_key = normalize_customer_name(customer_name)
    if customer_key not in deals_data['deals']:
        await ctx.send(f'❌ Deal for **{customer_key}** not found!')
        return

    deal = deals_data['deals'][customer_key]

    color = (
        discord.Color.red() if deal['status'] == 'canceled'
        else discord.Color.green() if deal['status'] == 'closed'
        else discord.Color.orange()
    )

    embed = discord.Embed(
        title=f'📋 Deal for {customer_key}',
        color=color,
        timestamp=datetime.utcnow()
    )

    embed.add_field(
        name='Status',
        value=(
            '❌ Canceled' if deal['status'] == 'canceled'
            else '✅ Closed' if deal['status'] == 'closed'
            else '🔔 Pending'
        ),
        inline=True
    )
    embed.add_field(name='Customer', value=deal['customer_name'], inline=True)
    embed.add_field(name='Setter', value=deal['setter_name'], inline=True)

    if deal.get('assigned_closer_name'):
        embed.add_field(name='Assigned Closer', value=deal['assigned_closer_name'], inline=True)

    if deal['status'] == 'closed':
        embed.add_field(name='Closer', value=deal['closer_name'], inline=True)
        embed.add_field(name='System Size', value=f"{deal['kw_size']} kW", inline=True)
        embed.add_field(name='Est. Revenue', value=f"${deal['revenue']:,.2f}", inline=True)
        embed.add_field(
            name='Closed Date',
            value=datetime.fromisoformat(deal['closed_at']).strftime('%Y-%m-%d %H:%M'),
            inline=True
        )

    embed.add_field(
        name='Created Date',
        value=datetime.fromisoformat(deal['created_at']).strftime('%Y-%m-%d %H:%M'),
        inline=True
    )
    embed.add_field(name='Internal Deal ID', value=f'#{deal["deal_id"]}', inline=True)

    await ctx.send(embed=embed)


@bot.command(name='pendingdeals', help='Show all pending appointments')
async def pending_deals(ctx):
    """Show all appointments that haven't been closed yet"""
    pending = [deal for deal in deals_data['deals'].values() if deal['status'] == 'appointment_set']

    if not pending:
        await ctx.send('✅ No pending appointments!')
        return

    embed = discord.Embed(
        title='🔔 Pending Appointments',
        description=f'{len(pending)} appointment(s) waiting to be closed',
        color=discord.Color.orange(),
        timestamp=datetime.utcnow()
    )

    for deal in pending[:10]:  # Show max 10
        created = datetime.fromisoformat(deal['created_at']).strftime('%m/%d %H:%M')
        assigned = deal.get('assigned_closer_name') or 'Unassigned'
        embed.add_field(
            name=f"{deal['customer_name']}",
            value=f"Setter: {deal['setter_name']}\nAssigned Closer: {assigned}\nCreated: {created}",
            inline=True
        )

    if len(pending) > 10:
        embed.set_footer(text=f'Showing 10 of {len(pending)} pending deals')

    await ctx.send(embed=embed)


@bot.command(name='todaystats', help='Show today\'s performance')
async def today_stats(ctx):
    """Show statistics for today"""
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # Count today's deals
    today_closed = [
        d for d in deals_data['deals'].values()
        if d['status'] == 'closed' and
        datetime.fromisoformat(d['closed_at']) >= today_start
    ]

    today_set = [
        d for d in deals_data['deals'].values()
        if datetime.fromisoformat(d['created_at']) >= today_start
    ]

    total_kw = sum(d['kw_size'] for d in today_closed if d['kw_size'])
    total_revenue = sum(d['revenue'] for d in today_closed if d['revenue'])

    embed = discord.Embed(
        title='📅 Today\'s Performance',
        color=discord.Color.green(),
        timestamp=datetime.utcnow()
    )

    embed.add_field(name='📞 Appointments Set', value=str(len(today_set)), inline=True)
    embed.add_field(name='✅ Deals Closed', value=str(len(today_closed)), inline=True)
    embed.add_field(name='⚡ Total kW', value=f'{total_kw:.1f}', inline=True)
    embed.add_field(name='💰 Est. Revenue', value=f'${total_revenue:,.2f}', inline=True)

    await ctx.send(embed=embed)


@tasks.loop(hours=24)
async def daily_stats_reset():
    """Daily stats notification (optional - currently not used)"""
    pass


@bot.command(name='help_solar', help='Show all solar tracking commands')
async def help_solar(ctx):
    """Show help for solar tracking commands"""
    embed = discord.Embed(
        title='☀️ Solar Deal Tracker - Command Guide',
        description='Track appointments and closed deals with ease!',
        color=discord.Color.blue()
    )

    embed.add_field(
        name='📝 Hashtag Workflows',
        value='`#appointmentset First Last [@CloserOptional]` - Log a new appointment (uses customer name)\n'
              '`#closeddeal First Last {kw}` - Close a deal\n'
              'Example: `#appointmentset John Smith @Mike`\n'
              'Example: `#closeddeal John Smith 8.5`',
        inline=False
    )

    embed.add_field(
        name='📊 Leaderboard Commands',
        value='`!leaderboard [timeframe]` - View rankings (all/today/week/month)\n'
              '`!mystats` - View your personal stats\n'
              '`!todaystats` - Today\'s company performance',
        inline=False
    )

    embed.add_field(
        name='🔍 Deal Tracking',
        value='`!dealinfo First Last` - View deal details by customer name\n'
              '`!pendingdeals` - See all pending appointments',
        inline=False
    )

    embed.add_field(
        name='🛠 Admin Tools',
        value='`!deletedeal First Last` - Hard delete a deal from the system\n'
              '`!canceldeal First Last` - Mark a closed deal as canceled and roll back stats',
        inline=False
    )

    embed.set_footer(text='Built for solar sales teams 🌞')

    await ctx.send(embed=embed)


# ---------- ADMIN COMMANDS ----------

@bot.command(name='deletedeal', help='Delete a deal by customer name (admin only)')
@commands.has_permissions(administrator=True)
async def delete_deal(ctx, *, customer_name: str):
    """Delete a deal from the system by customer name"""
    customer_key = normalize_customer_name(customer_name)
    if customer_key not in deals_data['deals']:
        await ctx.send(f'❌ Deal for **{customer_key}** not found!')
        return

    deal = deals_data['deals'][customer_key]

    # Remove from leaderboard stats
    if deal['status'] == 'closed':
        # Update closer stats
        if deal['closer_id'] in leaderboard_data['closers']:
            closer_data = leaderboard_data['closers'][deal['closer_id']]
            closer_data['deals_closed'] -= 1
            closer_data['total_kw'] -= deal['kw_size']
            closer_data['total_revenue'] -= deal['revenue']
            if customer_key in closer_data['deals']:
                closer_data['deals'].remove(customer_key)

        # Update setter stats
        if deal['setter_id'] in leaderboard_data['setters']:
            setter_data = leaderboard_data['setters'][deal['setter_id']]
            setter_data['appointments_closed'] -= 1
            setter_data['total_kw'] -= deal['kw_size']
            if customer_key in setter_data['deals']:
                setter_data['deals'].remove(customer_key)
    else:
        # Just an appointment
        if deal['setter_id'] in leaderboard_data['setters']:
            leaderboard_data['setters'][deal['setter_id']]['appointments_set'] -= 1

    # Delete the deal
    del deals_data['deals'][customer_key]

    save_deals(deals_data)
    save_leaderboard(leaderboard_data)

    await ctx.send(f'✅ Deal for **{customer_key}** has been deleted and stats updated.')


@bot.command(name='canceldeal', help='Mark a closed deal as canceled (admin only)')
@commands.has_permissions(administrator=True)
async def cancel_deal(ctx, *, customer_name: str):
    """Mark a closed deal as canceled and roll back stats, but keep the record."""
    customer_key = normalize_customer_name(customer_name)
    if customer_key not in deals_data['deals']:
        await ctx.send(f'❌ Deal for **{customer_key}** not found!')
        return

    deal = deals_data['deals'][customer_key]

    if deal['status'] != 'closed':
        await ctx.send(
            f'❌ Deal for **{customer_key}** is not closed (current status: {deal["status"]}). '
            'Only closed deals can be canceled.'
        )
        return

    kw_size = deal['kw_size'] or 0
    revenue = deal['revenue'] or 0

    # Roll back closer stats
    if deal['closer_id'] in leaderboard_data['closers']:
        closer_data = leaderboard_data['closers'][deal['closer_id']]
        closer_data['deals_closed'] -= 1
        closer_data['total_kw'] -= kw_size
        closer_data['total_revenue'] -= revenue
        if customer_key in closer_data['deals']:
            closer_data['deals'].remove(customer_key)

    # Roll back setter stats
    if deal['setter_id'] in leaderboard_data['setters']:
        setter_data = leaderboard_data['setters'][deal['setter_id']]
        setter_data['appointments_closed'] -= 1
        setter_data['total_kw'] -= kw_size
        if customer_key in setter_data['deals']:
            setter_data['deals'].remove(customer_key)

    # Update deal status to canceled
    deal['status'] = 'canceled'
    deal['canceled_at'] = datetime.now().isoformat()

    save_deals(deals_data)
    save_leaderboard(leaderboard_data)

    # DM setter + closer to notify cancel
    guild = ctx.guild
    guild_name = guild.name if guild else "your server"

    setter_member = guild.get_member(int(deal['setter_id'])) if guild else None
    closer_member = guild.get_member(int(deal['closer_id'])) if guild and deal.get('closer_id') else None

    cancel_msg = (
        f"❌ Deal for **{customer_key}** in **{guild_name}** has been marked as **canceled** by an admin.\n"
        f"System Size (original): {kw_size} kW\n"
        f"Est. Revenue (original): ${revenue:,.2f}"
    )

    await safe_dm(setter_member, cancel_msg)
    await safe_dm(closer_member, cancel_msg)

    await ctx.send(f'✅ Deal for **{customer_key}** has been marked as canceled and stats updated.')


# Run the bot
if __name__ == '__main__':
    TOKEN = os.getenv('DISCORD_BOT_TOKEN')

    if not TOKEN:
        print('Error: DISCORD_BOT_TOKEN environment variable not set!')
        print('Please set your bot token before running the bot.')
    else:
        bot.run(TOKEN)
