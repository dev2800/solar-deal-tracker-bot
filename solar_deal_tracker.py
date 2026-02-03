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

# Load data from files
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

# Save data to files
def save_deals(data):
    with open(DEALS_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def save_leaderboard(data):
    with open(LEADERBOARD_FILE, 'w') as f:
        json.dump(data, f, indent=2)

# Initialize data
deals_data = load_deals()
leaderboard_data = load_leaderboard()

def generate_deal_id():
    """Generate unique deal ID"""
    deal_id = deals_data['next_id']
    deals_data['next_id'] += 1
    save_deals(deals_data)
    return deal_id

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
    """Update or create closer stats"""
    if user_id not in leaderboard_data['closers']:
        leaderboard_data['closers'][user_id] = {
            'username': username,
            'deals_closed': 0,
            'total_kw': 0.0,
            'total_revenue': 0.0,
            'deals': []
        }
    else:
        leaderboard_data['closers'][user_id]['username'] = username


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
    
    # Process hashtags
    content = message.content.lower()
    
    # APPOINTMENT SET workflow
    if '#appointmentset' in content or '#appointment set' in content:
        # Generate unique deal ID
        deal_id = generate_deal_id()
        user_id = str(message.author.id)
        username = message.author.name
        
        # Update setter stats
        update_setter_stats(user_id, username)
        leaderboard_data['setters'][user_id]['appointments_set'] += 1
        
        # Create deal record
        deals_data['deals'][str(deal_id)] = {
            'deal_id': deal_id,
            'setter_id': user_id,
            'setter_name': username,
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
        
        # Send confirmation with deal ID
        embed = discord.Embed(
            title='🎯 Appointment Set!',
            description=f'{message.author.mention} just set an appointment!',
            color=discord.Color.green(),
            timestamp=datetime.utcnow()
        )
        embed.add_field(name='Deal ID', value=f'**#{deal_id}**', inline=True)
        embed.add_field(name='Setter', value=username, inline=True)
        embed.add_field(name='Status', value='🔔 Pending Close', inline=True)
        embed.set_footer(text='Use this Deal ID when closing: #closeddeal {deal_id} {kw}')
        
        await message.channel.send(embed=embed)

        # Update the leaderboard channels for this guild
        if message.guild:
            await update_leaderboards_for_guild(message.guild)
    
    # CLOSED DEAL workflow
    elif '#closeddeal' in content or '#closed deal' in content:
        # Parse: #closeddeal 1001 8.5
        parts = content.split()
        
        try:
            # Find the deal ID and kW size
            deal_id = None
            kw_size = None
            
            for i, part in enumerate(parts):
                if part.startswith('#'):
                    # Next part should be deal ID
                    if i + 1 < len(parts):
                        deal_id = parts[i + 1].strip().lstrip('#')
                    # Part after that should be kW
                    if i + 2 < len(parts):
                        kw_size = float(parts[i + 2].strip())
                    break
            
            if not deal_id or kw_size is None:
                await message.channel.send(
                    '❌ Invalid format! Use: `#closeddeal {deal_id} {kw_size}`\n'
                    'Example: `#closeddeal 1001 8.5`'
                )
                return
            
            # Check if deal exists
            if deal_id not in deals_data['deals']:
                await message.channel.send(
                    f'❌ Deal ID #{deal_id} not found! Make sure the appointment was set first.'
                )
                return
            
            deal = deals_data['deals'][deal_id]
            
            # Check if already closed
            if deal['status'] == 'closed':
                await message.channel.send(
                    f'❌ Deal #{deal_id} was already closed by {deal["closer_name"]}!'
                )
                return
            
            # Update deal
            closer_id = str(message.author.id)
            closer_name = message.author.name
            
            deal['status'] = 'closed'
            deal['closed_at'] = datetime.now().isoformat()
            deal['closer_id'] = closer_id
            deal['closer_name'] = closer_name
            deal['kw_size'] = kw_size
            deal['revenue'] = kw_size * 3.50  # Assuming $3.50/watt average
            
            # Update closer stats
            update_closer_stats(closer_id, closer_name)
            leaderboard_data['closers'][closer_id]['deals_closed'] += 1
            leaderboard_data['closers'][closer_id]['total_kw'] += kw_size
            leaderboard_data['closers'][closer_id]['total_revenue'] += deal['revenue']
            leaderboard_data['closers'][closer_id]['deals'].append(deal_id)
            
            # Update setter stats
            setter_id = deal['setter_id']
            update_setter_stats(setter_id, deal['setter_name'])
            leaderboard_data['setters'][setter_id]['appointments_closed'] += 1
            leaderboard_data['setters'][setter_id]['total_kw'] += kw_size
            leaderboard_data['setters'][setter_id]['deals'].append(deal_id)
            
            save_deals(deals_data)
            save_leaderboard(leaderboard_data)
            
            # Send celebration message
            embed = discord.Embed(
                title='🎉 DEAL CLOSED!',
                description=f'**Deal #{deal_id}** has been closed!',
                color=discord.Color.gold(),
                timestamp=datetime.utcnow()
            )
            embed.add_field(name='💰 System Size', value=f'{kw_size} kW', inline=True)
            embed.add_field(name='📊 Est. Revenue', value=f'${deal["revenue"]:,.2f}', inline=True)
            embed.add_field(name='🎯 Setter', value=deal['setter_name'], inline=False)
            embed.add_field(name='🤝 Closer', value=closer_name, inline=False)
            
            await message.channel.send(embed=embed)

            # Update the leaderboard channels for this guild
            if message.guild:
                await update_leaderboards_for_guild(message.guild)
            
        except ValueError:
            await message.channel.send(
                '❌ Invalid kW size! Make sure it\'s a number.\n'
                'Example: `#closeddeal 1001 8.5`'
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
        for deal_id in deals_list:
            if deal_id in deals_data['deals']:
                deal = deals_data['deals'][deal_id]
                if deal['closed_at']:
                    deal_date = datetime.fromisoformat(deal['closed_at'])
                    if deal_date >= start_date:
                        filtered.append(deal_id)
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
            deals = data['deals']
        
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
            deals = data['deals']
        
        closed_deals = len(deals)
        
        # Count appointments set in timeframe
        if timeframe == 'all':
            appts_set = data['appointments_set']
        else:
            # For time-based filters, count deals created in timeframe
            appts_set = sum(
                1 for deal_id, deal in deals_data['deals'].items()
                if deal['setter_id'] == setter_id and 
                (datetime.fromisoformat(deal['created_at']) >= 
                 (today_start if timeframe == 'today' else week_start if timeframe == 'week' else month_start))
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
    
    # Overall Stats (all time)
    total_deals = len([d for d in deals_data['deals'].values() if d['status'] == 'closed'])
    total_kw = sum(d['kw_size'] for d in deals_data['deals'].values() if d['kw_size'])
    total_revenue = sum(d['revenue'] for d in deals_data['deals'].values() if d['revenue'])
    
    stats_text = f"💼 **Total Deals:** {total_deals}\n"
    stats_text += f"⚡ **Total kW:** {total_kw:.1f}\n"
    stats_text += f"💰 **Est. Revenue:** ${total_revenue:,.2f}"
    
    embed.add_field(name='📊 Company Stats (All Time)', value=stats_text, inline=False)
    embed.set_footer(text=f'Timeframes: all, today, week, month | Use: !leaderboard [timeframe]')
    
    await ctx.send(embed=embed)


@bot.command(name='mystats', help='View your personal stats')
async def my_stats(ctx):
    """Show personal statistics"""
    user_id = str(ctx.author.id)
    
    embed = discord.Embed(
        title=f'📊 Stats for {ctx.author.name}',
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
        closer_text += f"💰 **Est. Revenue:** ${closer_data['total_revenue']:,.2f}"
        
        embed.add_field(name='Closer Performance', value=closer_text, inline=False)
    
    if user_id not in leaderboard_data['setters'] and user_id not in leaderboard_data['closers']:
        embed.description = "No stats yet! Start setting appointments with #appointmentset"
    
    await ctx.send(embed=embed)


@bot.command(name='dealinfo', help='Get information about a specific deal')
async def deal_info(ctx, deal_id: str):
    """Show details about a specific deal"""
    if deal_id not in deals_data['deals']:
        await ctx.send(f'❌ Deal #{deal_id} not found!')
        return
    
    deal = deals_data['deals'][deal_id]
    
    embed = discord.Embed(
        title=f'📋 Deal #{deal_id}',
        color=discord.Color.green() if deal['status'] == 'closed' else discord.Color.orange(),
        timestamp=datetime.utcnow()
    )
    
    embed.add_field(name='Status', value='✅ Closed' if deal['status'] == 'closed' else '🔔 Pending', inline=True)
    embed.add_field(name='Setter', value=deal['setter_name'], inline=True)
    
    if deal['status'] == 'closed':
        embed.add_field(name='Closer', value=deal['closer_name'], inline=True)
        embed.add_field(name='System Size', value=f"{deal['kw_size']} kW", inline=True)
        embed.add_field(name='Est. Revenue', value=f"${deal['revenue']:,.2f}", inline=True)
        embed.add_field(name='Closed Date', value=datetime.fromisoformat(deal['closed_at']).strftime('%Y-%m-%d %H:%M'), inline=True)
    
    embed.add_field(name='Created Date', value=datetime.fromisoformat(deal['created_at']).strftime('%Y-%m-%d %H:%M'), inline=True)
    
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
        embed.add_field(
            name=f"Deal #{deal['deal_id']}",
            value=f"Setter: {deal['setter_name']}\nCreated: {created}",
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
        value='`#appointmentset` - Log a new appointment (generates Deal ID)\n'
              '`#closeddeal {id} {kw}` - Close a deal\n'
              'Example: `#closeddeal 1001 8.5`',
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
        value='`!dealinfo {id}` - View deal details\n'
              '`!pendingdeals` - See all pending appointments',
        inline=False
    )
    
    embed.set_footer(text='Built for solar sales teams 🌞')
    
    await ctx.send(embed=embed)


# Admin commands for data management
@bot.command(name='deletedeal', help='Delete a deal (admin only)')
@commands.has_permissions(administrator=True)
async def delete_deal(ctx, deal_id: str):
    """Delete a deal from the system"""
    if deal_id not in deals_data['deals']:
        await ctx.send(f'❌ Deal #{deal_id} not found!')
        return
    
    deal = deals_data['deals'][deal_id]
    
    # Remove from leaderboard stats
    if deal['status'] == 'closed':
        # Update closer stats
        if deal['closer_id'] in leaderboard_data['closers']:
            closer_data = leaderboard_data['closers'][deal['closer_id']]
            closer_data['deals_closed'] -= 1
            closer_data['total_kw'] -= deal['kw_size']
            closer_data['total_revenue'] -= deal['revenue']
            if deal_id in closer_data['deals']:
                closer_data['deals'].remove(deal_id)
        
        # Update setter stats
        if deal['setter_id'] in leaderboard_data['setters']:
            setter_data = leaderboard_data['setters'][deal['setter_id']]
            setter_data['appointments_closed'] -= 1
            setter_data['total_kw'] -= deal['kw_size']
            if deal_id in setter_data['deals']:
                setter_data['deals'].remove(deal_id)
    else:
        # Just an appointment
        if deal['setter_id'] in leaderboard_data['setters']:
            leaderboard_data['setters'][deal['setter_id']]['appointments_set'] -= 1
    
    # Delete the deal
    del deals_data['deals'][deal_id]
    
    save_deals(deals_data)
    save_leaderboard(leaderboard_data)
    
    await ctx.send(f'✅ Deal #{deal_id} has been deleted and stats updated.')


# Run the bot
if __name__ == '__main__':
    TOKEN = os.getenv('DISCORD_BOT_TOKEN')
    
    if not TOKEN:
        print('Error: DISCORD_BOT_TOKEN environment variable not set!')
        print('Please set your bot token before running the bot.')
    else:
        bot.run(TOKEN)
