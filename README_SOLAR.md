# ☀️ Solar Deal Tracker - Discord Bot

An enterprise-level Discord bot designed specifically for solar sales teams to track appointments, closed deals, and team performance in real-time.

## 🎯 Key Features

### Hashtag-Based Workflow (No Manual Entry!)
- **#appointmentset** - Automatically generates unique Deal ID
- **#closeddeal {id} {kw}** - Closes deals with system size tracking
- Built-in validation prevents duplicate closures and fake entries

### Comprehensive Leaderboards
- **Closers Leaderboard**: Tracks deals closed, total kW sold, and revenue
- **Setters Leaderboard**: Shows appointments set, close rate, and deals closed
- **Time-based filtering**: View stats for today, this week, this month, or all-time
- **Real-time updates**: Instant leaderboard updates after each deal

### Enterprise Analytics
- Total deals sold (company-wide)
- Total kW installed
- Estimated revenue tracking
- Individual rep performance stats
- Pending appointment tracking
- Deal history and audit trail

### Anti-Gaming Features
- Unique deal IDs prevent duplicate entries
- Setter must create appointment before closer can close it
- Full audit trail (who set it, who closed it, when)
- Admin deletion commands with stat rollback
- Immutable deal IDs tied to specific appointments

---

## 📋 How It Works

### For Setters (Appointment Setters)

When you set an appointment, simply type in any Discord channel:
```
Just set an appointment! #appointmentset
```

The bot will:
- Generate a unique Deal ID (e.g., #1001)
- Log your appointment
- Update your stats
- Display the Deal ID for the closer to use

### For Closers (Sales Reps)

When you close a deal, type:
```
Closed the deal! #closeddeal 1001 8.5
```
Format: `#closeddeal {deal_id} {kw_size}`

The bot will:
- Verify the deal exists
- Check it hasn't been closed already
- Update both setter and closer stats
- Track system size and estimated revenue
- Celebrate the win! 🎉

---

## 🤖 Bot Commands

### User Commands

| Command | Description | Example |
|---------|-------------|---------|
| `!leaderboard [timeframe]` | View rankings | `!leaderboard week` |
| `!mystats` | Your personal stats | `!mystats` |
| `!todaystats` | Today's company performance | `!todaystats` |
| `!dealinfo {id}` | View deal details | `!dealinfo 1001` |
| `!pendingdeals` | See pending appointments | `!pendingdeals` |
| `!help_solar` | Show all commands | `!help_solar` |

**Timeframe options**: `all`, `today`, `week`, `month`

### Admin Commands (Require Administrator Permission)

| Command | Description | Example |
|---------|-------------|---------|
| `!deletedeal {id}` | Delete a deal and rollback stats | `!deletedeal 1001` |

---

## 📊 Leaderboard Metrics

### Closers Tracked By:
- 💼 Total deals closed
- ⚡ Total kW sold
- 💰 Estimated revenue generated
- 📊 Average system size

### Setters Tracked By:
- 📞 Appointments set
- ✅ Appointments closed
- 📈 Close rate percentage
- ⚡ Total kW from their appointments

### Company-Wide Stats:
- Total deals sold
- Total kW installed
- Total estimated revenue
- Deals closed today
- Deals closed this week

---

## 🚀 Setup Instructions

### 1. Create Discord Bot

1. Go to [Discord Developer Portal](https://discord.com/developers/applications)
2. Click "New Application" → Name it (e.g., "Solar Tracker")
3. Go to **Bot** tab → Click "Add Bot"
4. Enable these **Privileged Gateway Intents**:
   - ✅ MESSAGE CONTENT INTENT
   - ✅ SERVER MEMBERS INTENT
5. Click "Reset Token" and copy your bot token

### 2. Invite Bot to Server

1. Go to **OAuth2** → **URL Generator**
2. Select **Scopes**: `bot`
3. Select **Bot Permissions**:
   - ✅ Send Messages
   - ✅ Embed Links
   - ✅ Read Message History
   - ✅ Read Messages/View Channels
4. Copy the generated URL and open in browser
5. Select your server and authorize

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Bot Token

**Option A: Environment Variable (Recommended)**
```bash
export DISCORD_BOT_TOKEN="your_bot_token_here"
```

**Option B: .env File**
1. Copy `.env.example` to `.env`
2. Add your token:
```
DISCORD_BOT_TOKEN=your_actual_bot_token_here
```

### 5. Run the Bot

```bash
python solar_deal_tracker.py
```

You should see: `{BotName} has connected to Discord!`

---

## 💡 Usage Examples

### Example 1: Setter logs appointment
```
Setter: "Just got off the phone with a homeowner in Austin! #appointmentset"

Bot: 🎯 Appointment Set!
     Deal ID: #1001
     Setter: JohnDoe
     Status: 🔔 Pending Close
```

### Example 2: Closer closes the deal
```
Closer: "Closed it! 8.5kW system #closeddeal 1001 8.5"

Bot: 🎉 DEAL CLOSED!
     Deal #1001 has been closed!
     💰 System Size: 8.5 kW
     📊 Est. Revenue: $29,750.00
     🎯 Setter: JohnDoe
     🤝 Closer: JaneSmith
```

### Example 3: Check leaderboard
```
User: "!leaderboard week"

Bot: [Shows top 5 closers and setters for the week with deals and kW]
```

---

## 🔐 Anti-Gaming Measures

### Why This System Works:

1. **Unique Deal IDs**: Each appointment gets a unique, auto-incremented ID
2. **Two-Step Process**: Setter MUST create appointment before closer can close
3. **Validation**: Bot checks if deal exists and isn't already closed
4. **Audit Trail**: Every deal records:
   - Who set it (name + user ID)
   - When it was set
   - Who closed it (name + user ID)
   - When it was closed
   - System size
5. **No Manual Point Addition**: Reps can't just add points to themselves
6. **Admin Oversight**: Admins can delete fraudulent deals with full stat rollback

### What Reps CAN'T Do:
- ❌ Close a deal that doesn't exist
- ❌ Close the same deal twice
- ❌ Manually add points to themselves
- ❌ Edit deal IDs or stats directly
- ❌ Create deals without the hashtag workflow

### What Admins CAN Do:
- ✅ Delete fraudulent deals
- ✅ View full deal history
- ✅ See all pending appointments
- ✅ Track who set/closed each deal

---

## 📈 Customization Options

### Change Revenue Calculation
Edit line 169 in `solar_deal_tracker.py`:
```python
deal['revenue'] = kw_size * 3.50  # Change 3.50 to your $/watt
```

### Modify Leaderboard Size
Change the number of reps shown:
```python
for i, closer in enumerate(closers_list[:5]):  # Change :5 to show more/less
```

### Add Custom Commands
Follow the existing pattern:
```python
@bot.command(name='yourcommand', help='Description')
async def your_command(ctx):
    # Your code here
    await ctx.send('Response')
```

---

## 📁 Data Storage

All data is stored in JSON files:
- `deals_data.json` - Complete deal history
- `leaderboard_data.json` - Rep statistics

**Backup these files regularly!** They contain all your sales data.

---

## 🛠️ Troubleshooting

**Bot doesn't respond to hashtags:**
- Verify MESSAGE CONTENT INTENT is enabled
- Check bot has permission to read messages in the channel

**Deal ID not found:**
- Make sure setter used #appointmentset first
- Check the deal ID is correct (case-sensitive)

**Can't use admin commands:**
- User must have Administrator permission in Discord
- Bot must have proper permissions

**Stats seem wrong:**
- Use `!dealinfo {id}` to audit specific deals
- Check `deals_data.json` for data integrity

---

## 🎯 Best Practices

1. **Set Channel Permissions**: Consider creating a #deals channel where reps post
2. **Daily Standup**: Use `!todaystats` to review performance each morning
3. **Weekly Review**: Run `!leaderboard week` in team meetings
4. **Celebrate Wins**: The bot auto-celebrates closed deals - keep morale high!
5. **Backup Data**: Regularly backup your JSON files to prevent data loss

---

## 🚨 Important Notes

- **Revenue is Estimated**: Based on $/watt calculation (default: $3.50/watt)
- **Deal IDs Auto-Increment**: Starting at 1000, increases by 1 each appointment
- **All Times in UTC**: Timestamps use UTC timezone
- **Hashtags Are Case-Insensitive**: #AppointmentSet = #appointmentset

---

## 📞 Support

For issues or feature requests, modify the code or reach out for assistance!

## 📄 License

Free to use and modify for your solar business.

---

**Built for solar sales teams who want simple, transparent, and fraud-resistant deal tracking.** ☀️
