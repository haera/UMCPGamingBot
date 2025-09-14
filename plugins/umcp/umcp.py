
from typing import *

import discord
from discord.ext import commands, tasks


from . import db, config
from .util import SpamLimit, partition, make_keypad, parse_keypad


def check_is_admin(ctx: commands.Context):
    return ctx.message.author.id in ctx.command.cog.db.admins


def check_in_command_channel(ctx: commands.Context):
    return ctx.channel.id in config["command_channel_ids"]


class UMCPBot(commands.Cog):
    def __init__(self, client: commands.Bot):
        self.client = client

        self.db = db.UMCPDB()

        self.role_msgs: Dict[int, discord.Message] = {}
        #self.role_assign_cooldown = SpamLimit(commands.Cooldown(rate=30, per=120, bucket_type=commands.BucketType.user))
        #self.umcp_server: discord.Guild = self.client.get_guild(config["guild_id"])
        #self.role_channel: discord.TextChannel = self.umcp_server.get_channel(config["role_channel_id"])
        #self.streamer_role: discord.Role = self.umcp_server.get_role(config["streamer_role_id"])
        self.umcp_server: discord.Guild = self.client.get_guild(config["guild_id"])
        self.role_channel: discord.TextChannel = self.umcp_server.get_channel(config["role_channel_id"])
        self.streamer_role: discord.Role = self.umcp_server.get_role(config["streamer_role_id"])
        
        self.role_channel_cleanup.start()
        await self.check_streaming_role()

    def cog_unload(self):
        self.role_channel_cleanup.cancel()

    """
    Misc
    """
    @commands.command()
    @commands.check(check_in_command_channel)
    async def ping(self, ctx: commands.Context):
        """Ping!"""
        await ctx.send("Pong.")

    @commands.command()
    @commands.check(check_is_admin)
    async def admin(self, ctx: commands.Context, action: str, discord_id: int):
        """Adds or removes an admin

        <action> => add|remove
        <discord_id> => The discord id of the user to add or remove as an admin.
        """
        success = False
        if action == "add":
            success = self.db.add_admin(discord_id)
        elif action == "remove":
            success = self.db.remove_admin(discord_id)

        if success:
            await ctx.message.add_reaction("✅")

    @commands.command()
    @commands.check(check_is_admin)
    async def purgecache(self, ctx: commands.Context):
        """Purge database cache
        """
        self.db._fetch_all()

    """
    Game/Alias Management
    """
    @commands.command()
    @commands.check(check_is_admin)
    async def registergame(self, ctx: commands.Context, name: str, role: discord.Role):
        """Registers a new game in the db

        <name> => The name of the game
        <role> => The role id or @mention
        """
        success = self.db.add_game(name, role.id)

        if success:
            await ctx.message.add_reaction("✅")
        else:
            await ctx.send(f"A game with the name '{name}' already exists.")

    @commands.command()
    @commands.check(check_is_admin)
    async def registeralias(self, ctx: commands.Context, alias: str, game: str):
        """Registers an alias for a game in the db

        <alias> => The alias name
        <game> => The name of the game to alias
        """
        try:
            self.db.add_alias(game, alias)
            await ctx.message.add_reaction("✅")
        except db.DBError as e:
            await ctx.send(str(e))

    @commands.command()
    @commands.check(check_is_admin)
    async def registersubgame(self, ctx: commands.Context, sub_game_name: str, parent_game_name: str):
        """Links a role as the sub game of another.

        <sub_game> => The name of the sub game
        <parent_game> => The name of the parent game to link the sub game to
        """
        sub_game = self.db.get_game(sub_game_name, check_alias=True)
        parent_game = self.db.get_game(parent_game_name, check_alias=True)
        if not sub_game or not parent_game:
            await ctx.send(f"'{sub_game_name}' and/or '{parent_game_name}' are not valid games.")
            return

        success = self.db.add_sub_game(sub_game.game_id, parent_game.game_id)
        if success:
            await ctx.message.add_reaction("✅")
        else:
            await ctx.send(f"'{sub_game_name}' already belongs to a different game.")


    """
    Old Role Assignment
    """
    @commands.command()
    @commands.check(check_in_command_channel)
    async def games(self, ctx: commands.Context):
        """Lists all games available to add"""
        game_names = (game.name for game in self.db.games.values())
        await ctx.send("Games:\n" + "\n".join(sorted(game_names, key=str.casefold)))

    @commands.command()
    @commands.check(check_in_command_channel)
    async def addgame(self, ctx: commands.Context, *, games: str):
        """Adds the role for a game to the user

        <games> => Comma separated list of games to add
        """
        await self.set_games(ctx, games.split(","), add=True)

    @commands.command()
    @commands.check(check_in_command_channel)
    async def removegame(self, ctx: commands.Context, *, games: str):
        """Removes the role for a game from the user

         <games> => Comma separated list of games to remove
        """
        await self.set_games(ctx, games.split(","), add=False)

    async def set_games(self, ctx: commands.Context, games: List[str], add: bool):
        game_objs = (self.db.get_game(game.strip(), check_alias=True) for game in games)
        valid, invalid = partition(enumerate(game_objs), lambda x: x[1] is None)

        if valid:
            roles = (self.umcp_server.get_role(game.role_id) for i, game in valid)
            valid_names = ', '.join(game.name for i, game in valid)
            if add:
                await ctx.author.add_roles(*roles)
                await ctx.send(f"Added games: {valid_names} to {ctx.author.mention}")
            else:
                await ctx.author.remove_roles(*roles)
                await ctx.send(f"Removed games: {valid_names} from {ctx.author.mention}")

        if invalid:
            invalid_names = ', '.join(games[i] for i, _ in invalid)
            await ctx.send(f"Could not find games: {invalid_names}")

    """
    New Role Assignment
    """
    def names_to_games(self, games: List[str]) -> Tuple[List[db.Game], List[str]]:
        """Convert a list of game/alias names to a list of game row ids.

        Args:
            games (str): The comma seperated list of game/alias names

        Returns:
            List[db.Game]: The list of game row ids
            List[str]: The games that could not be found
        """
        game_ids = []
        not_found = []
        for game_name in games:
            game = self.db.get_game(game_name, check_alias=True)
            if game is not None:
                game_ids.append(game)
            else:
                not_found.append(game_name)

        return game_ids, not_found

    @commands.command()
    @commands.check(check_is_admin)
    async def autogen(self, ctx: commands.Context, *, misc_exclude: Optional[str]=None):
        """Automatically generate an alphabetical list of role assignment messages

        <misc_exclude> => Comma separated list of games that are misc. and should be appended to the end.
        """
        games_per_group = 9
        misc_exclude = [game.strip() for game in misc_exclude.split(",")] if misc_exclude else []
        misc_games, not_found = self.names_to_games(misc_exclude)

        if not_found:
            await ctx.send(f"Unknown game(s): {', '.join(not_found)}")
            return

        all_games = [game for game in self.db.games.values() if game not in misc_games]
        all_games.sort(key=lambda g: g.name.casefold())

        len_no_misc = len(all_games)  # this is also the index of the first misc game
        all_games.extend(misc_games)

        for x in range(0, len(all_games), games_per_group):
            # if the last game in this group is a misc game, then this group has misc games
            has_misc = (x+games_per_group) - 1 >= len_no_misc and misc_exclude
            # if the first game in this group is a misc game, then this group **only** has misc games.
            only_misc = x >= len_no_misc and misc_exclude
            # games in this group
            games = all_games[x:x+games_per_group]

            # If this category has misc games, we want to use the last non-misc game as the category last name
            last_game = all_games[len_no_misc - 1] if has_misc and not only_misc else games[-1]
            # we don't care if the first game is a misc game because then the category name will just be Misc. anyways.
            first_name = games[0].name
            last_name = last_game.name

            category_name = f"{first_name[0].upper()}-{last_name[0].upper()}"

            if has_misc:
                if only_misc:
                    category_name = "Misc."
                else:
                    category_name += " + Misc."

            await self.create_role_message(category_name, games)

        if ctx.channel.id != self.role_channel.id:
            await ctx.send(f"Done. Role assignment messages were generated in {self.role_channel.mention}.")

    @commands.command()
    @commands.check(check_is_admin)
    async def rolemessage(self, ctx: commands.Context, category_name: str, *, games: str):
        """Manually create a role assignment message

        <category_name> => The title for this set of games
        <games> => A comma separated list of games this message should assign (max 10)
        """
        games = [game.strip() for game in games.split(",")]

        num = len(games)
        if num > 10:
            await ctx.send(f"Too many roles in one message ({num}), max 10.")
            return

        game_objs, not_found = self.names_to_games(games)
        if not_found:
            await ctx.send(f"Unknown game(s): {', '.join(not_found)}")
            return

        await self.create_role_message(category_name, game_objs)

        if ctx.channel.id != self.role_channel.id:
            await ctx.send(f"Done. Role assignment messages were generated in {self.role_channel.mention}.")

    async def create_role_message(self, name: str, games: List[db.Game]):
        embed = discord.Embed(title=f"Role Assignment ({name}):",
                              description="React with the corresponding emojis to give yourself that role.\n\n",
                              type="rich", color=discord.Color.blue())

        for x, game in enumerate(games):
            emoji = make_keypad(x)
            embed.add_field(value=f"\u200b", name=f"{emoji} {game.name}", inline=True)

        msg = await self.role_channel.send("\u200b\n", embed=embed)
        self.db.add_role_message(msg.id, [game.game_id for game in games])

        for x in range(len(games)):
            emoji = make_keypad(x)
            await msg.add_reaction(emoji)

    async def get_role_message(self, message_id: int) -> Optional[discord.Message]:
        msg = self.role_msgs.get(message_id)
        if msg is not None:
            return msg

        try:
            msg = await self.role_channel.fetch_message(message_id)
        except discord.NotFound:
            self.db.remove_role_message(message_id)
            return None
        self.role_msgs[message_id] = msg
        return msg

    async def toggle_role(self, member: discord.Member, game_id: int):
        game = self.db.games[game_id]
        role: discord.Role = self.umcp_server.get_role(game.role_id)
        all_names: List[str] = [game.name]

        remove = role in member.roles

        if remove:
            await member.remove_roles(role)

            # if you remove a parent role, its sub roles should also be removed.
            children = [self.db.games[id] for id in (self.db.get_sub_games(game.game_id) or [])]

            if children:
                sub_roles = [self.umcp_server.get_role(game.role_id) for game in children]
                to_remove = [role for role in sub_roles if role in member.roles]
                if to_remove:
                    await member.remove_roles(*to_remove)
                    all_names.extend(role.name for role in to_remove)
        else:
            await member.add_roles(role)

            # if you add a sub role, its parent role should also be added.
            parent = self.db.get_parent_game(game_id)

            if parent:
                parent_role = self.umcp_server.get_role(self.db.games[parent].role_id)
                if parent_role not in member.roles:
                    await member.add_roles(parent_role)
                    all_names.append(parent_role.name)

        await self.role_channel.send(
            f"{member.mention}: "
            f"{'Removed' if remove else 'Assigned'} "
            f"{', '.join(all_names)}",
            delete_after=3
        )

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        if payload.user_id == self.client.user.id:
            return
        if payload.channel_id != self.role_channel.id:
            return

        #cd = self.role_assign_cooldown.get_user(payload.user_id)
        #if cd.update_rate_limit():
        #    return

        game_ids = self.db.role_messages.get(payload.message_id)
        if not game_ids:
            return

        msg = await self.get_role_message(payload.message_id)
        if not msg:
            return

        num = parse_keypad(payload.emoji.name)
        if num is None or not (0 <= num < len(game_ids)):
            await msg.clear_reaction(payload.emoji)
            return

        await self.toggle_role(payload.member, game_ids[num])
        await msg.remove_reaction(payload.emoji, payload.member)

    """
    Streamer Activity
    """
    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        if any(activity.type == discord.ActivityType.streaming for activity in after.activities):
            await after.add_roles(self.streamer_role)
        elif any(activity.type == discord.ActivityType.streaming for activity in before.activities):
            await after.remove_roles(self.streamer_role)

    async def check_streaming_role(self):
        member: discord.Member
        async for member in self.umcp_server.fetch_members():
            if any(activity.type == discord.ActivityType.streaming for activity in member.activities):
                print(f"Found streamer: {member}")
                await member.add_roles(self.streamer_role)
            else:
                await member.remove_roles(self.streamer_role)

    """
    Greeting
    """
    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        await member.send(
            f"Welcome to UMCP Gaming, {member.mention}! To get started, head over to {self.role_channel.mention} and "
            f"add games by reacting to the messages. Once you do so, you can view and interact with each game's "
            f"voice and text channels. <3 -speep"
        )


    @commands.Cog.listener()
    async def on_ready(self):
        self.umcp_server: discord.Guild = self.client.get_guild(config["guild_id"])
        self.role_channel: discord.TextChannel = self.umcp_server.get_channel(config["role_channel_id"])
        self.streamer_role: discord.Role = self.umcp_server.get_role(config["streamer_role_id"])
        console.log("working");
        console.log(self.umcp_server);
        console.log(self.client.get_guild(config["guild_id"]));

        del self.db
        self.db = db.UMCPDB()

        self.role_channel_cleanup.start()
        await self.check_streaming_role()

    @tasks.loop(minutes=10.0)
    async def role_channel_cleanup(self):
        # clean up any messages after role request messages that may have been lingering for a while
        most_recent = sorted(self.db.role_messages.keys())[-1]
        await self.role_channel.purge(after=discord.Object(most_recent))

        # clean up any un-removed reactions
        for msg in self.role_msgs.values():
            reacts = msg.reactions
            for r in reacts:
                # if the bot's reaction is the only one, we're good
                if r.count == 1 and r.me:
                    continue

                # if not, make sure the bot still has a reaction to the message
                if not r.me:
                    await msg.add_reaction(r.emoji)

                # then, remove all reactions that don't belong to the bot
                u: Union[discord.User, discord.Member]
                async for u in r.users():
                    if u != self.client.user:
                        await r.remove(u)
