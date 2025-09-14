
from typing import *

import discord
from discord.ext import commands, tasks

from . import db, config
from .util import SpamLimit, partition, make_keypad, parse_keypad


def check_is_admin(ctx: commands.Context):
    return ctx.message.author.id in ctx.command.cog.db.admins_cache


def check_in_command_channel(ctx: commands.Context):
    return ctx.channel.id in config["command_channel_ids"]


class UMCPBot(commands.Cog):
    def __init__(self, client: commands.Bot):
        self.client = client

        self.db: db.UMCPDB = None

        self.role_msgs: Dict[int, discord.Message] = {}
        self.role_assign_cooldown = SpamLimit(commands.Cooldown(rate=30, per=120, type=commands.BucketType.user))

        self.umcp_server: discord.Guild = None
        self.role_channel: discord.TextChannel = None
        self.streamer_role: discord.Role = None

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

    """
    Old Role Assignment
    """
    @commands.command()
    @commands.check(check_in_command_channel)
    async def games(self, ctx: commands.Context):
        """Lists all games available to add"""
        game_names = (name for name, id in self.db.games_cache.values())
        await ctx.send("Games:\n" + "\n".join(sorted(game_names)))

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
        game_ids = (self.db.get_game_id(game.strip(), check_alias=True) for game in games)
        valid, invalid = partition(enumerate(game_ids), lambda x: x[1] is None)

        if valid:
            valid_games = [self.db.games_cache[id] for x, id in valid]
            roles = (self.umcp_server.get_role(role_id) for name, role_id in valid_games)
            valid_names = ', '.join(name for name, _ in valid_games)
            if add:
                await ctx.author.add_roles(*roles)
                await ctx.send(f"Added games: {valid_names} to {ctx.author.mention}")
            else:
                await ctx.author.remove_roles(*roles)
                await ctx.send(f"Removed games: {valid_names} from {ctx.author.mention}")

        if invalid:
            invalid_names = ', '.join(games[x] for x, _ in invalid)
            await ctx.send(f"Could not find games: {invalid_names}")

    """
    New Role Assignment
    """
    def games_to_ids(self, games: List[str]) -> Tuple[List[int], List[str]]:
        """Convert a list of game/alias names to a list of game row ids.

        Args:
            games (str): The comma seperated list of game/alias names

        Returns:
            List[int]: The list of game row ids
            List[str]: The games that could not be found
        """
        game_ids = []
        not_found = []
        for game in games:
            id: Optional[int] = self.db.get_game_id(game, check_alias=True)
            if id is not None:
                game_ids.append(id)
            else:
                not_found.append(game)

        return game_ids, not_found

    @commands.command()
    @commands.check(check_is_admin)
    async def autogen(self, ctx: commands.Context, *, misc_exclude: Optional[str]=None):
        """Automatically generate an alphabetical list of role assignment messages

        <misc_exclude> => Comma separated list of games that are misc. and should be appended to the end.
        """
        misc_exclude = [game.strip() for game in misc_exclude.split(",")] if misc_exclude else []
        misc_game_ids, not_found = self.games_to_ids(misc_exclude)

        if not_found:
            await ctx.send(f"Unknown game(s): {', '.join(not_found)}")
            return

        all_games = [id for id, (name, role_id) in sorted(self.db.games_cache.items(), key=lambda x: x[1][0]) if id not in misc_game_ids]
        len_no_misc = len(all_games)
        all_games.extend(misc_game_ids)
        for x in range(0, len(all_games), 9):
            has_misc = x+9 >= len_no_misc and misc_exclude
            only_misc = x >= len_no_misc and misc_exclude

            games = all_games[x:x+9]
            first_name = self.db.games_cache[games[0]][0]
            last_game = all_games[len_no_misc - 1] if has_misc and not only_misc else games[-1]
            last_name = self.db.games_cache[last_game][0]
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

        game_ids, not_found = self.games_to_ids(games)
        if not_found:
            await ctx.send(f"Unknown game(s): {', '.join(not_found)}")
            return

        await self.create_role_message(category_name, game_ids)

        if ctx.channel.id != self.role_channel.id:
            await ctx.send(f"Done. Role assignment messages were generated in {self.role_channel.mention}.")

    async def create_role_message(self, name: str, game_ids: List[int]):
        embed = discord.Embed(title=f"Role Assignment ({name}):",
                              description="React with the corresponding emojis to give yourself that role.\n\n",
                              type="rich", color=discord.Color.blue())

        for x, id in enumerate(game_ids):
            game_name = self.db.games_cache[id][0]
            emoji = make_keypad(x)
            embed.add_field(value=f"\u200b", name=f"{emoji} {game_name}", inline=True)

        msg = await self.role_channel.send("\u200b\n", embed=embed)
        self.db.add_role_message(msg.id, game_ids)

        for x in range(len(game_ids)):
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
        game_name, role_id = self.db.games_cache[game_id]
        role: discord.Role = self.umcp_server.get_role(role_id)

        if role in member.roles:
            await member.remove_roles(role)
            await self.role_channel.send(f"{member.mention}: Removed {game_name}", delete_after=3)
        else:
            await member.add_roles(role)
            await self.role_channel.send(f"{member.mention}: Assigned {game_name}", delete_after=3)

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        if payload.user_id == self.client.user.id:
            return
        if payload.channel_id != self.role_channel.id:
            return

        cd = self.role_assign_cooldown.get_user(payload.user_id)
        if cd.update_rate_limit():
            return

        game_ids = self.db.role_message_cache.get(payload.message_id)
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
        if after.activity and after.activity.type == discord.ActivityType.streaming:
            await after.add_roles(self.streamer_role)
        elif before.activity and before.activity.type == discord.ActivityType.streaming:
            await after.remove_roles(self.streamer_role)


    @commands.Cog.listener()
    async def on_ready(self):
        self.umcp_server: discord.Guild = self.client.get_guild(config["guild_id"])
        self.role_channel: discord.TextChannel = self.umcp_server.get_channel(config["role_channel_id"])
        self.streamer_role: discord.Role = self.umcp_server.get_role(config["streamer_role_id"])

        del self.db
        self.db = db.UMCPDB()

    # @commands.Cog.listener()
    # async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent):
    #     if payload.channel_id != self.role_channel.id:
    #         return
    #
    #     if payload.message_id in self.db.role_message_cache:
    #         self.db.remove_role_message(payload.message_id)
