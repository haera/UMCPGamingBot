#!python3
import json
import logging
import os

from discord.ext import commands

import plugins

with open("config.json", "r") as f:
    config: dict = json.load(f)

client = commands.Bot(command_prefix=commands.when_mentioned_or('!'), description="UMCP Bot",
                      pm_help=False)

plugins.load_plugins(client, config)


@client.event
async def on_ready():
    logger.info(f"Logged in. User: {client.user}, ID: {client.user.id}")


@client.event
async def on_command_error(ctx: commands.Context, e: BaseException):
    if isinstance(e, (commands.BadArgument, commands.MissingRequiredArgument)):
        await ctx.send(str(e))
        return
    elif isinstance(e, (commands.CommandOnCooldown, commands.CommandNotFound, commands.CheckFailure)):
        return

    logger.error(f'Ignoring exception in command {ctx.command}')
    logger.error("Logging an uncaught exception",
                 exc_info=(type(e), e, e.__traceback__))


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s::%(name)s::%(levelname)s::%(message)s', level=logging.INFO)
    logger = logging.getLogger('UMCPBot')
    logger.info("Logging in...")

    token = os.environ.get("DISCORD_TOKEN") or config["bot"]["discord_token"]
    client.run(token)
