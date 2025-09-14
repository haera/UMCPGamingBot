import asyncio
from discord.ext import commands

config: dict = {}


async def init(bot: commands.Bot, cfg: dict):
    config.update(cfg[__name__])

    from .umcp import UMCPBot
    await bot.add_cog(UMCPBot(bot))
