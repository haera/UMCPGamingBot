from discord.ext import commands

config: dict = {}


def init(bot: commands.Bot, cfg: dict):
    config.update(cfg[__name__])

    from .umcp import UMCPBot
    bot.add_cog(UMCPBot(bot))
