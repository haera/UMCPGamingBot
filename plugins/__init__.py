import importlib
import asyncio

from discord.ext import commands


def load_plugins(bot: commands.Bot, config: dict):
    modules = []
    for plugin in config["plugins"]:
        module = importlib.import_module(f"plugins.{plugin}")
        modules.append(module)
        globals()[plugin] = module

    async def setup_plugins():
        for module in modules:
            await module.init(bot, config)

    bot.loop.create_task(setup_plugins())