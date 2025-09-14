import os
from typing import Optional, List, Tuple, Dict, Set
from urllib import parse

import psycopg2
import discord.utils

from .util import MappingProxy

DEV_URL = "https://postgres:lol@localhost:5432/postgres"


class DBError(Exception):
    def __init__(self, message):
        super().__init__(message)


# Really, this is just persistent storage at this point.
class UMCPDB(object):
    def __init__(self, url: str=DEV_URL):
        url = parse.urlparse(os.environ.get("DATABASE_URL", url))
        self.conn = psycopg2.connect(
            dbname=url.path.lstrip('/'),
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port
        )

        with self.conn, self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS games (
                    id SERIAL UNIQUE NOT NULL,
                    discord_id VARCHAR(20) NOT NULL,
                    name VARCHAR(50) NOT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS aliases (
                    id SERIAL UNIQUE NOT NULL,
                    alias VARCHAR(50) NOT NULL,
                    game_id INTEGER REFERENCES games(id)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS admins (
                  discord_id VARCHAR(20) NOT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS role_messages (
                  message_id VARCHAR(20) NOT NULL,
                  game_ids INTEGER[]
                );
            """)

        # Set of discord user ids of admins
        self.__admins: Set[int] = set()
        # Dict of {game row id: (game name, discord role id)}
        self.__games: Dict[int, Tuple[str, int]] = {}
        # Dict of {alias row id: (alias name, game row id)}
        self.__aliases: Dict[int, Tuple[str, int]] = {}
        # Dict of {discord message id: [game ids]}
        self.__role_messages: Dict[int, List[int]] = {}

        # Read-only caches
        # SequenceProxy isn't exactly correct for set, but it has pretty much the one method we need (__contains__)
        self.__admins_proxy = discord.utils.SequenceProxy(self.__admins)
        self.__games_proxy = MappingProxy(self.__games)
        self.__aliases_proxy = MappingProxy(self.__aliases)
        self.__messages_proxy = MappingProxy(self.__role_messages)

        self._fetch_all()

    # Cache all of these so we aren't doing SQL queries for simple reads.
    @property
    def admins_cache(self) -> Set[int]:
        return self.__admins_proxy

    @property
    def games_cache(self) -> Dict[int, Tuple[str, int]]:
        return self.__games_proxy

    @property
    def aliases_cache(self) -> Dict[int, Tuple[str, int]]:
        return self.__aliases_proxy

    @property
    def role_message_cache(self) -> Dict[int, List[int]]:
        return self.__messages_proxy

    def get_game_id(self, game_name: str, check_alias: bool=False) -> Optional[int]:
        game_name = game_name.lower()
        game = discord.utils.find(lambda g: g[1][0].lower() == game_name, self.games_cache.items())
        if game is not None:
            return game[0]

        if not check_alias:
            return None

        alias_id = self.get_alias_id(game_name)
        return None if alias_id is None else self.aliases_cache[alias_id][1]

    def get_alias_id(self, alias_name: str) -> Optional[int]:
        alias_name = alias_name.lower()
        alias = discord.utils.find(lambda a: a[1][0].lower() == alias_name, self.aliases_cache.items())
        return None if alias is None else alias[0]

    def _fetch_all(self):
        self._fetch_admins()
        self._fetch_games()
        self._fetch_aliases()
        self._fetch_role_messages()

    def _fetch_admins(self):
        with self.conn, self.conn.cursor() as cur:
            cur.execute("SELECT discord_id FROM admins;")
            self.__admins.clear()
            self.__admins.update((int(id[0]) for id in cur))

    def _fetch_games(self):
        with self.conn, self.conn.cursor() as cur:
            cur.execute("SELECT id, name, discord_id FROM games;")
            self.__games.clear()
            self.__games.update((row[0], (row[1], int(row[2]))) for row in cur)

    def _fetch_aliases(self):
        with self.conn, self.conn.cursor() as cur:
            cur.execute("SELECT id, alias, game_id FROM aliases;")
            self.__aliases.clear()
            self.__aliases.update((row[0], (row[1], row[2])) for row in cur)

    def _fetch_role_messages(self):
        with self.conn, self.conn.cursor() as cur:
            cur.execute("SELECT message_id, game_ids FROM role_messages;")
            self.__role_messages.clear()
            self.__role_messages.update((int(row[0]), row[1]) for row in cur)

    def add_admin(self, discord_id: int) -> bool:
        if discord_id in self.__admins:
            return False

        with self.conn, self.conn.cursor() as cur:
            cur.execute("INSERT INTO admins(discord_id) VALUES (%s);", (str(discord_id),))
            self.__admins.add(discord_id)
            return True

    def remove_admin(self, discord_id: int) -> bool:
        if discord_id in self.__admins:
            self.__admins.remove(discord_id)

        # To be on the safe side, run the SQL DELETE even if the discord_id wasn't found in the cache.
        with self.conn, self.conn.cursor() as cur:
            cur.execute("DELETE FROM admins WHERE discord_id=%s;", (str(discord_id),))
            return True

    def add_game(self, game_name: str, role_id: int) -> bool:
        if game_name.lower() in (game[0].lower() for game in self.games_cache.values()):
            return False

        with self.conn, self.conn.cursor() as cur:
            cur.execute("INSERT INTO games (discord_id, name) VALUES (%s, %s) RETURNING id;", (str(role_id), game_name))
            game_row_id: int = cur.fetchone()[0]
            self.__games[game_row_id] = (game_name, role_id)
            return True

    def remove_game(self, game_name: str):
        game_row_id = self.get_game_id(game_name, check_alias=True)
        if not game_row_id:
            return False

        with self.conn, self.conn.cursor() as cur:
            cur.execute("DELETE FROM aliases WHERE game_id=%s RETURNING id;", (game_row_id, ))
            for alias_row_id in cur.fetchall():
                self.__aliases.pop(alias_row_id)
            cur.execute("DELETE FROM games WHERE name ILIKE %s;", (game_name,))
            self.__games.pop(game_row_id)
            return True

    def add_alias(self, game_name: str, alias_name: str) -> bool:
        if alias_name.lower() in (game[0].lower() for game in self.games_cache.values()):
            raise DBError(f"A game with the name {alias_name} already exists.")
        if alias_name.lower() in (alias[0].lower() for alias in self.aliases_cache.values()):
            raise DBError(f"An alias with the name {alias_name} already exists.")

        game_row_id = self.get_game_id(game_name)
        if game_row_id is None:
            raise DBError(f"There is no game with the name {game_name}.")

        with self.conn, self.conn.cursor() as cur:
            cur.execute("INSERT INTO aliases (game_id, alias) VALUES (%s, %s) RETURNING id;", (game_row_id, alias_name))
            alias_row_id: int = cur.fetchone()[0]
            self.__aliases[alias_row_id] = (alias_name, game_row_id)
            return True

    def remove_alias(self, alias_name: str) -> bool:
        if alias_name.lower() not in (alias[0].lower() for alias in self.aliases_cache.values()):
            return False

        with self.conn, self.conn.cursor() as cur:
            cur.execute("DELETE FROM aliases WHERE alias ILIKE %s RETURNING id;", (alias_name, ))
            alias_row_id: int = cur.fetchone()[0]
            self.__aliases.pop(alias_row_id)
            return True

    def add_role_message(self, message_id: int, game_ids: List[int]) -> bool:
        if message_id in self.__role_messages:
            return False

        with self.conn, self.conn.cursor() as cur:
            cur.execute("INSERT INTO role_messages(message_id, game_ids) VALUES (%s, %s);", (str(message_id), game_ids))
            self.__role_messages[message_id] = game_ids
            return True

    def remove_role_message(self, message_id: int) -> bool:
        if message_id not in self.__role_messages:
            return False

        with self.conn, self.conn.cursor() as cur:
            cur.execute("DELETE FROM role_messages WHERE message_id=%s;", (str(message_id),))
            self.__role_messages.pop(message_id)
            return True
