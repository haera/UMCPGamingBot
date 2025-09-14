import os
from collections import defaultdict
from typing import Optional, List, Tuple, Dict, Set, NamedTuple
from urllib import parse

import psycopg2
import discord.utils

from .util import MappingProxy

DEV_URL = "https://postgres:lol@172.17.80.150:5432/postgres"


class Game(NamedTuple):
    name: str
    role_id: int
    game_id: int

# @dataclass(frozen=True)
# class Game:
#     name: str
#     role_id: int
#     game_id: int


class Alias(NamedTuple):
    name: str
    game_id: int  # game row id
    alias_id: int # alias row id
    # game: Game # reference to actual game


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
                    id         SERIAL UNIQUE NOT NULL,
                    discord_id VARCHAR(20)   NOT NULL,
                    name       VARCHAR(50)   NOT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS aliases (
                    id      SERIAL UNIQUE NOT NULL,
                    alias   VARCHAR(50)   NOT NULL,
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
                  game_ids   INTEGER[]
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sub_games (
                    sub_id    INTEGER UNIQUE REFERENCES games(id),
                    parent_id INTEGER        REFERENCES games(id)
                );
            """)

        # Set of discord user ids of admins
        self.__admins: Set[int] = set()
        # Dict of {game row id: (game name, discord role id)}
        self.__games: Dict[int, Game] = {}
        # Dict of {alias row id: (alias name, game row id)}
        self.__aliases: Dict[int, Alias] = {}
        # Dict of {discord message id: [game ids]}
        self.__role_messages: Dict[int, List[int]] = {}
        # Dict of {Sub-game ID: Parent-game ID}
        self.__parent_games: Dict[int, int] = {}
        # Dict of {Parent-game ID: [Sub-game IDs]}
        # (reverse mapping of self.__parent_games)
        self.__sub_games: Dict[int, List[int]] = defaultdict(list)

        # Read-only caches
        # SequenceProxy isn't exactly correct for set, but it has pretty much the one method we need (__contains__)
        self.__admins_proxy = discord.utils.SequenceProxy(self.__admins)
        self.__games_proxy = MappingProxy(self.__games)
        self.__aliases_proxy = MappingProxy(self.__aliases)
        self.__messages_proxy = MappingProxy(self.__role_messages)

        self._fetch_all()

    # Cache all of these so we aren't doing SQL queries for simple reads.
    @property
    def admins(self) -> Set[int]:
        return self.__admins_proxy

    @property
    def games(self) -> Dict[int, Game]:
        return self.__games_proxy

    @property
    def aliases(self) -> Dict[int, Alias]:
        return self.__aliases_proxy

    @property
    def role_messages(self) -> Dict[int, List[int]]:
        return self.__messages_proxy

    def get_sub_games(self, parent_game_id: int) -> Optional[List[int]]:
        return self.__sub_games.get(parent_game_id)
        # return discord.utils.SequenceProxy(l) if l else None

    def get_parent_game(self, sub_game_id: int) -> Optional[int]:
        return self.__parent_games.get(sub_game_id)

    def get_game(self, game_name: str, check_alias: bool=False) -> Optional[Game]:
        game_name = game_name.casefold()
        game = discord.utils.find(lambda g: g.name.casefold() == game_name, self.games.values())
        if game is not None:
            return game

        if not check_alias:
            return None

        alias = self.get_alias(game_name)
        return None if alias is None else self.games[alias.game_id]

    def get_alias(self, alias_name: str) -> Optional[Alias]:
        alias_name = alias_name.casefold()
        alias: Alias = discord.utils.find(lambda a: a.name.casefold() == alias_name, self.aliases.values())
        return alias

    def _fetch_all(self):
        self._fetch_admins()
        self._fetch_games()
        self._fetch_aliases()
        self._fetch_role_messages()
        self._fetch_sub_games()

    def _fetch_admins(self):
        with self.conn, self.conn.cursor() as cur:
            cur.execute("SELECT discord_id FROM admins;")
            self.__admins.clear()
            self.__admins.update((int(id[0]) for id in cur))

    def _fetch_games(self):
        with self.conn, self.conn.cursor() as cur:
            cur.execute("SELECT id, name, discord_id FROM games;")
            self.__games.clear()
            self.__games.update((row[0], Game(row[1], int(row[2]), row[0])) for row in cur)

    def _fetch_aliases(self):
        with self.conn, self.conn.cursor() as cur:
            cur.execute("SELECT id, alias, game_id FROM aliases;")
            self.__aliases.clear()
            self.__aliases.update((row[0], Alias(row[1], row[2], row[0])) for row in cur)

    def _fetch_role_messages(self):
        with self.conn, self.conn.cursor() as cur:
            cur.execute("SELECT message_id, game_ids FROM role_messages;")
            self.__role_messages.clear()
            self.__role_messages.update((int(row[0]), row[1]) for row in cur)

    def _fetch_sub_games(self):
        with self.conn, self.conn.cursor() as cur:
            cur.execute("SELECT sub_id, parent_id FROM sub_games;")
            self.__parent_games.clear()
            self.__parent_games.update(iter(cur))
            # update reverse mapping
            self.__sub_games.clear()
            for sub, parent in self.__parent_games.items():
                self.__sub_games[parent].append(sub)

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

    def add_game(self, game_name: str, role_id: int) -> Optional[Game]:
        if self.get_game(game_name, check_alias=True) is not None:
            return None

        with self.conn, self.conn.cursor() as cur:
            cur.execute("INSERT INTO games (discord_id, name) VALUES (%s, %s) RETURNING id;", (str(role_id), game_name))
            game_row_id: int = cur.fetchone()[0]
            self.__games[game_row_id] = Game(game_name, role_id, game_row_id)
            return self.__games[game_row_id]

    def remove_game(self, game_id: int) -> bool:
        # game_name = game_name.casefold()
        # game = self.get_game(game_name, check_alias=True)
        if game_id not in self.__games:
            return False

        with self.conn, self.conn.cursor() as cur:
            # first delete all aliases to this game
            cur.execute("DELETE FROM aliases WHERE game_id=%s RETURNING id;", (game_id, ))
            for alias_row_id, in cur.fetchall():
                self.__aliases.pop(alias_row_id)

            cur.execute("DELETE FROM sub_games WHERE sub_id=%s OR parent_id=%s;", (game_id, game_id))
            # if this is the child of a game, remove it from parent lists
            parent = self.__parent_games.pop(game_id, None)
            if parent:
                self.__sub_games[parent].remove(game_id)
            # if this is the parent of any games, remove it from child lists
            children = self.__sub_games.pop(game_id, [])
            for c in children:
                self.__parent_games.pop(c, None)

            # then finally delete the game itself
            cur.execute("DELETE FROM games WHERE id=%s;", (game_id,))
            self.__games.pop(game_id)
            return True

    def add_alias(self, game_name: str, alias_name: str) -> Alias:
        if alias_name.casefold() in (game.name.casefold() for game in self.games.values()):
            raise DBError(f"A game with the name {alias_name} already exists.")
        if alias_name.casefold() in (alias.name.casefold() for alias in self.aliases.values()):
            raise DBError(f"An alias with the name {alias_name} already exists.")

        game = self.get_game(game_name)
        if game is None:
            raise DBError(f"There is no game with the name {game_name}.")

        with self.conn, self.conn.cursor() as cur:
            cur.execute("INSERT INTO aliases (game_id, alias) VALUES (%s, %s) RETURNING id;", (game.game_id, alias_name))
            alias_row_id: int = cur.fetchone()[0]
            self.__aliases[alias_row_id] = Alias(alias_name, game.game_id, alias_row_id)
            return self.__aliases[alias_row_id]

    def remove_alias(self, alias_id: int) -> bool:
        if alias_id not in self.__aliases:
            return False

        with self.conn, self.conn.cursor() as cur:
            cur.execute("DELETE FROM aliases WHERE id=%s;", (alias_id, ))
            self.__aliases.pop(alias_id)
            return True

    def add_sub_game(self, sub_game_id: int, parent_game_id: int) -> bool:
        if sub_game_id in self.__parent_games:
            return False

        with self.conn, self.conn.cursor() as cur:
            cur.execute("INSERT INTO sub_games (sub_id, parent_id) VALUES (%s, %s);", (sub_game_id, parent_game_id))
            self.__parent_games[sub_game_id] = parent_game_id
            self.__sub_games[parent_game_id].append(sub_game_id)
            return True

    def remove_sub_game(self, sub_game_id: int):
        if sub_game_id not in self.__parent_games:
            return False

        with self.conn, self.conn.cursor() as cur:
            cur.execute("DELETE FROM sub_games WHERE sub_id=%s;", (sub_game_id, ))
            parent = self.__parent_games.pop(sub_game_id)
            self.__sub_games[parent].remove(sub_game_id)
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
