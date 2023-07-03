from pymysql import Connect


class StatsRepository:
    def __init__(self, conn: Connect):
        self.conn = conn

    def init_tables(self):
        create_table_comments = """
        CREATE TABLE IF NOT EXISTS comments(
            id VARCHAR(50) PRIMARY KEY, 
            user_id_fk VARCHAR(20), 
            content TEXT NOT NULL, 
            time DATETIME NOT NULL
        )
        """

        create_table_comments_emoji = """
        CREATE TABLE IF NOT EXISTS comments_emojis(
            comments_id_fk VARCHAR(50) NOT NULL , 
            emoji VARCHAR(100) NOT NULL, 
            emoji_count INT NOT NULL
        )
        """

        create_table_recomments = """
        CREATE TABLE IF NOT EXISTS recomments(
            id VARCHAR(50) PRIMARY KEY, 
            comment_id_fk VARCHAR(50) NOT NULL, 
            user_id_fk VARCHAR(20), 
            content TEXT NOT NULL, 
            time DATETIME NOT NULL
        )
        """

        create_table_recomments_emoji = """
        CREATE TABLE IF NOT EXISTS recomments_emojis(
            recomment_id_fk VARCHAR(50) NOT NULL, 
            emoji VARCHAR(100) NOT NULL, 
            emoji_count INT NOT NULL
        )
        """

        alter_table_comments = """
        ALTER TABLE comments ADD CONSTRAINT fk_comments_users FOREIGN KEY IF NOT EXISTS (user_id_fk) REFERENCES users(id)
        """

        alter_table_comments_emojis = """
        ALTER TABLE comments_emojis ADD CONSTRAINT fk_commentsEmoji_comments FOREIGN KEY IF NOT EXISTS (comments_id_fk) REFERENCES comments (id)
        """

        alter_table_recomments = """
        ALTER TABLE recomments ADD CONSTRAINT fk_recomments_comments FOREIGN KEY IF NOT EXISTS (comment_id_fk) REFERENCES comments (id)
        """

        alter_table_recomments_two = """
        ALTER TABLE recomments ADD CONSTRAINT fk_recomments_user FOREIGN KEY IF NOT EXISTS (user_id_fk) REFERENCES users (id)
        """

        alter_table_recomments_emojis = """
        ALTER TABLE recomments_emojis ADD CONSTRAINT recommentsEmoji_recomments FOREIGN KEY IF NOT EXISTS (recomment_id_fk) REFERENCES recomments (id)
        """

        conn = self.conn
        cursor = conn.cursor()

        cursor.execute(create_table_comments)
        cursor.execute(create_table_comments_emoji)
        cursor.execute(create_table_recomments)
        cursor.execute(create_table_recomments_emoji)
        cursor.fetchall()
        conn.commit()

        cursor.execute(alter_table_comments)
        cursor.execute(alter_table_comments_emojis)
        cursor.execute(alter_table_recomments)
        cursor.execute(alter_table_recomments_two)
        cursor.execute(alter_table_recomments_emojis)
        cursor.fetchall()
        conn.commit()

    def get_user_id(self, name):
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM users WHERE name = %s", (name))
        res = cursor.fetchone()

        if res is None:
            return None

        cursor.close()
        return res[0]

    def insert_comments(self, id, user_id, content, time):
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO comments (id, user_id_fk, content, time) VALUES (%s, %s, %s, %s) ",
                (id, user_id, content, time),
            )

            cursor.fetchone()
            self.commit()

        except Exception as e:
            raise e

    def insert_comments_emojis(self, comment_id, emoji, emoji_count):
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO comments_emojis (comments_id_fk, emoji, emoji_count) VALUES(%s, %s ,%s)",
                (comment_id, emoji, emoji_count),
            )

            cursor.fetchone()
            self.commit()
        except Exception as e:
            raise e

    def insert_recomments(self, id, comment_id, user_id, content, time):
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO recomments (id, comment_id_fk, user_id_fk, content, time) VALUES (%s, %s, %s, %s, %s)",
                (id, comment_id, user_id, content, time),
            )

            cursor.fetchone()
            self.commit()
        except Exception as e:
            raise e

    def insert_recomments_emojis(self, recomment_id, emoji, emoji_count):
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO recomments_emojis (recomment_id_fk, emoji, emoji_count) VALUES(%s, %s ,%s)",
                (recomment_id, emoji, emoji_count),
            )

            cursor.fetchone()
            self.commit()
        except Exception as e:
            raise e
