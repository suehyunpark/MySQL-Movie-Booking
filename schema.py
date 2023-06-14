DB_NAME = "DB2018_17119"

TABLES = {}
TABLES["movie"] = """\
    CREATE TABLE movie(
        movie_id INT NOT NULL AUTO_INCREMENT,
        title VARCHAR(255),
        director VARCHAR(255),
        price INT,
        PRIMARY KEY (movie_id),
        UNIQUE (title),
        CHECK (price >= 0 AND price <= 100000)
        );
    """

TABLES["user"] = """
    CREATE TABLE user(
        user_id INT NOT NULL AUTO_INCREMENT,
        name VARCHAR(255),
        age INT,
        class ENUM('basic', 'premium', 'vip'),
        PRIMARY KEY (user_id),
        UNIQUE (name, age),
        CHECK (age >= 12 AND age <= 110)
        );
    """

TABLES["reservation"] = """\
    CREATE TABLE reservation(
        movie_id INT NOT NULL,
        user_id INT NOT NULL,
        reservation_price INT,
        PRIMARY KEY (movie_id, user_id),
        FOREIGN KEY (movie_id) REFERENCES movie(movie_id) ON DELETE CASCADE,
        FOREIGN KEY (user_id) REFERENCES user(user_id) ON DELETE CASCADE
        );
    """

TABLES["rating"] = """\
    CREATE TABLE rating(
        movie_id INT NOT NULL,
        user_id INT NOT NULL,
        rating INT,
        PRIMARY KEY (movie_id, user_id),
        FOREIGN KEY (movie_id, user_id) REFERENCES reservation(movie_id, user_id) ON DELETE CASCADE,
        CHECK (rating >= 1 AND rating <= 5)
        );
    """