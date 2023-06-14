from schema import DB_NAME

#  all format parameters are converted via str(),
insert_into_movie = """\
    INSERT INTO movie (title, director, price)
    VALUES (%s, %s, %s);
    """
    
insert_into_user = """\
    INSERT INTO user (name, age, class)
    VALUES (%s, %s, %s);
    """
    
insert_into_reservation = """\
    INSERT INTO reservation (movie_id, user_id, reservation_price)
    VALUES (%s, %s, %s);
    """
    
insert_into_rating = """\
    INSERT INTO rating (movie_id, user_id, rating)
    VALUES (%s, %s, %s);
    """
    
select_all_from_movie = """\
    SELECT movie.movie_id, title, director, price, AVG(reservation_price) as avg_price, COUNT(DISTINCT reservation.user_id) as num_reservations, AVG(rating) as avg_rating
    FROM movie 
    LEFT OUTER JOIN reservation ON movie.movie_id = reservation.movie_id 
    LEFT OUTER JOIN rating ON movie.movie_id = rating.movie_id
    GROUP BY movie.movie_id
    ORDER BY movie.movie_id;
    """
    
select_all_from_user = """\
    SELECT *
    FROM user
    ORDER BY user_id;
    """

# multiple commands
drop_all_tables = """\
    DROP TABLE IF EXISTS rating, reservation, user, movie;
    """
    
check_database_empty = f"""\
    SELECT COUNT(DISTINCT table_name) 
    FROM information_schema.tables
    where table_schema = '{DB_NAME}';
    """
     
delete_from_movie = """\
    DELETE FROM movie
    WHERE movie_id = %s;
    """
    
delete_from_user = """\
    DELETE FROM user
    WHERE user_id = %s;
    """
    
select_id_from_movie = """\
    SELECT movie_id
    FROM movie
    WHERE title = %s;
    """
    
select_id_from_user = """\
    SELECT user_id
    FROM user
    WHERE name = %s AND age = %s;
    """
    
select_price_from_movie = """\
    SELECT price
    FROM movie
    WHERE movie_id = %s;
    """
    
select_class_from_user = """\
    SELECT class
    FROM user
    WHERE user_id = %s;
    """
    
count_num_reservations = """\
    SELECT COUNT(DISTINCT user_id)
    FROM reservation
    GROUP BY movie_id
    HAVING movie_id = %s;
    """
    
select_users_for_movie = """\
    SELECT DISTINCT reservation.user_id, name, age, reservation_price, rating
    FROM reservation 
    JOIN user ON reservation.user_id = user.user_id
    LEFT OUTER JOIN rating ON reservation.movie_id = rating.movie_id AND reservation.user_id = rating.user_id
    WHERE reservation.movie_id = %s
    ORDER BY reservation.user_id;
    """
    
select_movies_for_user = """\
    SELECT DISTINCT reservation.movie_id, title, director, reservation_price, rating
    FROM reservation
    JOIN movie ON reservation.movie_id = movie.movie_id
    LEFT OUTER JOIN rating ON reservation.movie_id = rating.movie_id AND reservation.user_id = rating.user_id
    WHERE reservation.user_id = %s
    ORDER BY reservation.movie_id;
    """
    
check_movie_id = """\
    SELECT *
    FROM movie
    WHERE movie_id = %s;
    """
    
check_user_id = """\
    SELECT *
    FROM user
    WHERE user_id = %s;
    """

select_unseen = """\
    SELECT movie.movie_id, title, price, COUNT(DISTINCT reservation.user_id) as num_reservations, AVG(rating) as avg_rating
    FROM movie
    JOIN reservation USING (movie_id)
    LEFT OUTER JOIN rating USING (movie_id)
    WHERE NOT EXISTS (
        SELECT 1
        FROM reservation
        WHERE reservation.user_id = %s AND reservation.movie_id = movie.movie_id
    )
    GROUP BY movie_id
    """

select_highest_rating_movie = """\
    WITH unseen AS (
        SELECT movie.movie_id, title, price, COUNT(DISTINCT reservation.user_id) as num_reservations, AVG(rating) as avg_rating
        FROM movie
        JOIN reservation USING (movie_id)
        LEFT OUTER JOIN rating USING (movie_id)
        WHERE NOT EXISTS (
            SELECT 1
            FROM reservation
            WHERE reservation.user_id = %s AND reservation.movie_id = movie.movie_id
        )
        GROUP BY movie_id
    ),
    max_rating AS (
        SELECT MAX(avg_rating) as max_avg_rating FROM unseen
    )
    SELECT unseen.movie_id, unseen.title, unseen.price, unseen.num_reservations, unseen.avg_rating
    FROM unseen, max_rating
    WHERE unseen.avg_rating = max_rating.max_avg_rating
    ORDER BY unseen.movie_id
    LIMIT 1;
    """
    
select_most_popular_movie = """\
    WITH unseen AS (
        SELECT movie.movie_id, title, price, COUNT(DISTINCT reservation.user_id) as num_reservations, AVG(rating) as avg_rating
        FROM movie
        JOIN reservation USING (movie_id)
        LEFT OUTER JOIN rating USING (movie_id)
        WHERE NOT EXISTS (
            SELECT 1
            FROM reservation
            WHERE reservation.user_id = %s AND reservation.movie_id = movie.movie_id
        )
        GROUP BY movie_id
    ),
    max_reservations AS (
        SELECT MAX(num_reservations) as max_num_reservations FROM unseen
    )
    SELECT unseen.movie_id, unseen.title, unseen.price, unseen.num_reservations, unseen.avg_rating
    FROM unseen, max_reservations
    WHERE unseen.num_reservations = max_reservations.max_num_reservations
    ORDER BY unseen.movie_id
    LIMIT 1;
    """
   
count_num_movies = """\
    SELECT COUNT(DISTINCT movie_id)
    FROM movie;
    """

count_num_users = """\
    SELECT COUNT(DISTINCT user_id)
    FROM user;
    """
    
select_user_movie_rating_triples = """\
    SELECT user_id, movie_id, rating
    FROM rating
    WHERE user_id IN (
        SELECT user_id
        FROM rating
        GROUP BY user_id
        HAVING SUM(rating) > 0
    );
    """
    
select_movie_recommend_info = """\
    SELECT movie_id, title, price, AVG(rating) as avg_rating
    FROM movie
    LEFT OUTER JOIN rating USING (movie_id)
    WHERE movie_id IN (%(movies)s) 
        AND movie_id NOT IN (
            SELECT movie_id 
            FROM reservation 
            WHERE user_id = %(user_id)s
        )
    GROUP BY movie_id
    ORDER BY movie_id
    LIMIT %(top_k)s;
    """