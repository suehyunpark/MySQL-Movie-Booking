import csv
from contextlib import contextmanager

import mysql.connector.errors as errors
import numpy as np
from mysql.connector import Error, connect, errorcode
from time import sleep

from messages import *
from schema import DB_NAME, TABLES
from sql_queries import *
from utils import *

DEFAULT_DATA = "data.csv"
NUM_TRIES = 3

class SQLConnector:
    def __init__(self):
        for _ in range(NUM_TRIES):
            try:
                self._connect()
            except:
                sleep(3)
                continue
            else:
                break


    def _connect(self):
        self.connection = connect(
            host="astronaut.snu.ac.kr",
            port=7000,
            user=DB_NAME,
            password=DB_NAME,
            db=DB_NAME,
            charset="utf8",
            connection_timeout=3
        )
        
    
    def _reconnect(self):
        self.connection.reconnect(attempts=3, delay=5)
        
    
    @contextmanager
    def _connection(self):
        try:
            yield self.connection
        except errors.OperationalError as e:
            if e.errno == errorcode.CR_SERVER_LOST:
                self._reconnect()
                yield self.connection
        except:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()
    
    
    @contextmanager
    def _connection_without_halt(self):
        try:
            yield self.connection
        except errors.OperationalError as e:
            if e.errno == errorcode.CR_SERVER_LOST:
                self._reconnect()
                yield self.connection
        except:
            self.connection.rollback()
        else:
            self.connection.commit()
    
    
    @contextmanager
    def _cursor(self):
        with self._connection() as connection:
            cursor = connection.cursor(buffered=True)
            try:
                yield cursor
            finally:
                cursor.close()
                
                
    @contextmanager
    def _optional_cursor(self, cursor=None):
        if cursor is None:
            with self._cursor() as new_cursor:
                yield new_cursor
        else:
            yield cursor
                
                
    def terminate(self):
        self.connection.close()
        
        

class MovieBookingAgent(SQLConnector):
    def __init__(self):
        super().__init__()
    

    # Problem 1 (5 pt.)
    def initialize_database(self, only_create_tables=False):
        with self._cursor() as cursor:
            cursor.execute(check_database_empty)
            num_tables = cursor.fetchone()[0]
            if num_tables == 0:
                for create_table_query in TABLES.values():
                    cursor.execute(create_table_query)
                
            if only_create_tables:
                cursor.execute("SHOW TABLES")
                all_tables = cursor.fetchall()
                return all_tables  # for debugging

        with open(DEFAULT_DATA) as csvfile:
            reader = csv.DictReader(csvfile)
            for i, row in enumerate(reader):
                with self._connection_without_halt() as connection:
                    cursor = connection.cursor(buffered=True)
                    title, director, price = row["title"], row["director"], int(row["price"])
                    name, age, class_ = row["name"], int(row["age"]), row["class"]
                    try:
                        self.insert_movie(title, director, price, cursor)
                    except MovieTitleAlreadyExistsError:  # note that each record is a reservation
                        pass
                    try:
                        self.insert_user(name, age, class_, cursor)
                    except UserAlreadyExistsError:
                        pass
                    
                    # ID may not be equal to the lastrowid because of the duplicate entries
                    cursor.execute(select_id_from_movie, (title,))
                    movie_id = cursor.fetchone()[0]
                    cursor.execute(select_id_from_user, (name, age))
                    user_id = cursor.fetchone()[0]
                    
                    self.book_movie(movie_id, user_id, price, class_, cursor)
                
        return DatabaseInitializeSuccess()
    
    
    # Problem 15 (5 pt.)
    def reset(self, confirmation, only_create_tables=False):
        if confirmation == 'n':
            return None
        
        with self._cursor() as cursor:
            cursor.execute(drop_all_tables)
            
        return self.initialize_database(only_create_tables)


    # Problem 2 (4 pt.)
    def print_movies(self):
        headers = ["id", "title", "director", "price", "avg. price", "reservation", "avg. rating"]
        with self._cursor() as cursor:
            cursor.execute(select_all_from_movie)
            records = cursor.fetchall()
        
        return format_select_output(headers, records)


    # Problem 3 (3 pt.)
    def print_users(self):
        headers = ["id", "name", "age", "class"]
        with self._cursor() as cursor:
            cursor.execute(select_all_from_user)
            records = cursor.fetchall()
            
        return format_select_output(headers, records)


    # Problem 4 (4 pt.)
    def insert_movie(self, title, director, price, cursor=None):
        with self._optional_cursor(cursor) as cursor:
            try:
                cursor.execute(insert_into_movie, (title, director, price))
            except Error as err:
                if err.errno == errorcode.ER_DUP_ENTRY:
                    raise MovieTitleAlreadyExistsError(title)
                elif err.errno == errorcode.ER_CHECK_CONSTRAINT_VIOLATED:
                    raise MoviePriceError()
        
        return MovieInsertSuccess()


    # Problem 6 (4 pt.)
    def remove_movie(self, movie_id):
        with self._cursor() as cursor:
            cursor.execute(delete_from_movie, (movie_id,))
            if cursor.rowcount == 0:
                raise MovieNotExistError(movie_id)
        
        return MovieRemoveSuccess()


    # Problem 5 (4 pt.)
    def insert_user(self, name, age, class_, cursor=None):
        with self._optional_cursor(cursor) as cursor:
            try:
                cursor.execute(insert_into_user, (name, age, class_))
            except Error as err:
                if err.errno == errorcode.ER_DUP_ENTRY:
                    raise UserAlreadyExistsError(name, age)
                elif err.errno == errorcode.ER_CHECK_CONSTRAINT_VIOLATED:
                    raise UserAgeError()
                elif err.errno == errorcode.WARN_DATA_TRUNCATED:  # unallowed value for an ENUM field
                    raise UserClassError()
        
        return UserInsertSuccess()


    # Problem 7 (4 pt.)
    def remove_user(self, user_id):
        with self._cursor() as cursor:
            cursor.execute(delete_from_user, (user_id,))
            if cursor.rowcount == 0:
                raise UserNotExistError(user_id)
        
        return UserRemoveSuccess()


    # Problem 8 (5 pt.)
    def book_movie(self, movie_id, user_id, price=None, class_=None, cursor=None):
        with self._optional_cursor(cursor) as cursor:
            cursor.execute(count_num_reservations, (movie_id,))
            num_reservations = cursor.fetchall()
            if num_reservations and num_reservations[0][0] == 10:
                raise MovieFullyBookedError(movie_id)

            if price:
                orig_price = price
            else:
                cursor.execute(select_price_from_movie, (movie_id,))
                orig_price_ = cursor.fetchone()
                if orig_price_ is None:
                    raise MovieNotExistError(movie_id)
                orig_price = orig_price_[0]  # (price,)
            if class_:
                user_class = class_
            else:
                cursor.execute(select_class_from_user, (user_id,))
                user_class_ = cursor.fetchone()
                if user_class_ is None:
                    raise UserNotExistError(user_id)
                user_class = user_class_[0]  # (class,)
            reservation_price = calculate_reservation_price(orig_price, user_class)
            
            try:
                cursor.execute(insert_into_reservation, (movie_id, user_id, reservation_price))
            except Error as err:
                if err.errno == errorcode.ER_DUP_ENTRY:
                    raise MovieAlreadyBookedError(user_id, movie_id)
                elif err.errno == errorcode.ER_NO_REFERENCED_ROW:
                    if "`movie`" in str(err):
                        raise MovieNotExistError(movie_id)
                    elif "`user`" in str(err):
                        raise UserNotExistError(user_id)
        
        return MovieBookSuccess()


    # Problem 9 (5 pt.)
    def rate_movie(self, movie_id, user_id, rating):
        with self._cursor() as cursor:
            cursor.execute(check_movie_id, (movie_id,))
            movie_exists = cursor.fetchone()
            if not movie_exists:
                raise MovieNotExistError(movie_id)
            
            cursor.execute(check_user_id, (user_id,))
            user_exists = cursor.fetchone()
            if not user_exists:
                raise UserNotExistError(user_id)
            
            try:
                cursor.execute(insert_into_rating, (movie_id, user_id, rating))
            except Error as err:
                if err.errno in (errorcode.ER_NO_REFERENCED_ROW, errorcode.ER_NO_REFERENCED_ROW_2):
                    raise UserNotBookedError(user_id, movie_id)
                elif err.errno == errorcode.ER_DUP_ENTRY:
                    raise UserAlreadyRatedError(user_id, movie_id)
                elif err.errno == errorcode.ER_CHECK_CONSTRAINT_VIOLATED:
                    raise RatingError()
            
        return MovieRateSuccess()


    # # Problem 10 (5 pt.)
    def print_users_for_movie(self, movie_id):
        headers = ["id", "name", "age", "res. price", "rating"]
        with self._cursor() as cursor:
            cursor.execute(check_movie_id, (movie_id,))
            movie_exists = cursor.fetchone()
            if not movie_exists:
                raise MovieNotExistError(movie_id)
            
            cursor.execute(select_users_for_movie, (movie_id,))
            records = cursor.fetchall()
        
        return format_select_output(headers, records)


    # Problem 11 (5 pt.)
    def print_movies_for_user(self, user_id):
        headers = ["id", "title", "director", "res. price", "rating"]
        with self._cursor() as cursor:
            cursor.execute(check_user_id, (user_id,))
            user_exists = cursor.fetchone()
            if not user_exists:
                raise UserNotExistError(user_id)
            
            cursor.execute(select_movies_for_user, (user_id,))
            records = cursor.fetchall()
        
        return format_select_output(headers, records)


    # Problem 12 (6 pt.)
    def recommend_popularity(self, user_id):
        headers = ["id", "title", "res. price", "reservation", "avg. rating"]
        output = ""
        
        with self._cursor() as cursor:
            cursor.execute(check_user_id, (user_id,))
            user_exists = cursor.fetchone()
            if not user_exists:
                raise UserNotExistError(user_id)
            
            cursor.execute(select_class_from_user, (user_id,))
            user_class = cursor.fetchone()[0]
            
            cursor.execute(select_highest_rating_movie, (user_id,))
            result1 = cursor.fetchone()
            highest_rating_record = [self._replace_reservation_price(result1, user_class)] if result1 else []
            output += format_select_output(headers, highest_rating_record, title="Rating-based")
            output += "\n"
            cursor.execute(select_most_popular_movie, (user_id,))
            result2 = cursor.fetchone()
            most_popular_record = [self._replace_reservation_price(result2, user_class)] if result2 else []
            output += format_select_output(headers, most_popular_record, title="Popularity-based")
        
        return output
    
    
    def _replace_reservation_price(self, record, user_class):
        orig_price = record[2]
        reservation_price = calculate_reservation_price(orig_price, user_class)
        return record[:2] + (reservation_price,) + record[3:]


    # Problem 13 (10 pt.)
    def recommend_item_based(self, user_id, k):
        headers = ["id", "title", "res. price", "avg. rating", "expected rating"]
        
        with self._cursor() as cursor:
            cursor.execute(check_user_id, (user_id,))
            user_exists = cursor.fetchone()
            if not user_exists:
                raise UserNotExistError(user_id)
            
            cursor.execute(select_all_from_user)
            user_ids = [record[0] for record in cursor.fetchall()]
            cursor.execute(select_all_from_movie)
            movie_ids = [record[0] for record in cursor.fetchall()]
            
            cursor.execute(select_user_movie_rating_triples)
            user_movie_ratings = cursor.fetchall()
            
        matrix = np.zeros((len(user_ids), len(movie_ids)))
        user_has_ratings = False
        for user_id_, movie_id, rating in user_movie_ratings:
            user_has_ratings = (user_has_ratings or user_id_ == user_id)
            user_idx = user_ids.index(user_id_)
            movie_idx = movie_ids.index(movie_id)
            matrix[user_idx, movie_idx] = rating
        if not user_has_ratings:
            raise RatingNotExistError()
        
        zeros = matrix == 0
        means = np.around(np.ma.array(matrix, mask=zeros).mean(axis=0), 2)
        filled_matrix = matrix + zeros * means.data

        mu = np.around(np.mean(filled_matrix), 4)
        diff = filled_matrix - mu
        dot_product = np.dot(diff.T, diff)
        norms = np.linalg.norm(diff, axis=0)
        similarity_matrix = np.around(dot_product / np.outer(norms, norms), 4)

        user_idx = user_ids.index(user_id)
        def weighted_average(movie_idx):
            movie_id = movie_ids[movie_idx]
            user_ratings = filled_matrix[user_idx]
            user_ratings = np.delete(user_ratings, movie_idx)
            similarity_weights = similarity_matrix[movie_idx]
            similarity_weights = np.delete(similarity_weights, movie_idx)
            estimated_rating = np.round(np.sum(user_ratings * similarity_weights) / np.sum(similarity_weights), 4)
            return movie_id, estimated_rating
        
        # Compute estimated ratings for movies that the user has not rated
        not_rated_indices = np.where(matrix[user_idx] == 0)[0]
        estimated_ratings_dict = dict(map(weighted_average, not_rated_indices))  # movie_id: estimated_rating
        placeholders = ', '.join(['%s'] * len(estimated_ratings_dict))
        
        with self._cursor() as cursor:
            cursor.execute(select_class_from_user, (user_id,))
            user_class = cursor.fetchone()[0]
            cursor.execute(select_movie_recommend_info.format(placeholders=placeholders), tuple(estimated_ratings_dict.keys()) + (user_id,))  # candidate movies that user has already seen is filtered out
            movie_records = cursor.fetchall()
        movie_with_estimated_rating_records = []
        for record in movie_records:
            record = self._replace_reservation_price(record, user_class)
            record += (estimated_ratings_dict[record[0]],)  # append estimated rating (record[0] is movie_id)
            movie_with_estimated_rating_records.append(record)
        # Sort by estimated rating, then by movie_id, and select top k
        top_k_records = sorted(movie_with_estimated_rating_records, key=lambda x: (-x[4], x[0]))[:k]
        
        return format_select_output(headers, top_k_records)