import csv
from contextlib import contextmanager

import mysql.connector.errors as errors
import numpy as np
from mysql.connector import Error, connect, errorcode

from messages import *
from schema import DB_NAME, DEFAULT_DATA, TABLES
from sql_queries import *
from utils import *


class MovieBookingAgent:
    def __init__(self):
        pass
    
    @contextmanager
    def _connection(self):
        connection = connect(
            host="astronaut.snu.ac.kr",
            port=7000,
            user=DB_NAME,
            password=DB_NAME,
            db=DB_NAME,
            charset="utf8"
        )
        try:
            yield connection
        except:
            connection.rollback()
            raise
        else:
            connection.commit()
        finally:
            connection.close()
    
    
    @contextmanager
    def _cursor(self):
        with self._connection() as connection:
            cursor = connection.cursor(buffered=True)
            try:
                yield cursor
            finally:
                cursor.close()


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
                # TODO: check all constraints and then perform all insertions -> rollback이 있어서 안전?
                title, director, price = row["title"], row["director"], int(row["price"])
                name, age, class_ = row["name"], int(row["age"]), row["class"]
                try:
                    self.insert_movie(title, director, price)
                except MovieTitleAlreadyExistsError:
                    pass
                except MoviePriceError:  # ignore record
                    continue
                try:
                    self.insert_user(name, age, class_)
                except UserAlreadyExistsError:
                    pass
                except (UserAgeError, UserClassError):  # ignore record
                    continue
                
                with self._cursor() as cursor:
                    # ID may not be equal to the lastrowid because of the duplicate entries
                    cursor.execute(select_id_from_movie, (title,))
                    movie_id = cursor.fetchone()[0]
                    cursor.execute(select_id_from_user, (name, age))
                    user_id = cursor.fetchone()[0]
                
                try:
                    self.book_movie(movie_id, user_id, price, class_)
                except:
                    continue
                    
                
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
    def insert_movie(self, title, director, price):
        with self._cursor() as cursor:
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
    def insert_user(self, name, age, class_):
        with self._cursor() as cursor:
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
    def book_movie(self, movie_id, user_id, price=None, class_=None):
        with self._cursor() as cursor:
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
            try:
                cursor.execute(insert_into_rating, (movie_id, user_id, rating))
                # cursor.execute("SELECT * FROM rating;")
                # print(format_select_output(["movie_id", "user_id", "rating"], cursor.fetchall()))
            except Error as err:
                # print(str(err))
                if err.errno == errorcode.ER_DUP_ENTRY:
                    raise UserAlreadyRatedError(user_id, movie_id)
                elif err.errno in (errorcode.ER_NO_REFERENCED_ROW, errorcode.ER_NO_REFERENCED_ROW_2):
                    if "`movie`" in str(err):
                        raise MovieNotExistError(movie_id)
                    elif "`user`" in str(err):
                        raise UserNotExistError(user_id)
                    elif "`reservation`" in str(err):
                        raise UserNotBookedError(user_id, movie_id)
                elif err.errno == errorcode.ER_CHECK_CONSTRAINT_VIOLATED:
                    raise RatingError()
            
        return MovieRateSuccess()


    # # Problem 10 (5 pt.)
    def print_users_for_movie(self, movie_id):
        headers = ["id", "name", "age", "res. price", "rating"]
        with self._cursor() as cursor:
            cursor.execute(check_movie_id, (movie_id,))
            if len(cursor.fetchall()) == 0:
                raise MovieNotExistError(movie_id)
            
            cursor.execute(select_users_for_movie, (movie_id,))
            records = cursor.fetchall()
        
        return format_select_output(headers, records)


    # Problem 11 (5 pt.)
    def print_movies_for_user(self, user_id):
        headers = ["id", "title", "director", "res. price", "rating"]
        with self._cursor() as cursor:
            cursor.execute(check_user_id, (user_id,))
            if len(cursor.fetchall()) == 0:
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
            if len(cursor.fetchall()) == 0:
                raise UserNotExistError(user_id)
            
            cursor.execute(select_class_from_user, (user_id,))
            user_class = cursor.fetchone()[0]
            
            # cursor.execute(select_unseen, (user_id,))
            print("Unseen movies:")
            print(cursor.fetchall())
            cursor.execute(select_highest_rating_movie, (user_id,))
            highest_rating_record = self._replace_reservation_price(cursor.fetchone(), user_class)
            output += format_select_output(headers, [highest_rating_record], title="Rating-based")
            output += "\n"
            cursor.execute(select_most_popular_movie, (user_id,))
            most_popular_record = self._replace_reservation_price(cursor.fetchone(), user_class)
            output += format_select_output(headers, [most_popular_record], title="Popularity-based")
        
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
            if len(cursor.fetchall()) == 0:
                raise UserNotExistError(user_id)
            
            cursor.execute(select_all_from_user)
            user_ids = [record[0] for record in cursor.fetchall()]
            cursor.execute(select_all_from_movie)
            movie_ids = [record[0] for record in cursor.fetchall()]
            
            cursor.execute(select_user_movie_rating_triples)
            user_movie_ratings = cursor.fetchall()
            
            # cursor.execute("SELECT * FROM rating;")
            # print(format_select_output(["movie_id", "user_id", "rating"], cursor.fetchall()))
        
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
            
        not_rated_indices = np.where(matrix[user_idx] == 0)[0]
        estimated_ratings = list(map(weighted_average, not_rated_indices))
        # print("Estimated ratings:", estimated_ratings)
        estimated_ratings_dict = dict(sorted(estimated_ratings, key=lambda x: (x[1], x[0]), reverse=True))  # (movie_id, estimated_rating)
        # print("Top k movie rating:", estimated_ratings_dict)
        candidate_movies = ', '.join(list(map(str, estimated_ratings_dict.keys())))
        
        with self._cursor() as cursor:
            cursor.execute(select_class_from_user, (user_id,))
            user_class = cursor.fetchone()[0]
            cursor.execute(select_movie_recommend_info, {"movies": candidate_movies, "user_id": user_id, "top_k": k})
            movie_records = cursor.fetchall()
            # print("Movie records:", movie_records)
        final_records = []
        for record in movie_records:
            record = self._replace_reservation_price(record, user_class)
            record += (estimated_ratings_dict[record[0]],)  # append estimated rating (record[0] is movie_id)
            final_records.append(record)
        final_records = sorted(final_records, key=lambda x: (-x[4], x[0]))  # first sort by estimated rating, then by movie_id
        
        return format_select_output(headers, final_records)