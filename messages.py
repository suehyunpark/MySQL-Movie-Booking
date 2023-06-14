# ---------------------------------------------------------------------------- #
#                       Success messages in DBMS                               #
# ---------------------------------------------------------------------------- #

class SuccessLog:
    """Class that contains the messages for a successful operation."""
    def __init__(self, message):
        self.message = message
        
    def __str__(self):
        return self.message


class DatabaseInitializeSuccess(SuccessLog):
    def __init__(self):
        super().__init__("Database successfully initialized")

    
class UserInsertSuccess(SuccessLog):
    def __init__(self):
        super().__init__("One user successfully inserted")
        

class MovieInsertSuccess(SuccessLog):
    def __init__(self):
        super().__init__("One movie successfully inserted")
        

class UserRemoveSuccess(SuccessLog):
    def __init__(self):
        super().__init__("One user successfully removed")
        
        
class MovieRemoveSuccess(SuccessLog):
    def __init__(self):
        super().__init__("One movie successfully removed")
        
    
class MovieBookSuccess(SuccessLog):
    def __init__(self):
        super().__init__("Movie successfully booked")
        
        
class MovieRateSuccess(SuccessLog):
    def __init__(self):
        super().__init__("Movie successfully rated")
        

# ---------------------------------------------------------------------------- #
#                       Failure messages in DBMS                               #
# ---------------------------------------------------------------------------- #


class CustomBaseException(Exception):
    """Base class for custom exceptions."""
    pass


class MovieTitleAlreadyExistsError(CustomBaseException):
    """Raised when the movie title already exists."""
    def __init__(self, title):
        super().__init__(f"The movie {title} already exists")
        

class UserAgeError(CustomBaseException):
    """Raised when the user age is not in the range of 12 to 110."""
    def __init__(self):
        super().__init__("User age should be from 12 to 110")
        
        
class UserClassError(CustomBaseException):
    """Raised when the user class is not in the range of basic, premium, vip."""
    def __init__(self):
        super().__init__("User class should be basic, premium or vip")
        
        
class UserAlreadyExistsError(CustomBaseException):
    """Raised when the user already exists."""
    def __init__(self, name, age):
        super().__init__(f"The user ({name}, {age}) already exists")
        
        
class MovieNotExistError(CustomBaseException):
    """Raised when the movie does not exist."""
    def __init__(self, movie_id):
        super().__init__(f"Movie {movie_id} does not exist")
        
        
class UserNotExistError(CustomBaseException):
    """Raised when the user does not exist."""
    def __init__(self, user_id):
        super().__init__(f"User {user_id} does not exist")
        

class MoviePriceError(CustomBaseException):
    """Raised when the movie price is not in the range of 0 to 100000."""
    def __init__(self):
        super().__init__("Movie price should be from 0 to 100000")


class MovieAlreadyBookedError(CustomBaseException):
    """Raised when the user has already booked the movie."""
    def __init__(self, user_id, movie_id):
        super().__init__(f"User {user_id} has already booked movie {movie_id}")
        
        
class MovieFullyBookedError(CustomBaseException):
    """Raised when the movie is fully booked."""
    def __init__(self, movie_id):
        super().__init__(f"Movie {movie_id} has already been fully booked")


class UserNotBookedError(CustomBaseException):
    """Raised when the user has not booked the movie."""
    def __init__(self, user_id, movie_id):
        super().__init__(f"User {user_id} has not booked movie {movie_id} yet")

        
class UserAlreadyRatedError(CustomBaseException):
    """Raised when the user has already rated the movie."""
    def __init__(self, user_id, movie_id):
        super().__init__(f"User {user_id} has already rated movie {movie_id}")
        
        
class RatingNotExistError(CustomBaseException):
    """Raised when the rating does not exist."""
    def __init__(self):
        super().__init__("Rating does not exist")
        

class RatingError(CustomBaseException):
    """Raised when the rating is not in the range of 1 to 5."""
    def __init__(self):
        super().__init__("Wrong value for a rating")
        
       
class InvalidActionError(CustomBaseException):
    """Raised when the action is invalid, i.e., menu other than 1-15 is selected."""
    def __init__(self):
        super().__init__("Invalid action")