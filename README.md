# Simple Movie Database Application with Python and MySQL

This project aims to build and manipulate a movie database using Python and MySQL. The application simulates a movie booking system, allowing users to browse movies, add or remove entries, and even make reservations.

## Requirements
- Python 3.6+
- MySQL
  - The project's DB was provided by `astronaut.snu.ac.kr`; you will need to connect your own MySQL account.

## Instructions
1. Install dependencies:
```
pip install -r requirements.txt
```
This will install `mysql-connector-python` and `numpy`.

2. Run the application:
```
python run.py
```

3. Input a number to interact with the application.
Provided features are:
```
1. initialize database
2. print all movies
3. print all users
4. insert a new movie 5. remove a movie
6. insert a new user 7. remove a user
8. book a movie
9. rate a movie
10. print all users who booked for a movie
11. print all movies booked by a user
12. recommend a movie for a user using popularity-based method
13. recommend a movie for a user using item-based collaborative filtering 
14. exit
```

Sample user input for action 4:
```
Select your action: 4
Movie title: Alien
Movie director: Ridley Scott 
Movie price: 10000
```

Sample user input for action 6:
```
Select your action: 6 
User name: My User Name 
User age: 19
User class: basic
```

Sample user input for action 9:
```
Select your action: 9 
Movie ID: 3
User ID: 1
Ratings (1~5): 4
```

Sample user input for action 13:
```
Select your action: 13
User ID: 2
Recommend Count: 1
```


## Core Modules
- `run.py`: Provides the interface between the user and the application. It takes in inputs from the user for desired operations and arguments and executes them accordingly.

- `schema.py`: Defines the `CREATE TABLE` statements to set up the database schema. Tables created are: `movie`, `user`, `reservation`, and `rating`, with `reservation` dependent on `movie` and `user` tables, and `rating` dependent on `reservation`.

- `agent.py`: Implements 15 different operations as requested by the user. These include database initialization, displaying all movie or user information, adding or deleting movie or user entries, making reservations, and providing movie recommendations via item-based collaborative filtering.

- `sql_queries.py`: Contains all SQL queries necessary for the application based on the defined schema.

- `utils.py`: Offers utility functions for formatting SQL `SELECT` query outputs, calculating ticket prices based on movie prices and customer classes, and defines an `Enum` class for discount rates.

- `messages.py`: Defines exception classes to output log and error messages when an operation is successfully performed or fails.

## Implementation Details
- `schema.py`: Utilizes various `CREATE TABLE` options to enforce conditions like uniqueness constraints (`UNIQUE`), data type constraints (`ENUM`), and checks (`CHECK`).

- `agent.py`: Uses Python's native `contextlib` to manage database connections and cursors efficiently. It also includes a retry mechanism for intermittent server issues and handles SQL errors gracefully.

Note that while reading `data.csv`, if there are duplicate movie titles with different directors or prices, or duplicate customer names with different ages or classes, the application will assume these as valid and unique entries.

---
This project was done as part of Spring 2023 Database M1522.001800 course of Seoul National University.
