from agent import MovieBookingAgent
from messages import *
import traceback


SUBMISSION = False


# Total of 70 pt.
def main():
    agent = MovieBookingAgent()
    
    if SUBMISSION:
        confirmation = input("Are you sure to reset the database? (y/n): ")
        created_tables = agent.reset(confirmation, only_create_tables=True)
        print("Created tables:", created_tables)
        return
    
    while True:
        print('============================================================')
        print('1. initialize database')
        print('2. print all movies')
        print('3. print all users')
        print('4. insert a new movie')
        print('5. remove a movie')
        print('6. insert a new user')
        print('7. remove an user')
        print('8. book a movie')
        print('9. rate a movie')
        print('10. print all users who booked for a movie')
        print('11. print all movies rated by an user')
        print('12. recommend a movie for a user using popularity-based method')
        print('13. recommend a movie for a user using item-based collaborative filtering')
        print('14. exit')
        print('15. reset database')
        print('============================================================')
        menu = int(input('Select your action: '))

        try:
            if menu == 1:
                result = agent.initialize_database()
                
            elif menu == 2:
                result = agent.print_movies()
                
            elif menu == 3:
                result = agent.print_users()
                
            elif menu == 4:
                title = input("Movie title: ")
                director = input("Movie director: ")
                price = input("Movie price: ")
                result = agent.insert_movie(title, director, price)
                
            elif menu == 5:
                movie_id = input("Movie ID: ")
                result = agent.remove_movie(movie_id)
            
            elif menu == 6:
                name = input("User name: ")
                age = input("User age: ")
                class_ = input("User class: ")
                result = agent.insert_user(name, age, class_)
                
            elif menu == 7:
                user_id = input("User ID: ")
                result = agent.remove_user(user_id)
            
            elif menu == 8:
                movie_id = input("Movie ID: ")
                user_id = input("User ID: ")
                result = agent.book_movie(movie_id, user_id)
                
            elif menu == 9:
                movie_id = input("Movie ID: ")
                user_id = input("User ID: ")
                rating = input("Ratings (1~5): ")
                result = agent.rate_movie(movie_id, user_id, rating)
                
            elif menu == 10:
                movie_id = input("Movie ID: ")
                result = agent.print_users_for_movie(movie_id)
                
            elif menu == 11:
                user_id = input("User ID: ")
                result = agent.print_movies_for_user(user_id)
                
            elif menu == 12:
                user_id = input("User ID: ")
                result = agent.recommend_popularity(user_id)
                
            elif menu == 13:
                user_id = input("User ID: ")
                n = input("Number of movies to recommend: ")
                result = agent.recommend_item_based(int(user_id), int(n))
            elif menu == 14:
                print("Bye!")
                break
            elif menu == 15:
                confirmation = input("Are you sure to reset the database? (y/n): ")
                result = agent.reset(confirmation)
            else:
                raise InvalidActionError()
        except (CustomBaseException, InvalidActionError) as e:
            print(e)
        # except:
        #     print(traceback.format_exc())
        #     break
        else:
            if result:
                print(result)
    
    agent.terminate()

if __name__ == "__main__":
    main()