from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
import requests
from bs4 import BeautifulSoup  

def scrape_goodreads_data(url_list, output_file_name):
    # Define retry strategy
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["HEAD", "GET", "OPTIONS"])
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)

    # Create a session with the retry adapter
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    title = []
    authors = []
    avg_ratings = []
    rating = []
    year = []
    descriptions = []
    total_pages = []
    no_book_by_author = []
    author_followers = []
    genres = []
    no_of_reviews = []
    five_star_ratings = []
    four_star_ratings = []
    three_star_ratings = []
    two_star_ratings = []
    one_star_ratings = []

    for book_url in url_list: 
        try:
            book_response = session.get(book_url)
            book_soup = BeautifulSoup(book_response.content, "lxml")

            book_title = book_soup.find('h1').text.strip()
            author = book_soup.find('span', class_='ContributorLink__name').text.strip()
            rating_text = book_soup.find("div", class_="RatingStatistics__meta").text.strip().split()
            avg_rating = rating_text[0]
            ratings = rating_text[-2]
            description = book_soup.find("span", class_="Formatted").text.strip()
            total_page = book_soup.find("div", class_="BookDetails").find("p", {"data-testid": "pagesFormat"}).text.strip()
            one = book_soup.find("div", {"data-testid": "labelTotal-1"}).text.strip()
            two = book_soup.find("div", {"data-testid": "labelTotal-2"}).text.strip()
            three = book_soup.find("div", {"data-testid": "labelTotal-3"}).text.strip()
            four = book_soup.find("div", {"data-testid": "labelTotal-4"}).text.strip()
            five = book_soup.find("div", {"data-testid": "labelTotal-5"}).text.strip()
            reviews = book_soup.find("span", class_="u-dot-before").text.strip()
            genre = book_soup.find("ul", class_="CollapsableList").find("span", class_ ="Button__labelItem").text.strip()
            first_publish = book_soup.find('div', class_='FeaturedDetails').text.split('published')[-1].strip()
            no_books = book_soup.find("div", class_="AuthorPreview").find("span", class_="Text Text__body3 Text__subdued").text.split()[0].strip()
            followers = book_soup.find("div", class_="AuthorPreview").find("span", class_="Text Text__body3 Text__subdued").text.split('s')[-2].strip()

            title.append(book_title)
            authors.append(author)
            avg_ratings.append(avg_rating)
            rating.append(ratings)
            descriptions.append(description)
            year.append(first_publish)
            total_pages.append(total_page)
            no_of_reviews.append(reviews)
            five_star_ratings.append(five)
            four_star_ratings.append(four)
            three_star_ratings.append(three)
            two_star_ratings.append(two)
            one_star_ratings.append(one)
            no_book_by_author.append(no_books)
            author_followers.append(followers)
            genres.append(genre)

        except (Exception, IndexError) as e:
            print(f"Skipping a book due to error: {e}")
    
    good_reads = pd.DataFrame({
        "Title": title,
        "Authors": authors,
        "Avg Ratings": avg_ratings,
        "Rating": rating,
        "first_published": year,
        "Description": descriptions, 
        "Total_Pages": total_pages,
        "Genre": genres,
        "author_followers": author_followers,
        "no_of_reviews": no_of_reviews,
        "no_book_by_author": no_book_by_author,
        "one_star_ratings": one_star_ratings,
        "two_star_ratings": two_star_ratings,
        "three_star_ratings": three_star_ratings,
        "four_star_ratings": four_star_ratings,
        "five_star_ratings": five_star_ratings
    })

    try:
        good_reads.to_csv(output_file_name, index=False)
        return True
    except Exception as e:
        print(f"Error saving CSV file: {e}")
        return False

# Example usage:
url_list = [
    "https://www.goodreads.com/book/show/35852435-becoming-supernatural"    
]
output_file_name = "becoming_super_natural_book_details.csv"
success = scrape_goodreads_data(url_list, output_file_name)
if success:
    print("CSV file successfully created!")
else:
    print("Failed to create CSV file.")