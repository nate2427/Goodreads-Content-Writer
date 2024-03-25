import requests
import json
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()

def extract_review_data(book_id, num_pages, api_key):
    # GraphQL endpoint URL
    url = 'https://kxbwmqov6jgg3daaamb744ycu4.appsync-api.us-east-1.amazonaws.com/graphql'

    # Initialize lists to store data
    reviewer_names = []
    reviewer_avatars = []
    comment_texts = []
    creation_dates = []
    ratings = []
    like_counts = []
    profile_urls = []
    shelf_names = []
    tag_names = []

    # GraphQL query variables
    variables = {
        "filters": {
            "resourceType": "WORK",
            "resourceId": f"kca://work/amzn1.gr.work.v1.{book_id}"
        },
        "pagination": {
            "limit": 30
        }
    }

    # GraphQL query
    query = """
        query getReviews($filters: BookReviewsFilterInput!, $pagination: PaginationInput) {
          getReviews(filters: $filters, pagination: $pagination) {
            ...BookReviewsFragment
            __typename
          }
        }

        fragment BookReviewsFragment on BookReviewsConnection {
          totalCount
          edges {
            node {
              ...ReviewCardFragment
              __typename
            }
            __typename
          }
          pageInfo {
            prevPageToken
            nextPageToken
            __typename
          }
          __typename
        }

        fragment ReviewCardFragment on Review {
          __typename
          id
          creator {
            ...ReviewerProfileFragment
            __typename
          }
          recommendFor
          updatedAt
          createdAt
          spoilerStatus
          lastRevisionAt
          text
          rating
          shelving {
            shelf {
              name
              webUrl
              __typename
            }
            taggings {
              tag {
                name
                webUrl
                __typename
              }
              __typename
            }
            webUrl
            __typename
          }
          likeCount
          viewerHasLiked
          commentCount
        }

        fragment ReviewerProfileFragment on User {
          id: legacyId
          imageUrlSquare
          isAuthor
          ...SocialUserFragment
          textReviewsCount
          viewerRelationshipStatus {
            isBlockedByViewer
            __typename
          }
          name
          webUrl
          contributor {
            id
            works {
              totalCount
              __typename
            }
            __typename
          }
          __typename
        }

        fragment SocialUserFragment on User {
          viewerRelationshipStatus {
            isFollowing
            isFriend
            __typename
          }
          followersCount
          __typename
        }
    """

    # Headers
    headers = {
        "Content-Type": "application/json",
        "X-Api-Key": api_key
    }

    # Loop through the specified number of pages
    for page in range(num_pages):
      print(f'Scraping Page {page}\n')
      # Add page token for pagination
      if page > 0:
          variables["pagination"]["after"] = page_token

      # GraphQL request
      response = requests.post(url, json={
          "operationName": "getReviews",
          "variables": variables,
          "query": query
      }, headers=headers)  # Include headers in the request

      # Parse response
      response_data = response.json()

      # Extract data from current page
      reviews = response_data["data"]["getReviews"]["edges"]

      # Extract data from reviews
      for review in reviews:
          node = review["node"]
          reviewer_names.append(node["creator"]["name"])
          reviewer_avatars.append(node["creator"]["imageUrlSquare"])
          comment_texts.append(node["text"])
          creation_dates.append(node["createdAt"])
          ratings.append(node["rating"])
          like_counts.append(node["likeCount"])
          profile_urls.append(node["creator"]["webUrl"])
          shelf_names.append(node["shelving"]["shelf"]["name"])
          tags = [tag['tag']["name"] for tag in node["shelving"]["taggings"]]
          tag_names.append(tags)


      # Check for next page token
      page_token = response_data["data"]["getReviews"]["pageInfo"].get("nextPageToken")

      # Break loop if there are no more pages
      if not page_token:
          break

    # Create DataFrame
    df = pd.DataFrame({
        "Reviewer Name": reviewer_names,
        "Comment": comment_texts,
        "Creation Date": creation_dates,
        "Rating": ratings,
        "Number of Likes": like_counts,
        "Profile URL": profile_urls,
        "Shelf Name": shelf_names,
        "Tag Names": tag_names
    })

    return df

if __name__ == "__main__":
  # book_id -> 5NB0xZVy7hlyaO9xdCptrg (find in url of book review page)
  book_id = input("Enter the book ID: ")
  output_file_name = input("Enter name of output CSV: ")
  num_pages = int(input("Enter the number of pages to extract: "))
  api_key = os.getenv("GOODREADS_API_KEY")

  review_data = extract_review_data(book_id, num_pages, api_key)
  print(review_data.head())
  try:
      review_data.to_csv(output_file_name)
      print("Successfully scraped and saved the reviews.")
  except:
      print("Error saving scraped data.")