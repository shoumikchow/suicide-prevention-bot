import json
import logging
import time
from os import environ

import requests
import tweepy
from textblob import TextBlob

BEARER = environ['bearer']
API_KEY = environ['api_key']
CONSUMER_SECRET = environ['consumer_secret']
ACCESS_TOKEN = environ['access_token']
ACCESS_TOKEN_SECRET = environ['access_token_secret']

KEYWORDS = [{
    'value': '"kill myself" -is:retweet -is:quote',
    'tag': 'killmyself'
}, {
    'value': '"killing myself" -is:retweet -is:quote',
    'tag': 'killingmyself'
}, {
    'value': '"i want to die" -is:retweet -is:quote',
    'tag': 'iwtd'
}]

DEFAULT_MESSAGE = 'If you are suicidal or depressed, please call 800-273-8255 or text "HOME" to 741741. If you are outside the US, please check https://www.befrienders.org/ to find a hotline number in your country. \u2665'

logging.basicConfig(filename='app.log',
                    filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')


def is_negative(tweet):
    t = TextBlob(tweet)
    if t.sentiment.polarity <= 0:
        return True


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def authorize_v1(api_key, api_secret, access_token, access_token_secret):
    auth = tweepy.OAuthHandler(api_key, api_secret)
    try:
        redirect_url = auth.get_authorization_url()
    except tweepy.TweepError:
        logging.exception('Error! Failed to get request token.')

    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    return api


def tweet_v1(api, text, text_id, author):
    if is_negative(text):
        api.update_status(status=f"@{author} {DEFAULT_MESSAGE}",
                          in_reply_to_status_id=text_id)


def get_rules(headers, bearer_token):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers)
    if response.status_code != 200:
        logging.error(
            f"Cannot get rules (HTTP {response.status_code}): {response.text}",
            exc_info=True)
        raise Exception(
            f"Cannot get rules (HTTP {response.status_code}): {response.text}")

    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(headers, bearer_token, rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload)
    if response.status_code != 200:
        logging.error(
            f"Cannot delete rules (HTTP {response.status_code}): {response.text}",
            exc_info=True)
        raise Exception(
            f"Cannot delete rules (HTTP {response.status_code}): {response.text}"
        )
    print(json.dumps(response.json()))


def set_rules(headers, delete, bearer_token):

    payload = {"add": KEYWORDS}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload,
    )
    if response.status_code != 201:
        logging.error(
            f"Cannot add rules (HTTP {response.status_code}): {response.text}",
            exc_info=True)
        raise Exception(
            f"Cannot add rules (HTTP {response.status_code}): {response.text}")
    print(json.dumps(response.json()))


def get_stream(headers, set, bearer_token, api):

    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream?expansions=author_id",
        headers=headers,
        stream=True,
    )
    print(response.status_code)

    if response.status_code != 200:
        if response.status_code == 429:
            print("Too many requests")
            time.sleep(900)
        else:
            logging.error(
                f"Cannot get stream (HTTP {response.status_code}): {response.text}",
                exc_info=True)
            raise Exception(
                f"Cannot get stream (HTTP {response.status_code}): {response.text}"
            )

    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            text = json_response['data']['text']
            text_id = json_response['data']['id']
            author = json_response['includes']['users'][0]['username']

            tweet_v1(api, text, text_id, author)
            print(f"@{author} {text}")
            print()


def main():

    bearer_token = BEARER
    api_key = API_KEY
    api_secret = API_SECRET
    access_token = ACCESS_TOKEN
    access_token_secret = ACCESS_TOKEN_SECRET

    headers = create_headers(bearer_token)
    api = authorize_v1(api_key, api_secret, access_token, access_token_secret)
    rules = get_rules(headers, bearer_token)
    delete = delete_all_rules(headers, bearer_token, rules)
    set_ = set_rules(headers, delete, bearer_token)
    get_stream(headers, set_, bearer_token, api)


if __name__ == "__main__":
    main()
