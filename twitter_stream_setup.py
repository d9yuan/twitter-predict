import socket
import sys
import requests
import json
import yaml

config = yaml.safe_load(open("./config.yml"))

bearer_token = config['auth']['twitter']['BEAR_TOKEN']

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r

def get_tweets():
	url = 'https://api.twitter.com/2/tweets/search/stream'
	response = requests.get(url, auth=bearer_oauth, stream=True)
	return response


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(
                response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def set_rules(delete):
    rules = [
        {"value": "bitcoin lang:en is:verified lang:en -is:retweet",
        "tag": "bitcoin tweets verified"}
    ]
    payload = {"add": rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(
                response.status_code, response.text)
        )
    print(json.dumps(response.json()))

def send_tweets_to_spark(http_resp, tcp_connection):
	for line in http_resp.iter_lines():
            try:
                if line != b'':
                    full_tweet = json.loads(line)
                    tweet_text = full_tweet['data']['text']
                    tcp_connection.send(tweet_text.encode())
            except:
                e = sys.exc_info()[0]
                print("Error: %s" % e)


TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
rules = get_rules()
delete = delete_all_rules(rules)
set = set_rules(delete)
resp = get_tweets()
send_tweets_to_spark(resp, conn)
