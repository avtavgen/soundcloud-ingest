import requests
import sys
from time import sleep
from random import randint
from datetime import datetime


CATEGORIES = ["alternativerock", "ambient", "classical", "country", "dancehall", "disco", "danceedm", "deephouse",
              "drumbass", "dubstep", "electronic", "house", "indie", "latin", "metal", "piano", "pop", "reggae",
              "reggaeton", "rock", "soundtrack", "techno", "trance", "trap", "triphop", "world", "rbsoul", "jazzblues",
              "hiphoprap", "folksingersongwriter", "all-music"]

KIND = ["trending"]


class SoundcloudProcessor(object):
    def __init__(self, entity, log, retry=3):
        self.log = log
        self.retry = retry
        self.entity = entity
        self.next = None
        self.base_url = "https://api-v2.soundcloud.com/"
        self.headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) '
                                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

    def _make_request(self, url, next=None):
        if next:
            url = next
        retries = 0
        while retries <= self.retry:
            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                self.log.info("{}".format(e))
                sleep(60)
                retries += 1
                if retries <= self.retry:
                    self.log.info("Trying again!")
                    continue
                else:
                    sys.exit("Max retries reached")
            except Exception as e:
                self.log.info("{}: Failed to make Soundcloud request on try {}".format(e, retries))
                retries += 1
                if retries <= self.retry:
                    self.log.info("Trying again!")
                    continue
                else:
                    sys.exit("Max retries reached")

    def _get_tracks(self):
        for category in CATEGORIES:
            self.users = []
            self.tracks = []
            for kind in KIND:
                response = self._make_request("{}charts?kind={}&genre=soundcloud%3Agenres%3A{}&high_tier_only=false"
                                              "&client_id=PmqbpuYsHUQ7ZYrW6qUlPcdpVFETRzc0&limit=200&offset=0"
                                              "&linked_partitioning=1&app_version=1533891405"
                                              "&app_locale=en".format(self.base_url, kind, category))
                track_list = response["collection"]
                self.log.info("Collecting {} tracks form {} category, {} kind.".format(len(track_list), category, kind))
                for track in track_list:
                    try:
                        track_data, user_data = self._get_info(track)
                        self.tracks.append(track_data)
                        self.users.append(user_data)
                        # sleep(randint(6, 8))
                    except Exception as e:
                        self.log.info("Failed to fetch data: {}".format(e))
                        continue
            self.entity.save(tracks=self.tracks, users=self.users)

    def _get_info(self, track):
        track_data = dict()
        track_data["url"] = track["track"]["permalink_url"]
        track_id = track["track"]["id"]
        uri = "soundcloud␟track␟{}".format(track_id)
        track_data["score"] = track["score"]
        track_data["date"] = datetime.now().strftime("%Y-%m-%d")
        track_data["uri"] = uri
        track_data["artwork_url"] = track["track"]["artwork_url"]
        track_data["comment_count"] = track["track"]["comment_count"]
        track_data["duration"] = track["track"]["full_duration"]
        track_data["description"] = track["track"]["description"]
        track_data["genre"] = track["track"]["genre"]
        track_data["label_name"] = track["track"]["label_name"]
        track_data["likes_count"] = track["track"]["likes_count"]
        track_data["playback_count"] = track["track"]["playback_count"]
        track_data["created_at"] = track["track"]["created_at"]
        track_data["reposts_count"] = track["track"]["reposts_count"]
        track_data["tag_list"] = track["track"]["tag_list"].split(" ")
        track_data["title"] = track["track"]["title"]
        track_data["user_id"] = track["track"]["user_id"]

        user_data = self._get_user_data(track["track"]["user"])

        self.log.info(track_data)
        self.log.info(user_data)
        return track_data, user_data

    def _get_user_data(self, user):
        user_data = dict()
        uri = "soundcloud␟user␟{}".format(user["id"])
        url = user["permalink_url"]
        user_data["uri"] = uri
        user_data["ingested"] = False
        user_data["date"] = datetime.now().strftime("%Y-%m-%d")
        user_data["avatar_url"] = user["avatar_url"]
        user_data["first_name"] = user["first_name"]
        user_data["last_name"] = user["last_name"]
        user_data["city"] = user["city"]
        user_data["country_code"] = user["country_code"]
        user_data["screenname"] = user["permalink"]
        user_data["url"] = url
        user_data["username"] = user["username"]
        user_data["id"] = user["id"]

        auth = "&client_id=PmqbpuYsHUQ7ZYrW6qUlPcdpVFETRzc0&app_version=1533891405&app_locale=en"
        next = None
        followers_count = 0
        while True:
            if next:
                next += auth
            response = self._make_request("{}users/{}/followers?offset=1534412026604&limit=500"
                                          "&client_id=PmqbpuYsHUQ7ZYrW6qUlPcdpVFETRzc0&app_version=1533891405"
                                          "&app_locale=en".format(self.base_url, user["id"]), next)
            followers_count += len(response["collection"])
            next = response["next_href"]
            if not next:
                break

        following_count = 0
        while True:
            if next:
                next += auth
            response = self._make_request("{}users/{}/followings?client_id=PmqbpuYsHUQ7ZYrW6qUlPcdpVFETRzc0&limit=10000"
                                          "&offset=0&linked_partitioning=1&app_version=1533891405"
                                          "&app_locale=en".format(self.base_url, user["id"]), next)
            following_count += len(response["collection"])
            next = response["next_href"]
            if not next:
                break

        track_count = 0
        while True:
            if next:
                next += auth
            response = self._make_request("{}users/{}/tracks?representation=&client_id=PmqbpuYsHUQ7ZYrW6qUlPcdpVFETRzc0"
                                          "&limit=1000&offset=0&linked_partitioning=1&app_version=1533891405"
                                          "&app_locale=en".format(self.base_url, user["id"]), next)
            track_count += len(response["collection"])
            next = response["next_href"]
            if not next:
                break

        user_data["followers"] = followers_count
        user_data["following"] = following_count
        user_data["track_count"] = track_count

        return user_data

    def fetch(self):
        self.log.info('Making request to Soundcloud for daily creators export')
        self._get_tracks()
        return self
