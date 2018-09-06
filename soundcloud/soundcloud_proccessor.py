import json
import requests
import sys
import re

from time import sleep
from random import randint
from datetime import datetime


CATEGORIES = ["alternativerock", "ambient", "classical", "country", "dancehall", "disco", "danceedm", "deephouse",
              "drumbass", "dubstep", "electronic", "house", "indie", "latin", "metal", "piano", "pop", "reggae",
              "reggaeton", "rock", "soundtrack", "techno", "trance", "trap", "triphop", "world", "rbsoul", "jazzblues",
              "hiphoprap", "folksingersongwriter", "all-music"]

KIND = ["trending"] #, "top"

contact_regex = {
  "phone": "(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})",
  "email": "(\S+@\S+\.\S+)",
  "website_links": r"""(?i)\b((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\-]+[.](?:com|net|org|edu|gov|us|me)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:com|net|org|edu|gov|us|me)\b/?(?!@)))"""
}


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
                        sleep(randint(2, 4))
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
        track_data["tag_list"] = track["track"]["tag_list"]
        track_data["title"] = track["track"]["title"]
        track_data["user_id"] = track["track"]["user_id"]

        user_data = self._get_user_data(track["track"]["user_id"])

        self.log.info(track_data)
        self.log.info(user_data)
        return track_data, user_data

    def _get_user_data(self, user_id):
        user_data = dict()

        user = self._make_request("{}users/{}?client_id=PmqbpuYsHUQ7ZYrW6qUlPcdpVFETRzc0".format(self.base_url, user_id))

        uri = "soundcloud␟user␟{}".format(user_id)
        url = user["permalink_url"]
        try:
            website = user["website"]
        except KeyError:
            website = None
        user_data["uri"] = uri
        user_data["date"] = datetime.now().strftime("%Y-%m-%d")
        user_data["avatar_url"] = user["avatar_url"]
        user_data["first_name"] = user["first_name"]
        user_data["last_name"] = user["last_name"]
        user_data["city"] = user["city"]
        try:
            user_data["country"] = user["country"]
        except KeyError:
            user_data["country"] = None
        try:
            user_data["country_code"] = user["country_code"]
        except KeyError:
            user_data["country_code"] = None
        user_data["screenname"] = user["permalink"]
        user_data["url"] = url
        user_data["username"] = user["username"]
        user_data["id"] = user["id"]
        user_data["followers"] = user["followers_count"]
        user_data["following"] = user["followings_count"]
        user_data["track_count"] = user["track_count"]
        user_data["reposts_count"] = user["reposts_count"]
        user_data["comments_count"] = user["comments_count"]
        user_data["website"] = website
        user_data["description"] = user["description"]

        relations, website_urls = self._get_relations(user_id)

        contact_info = self._scrape_contact_info(user["description"])

        total_urls = ""

        if website:
            total_urls += "," + website

        if website_urls:
            total_urls += ",".join(website_urls)

        if contact_info["website_links"] != "null":
            total_urls += "," + contact_info["website_links"]

        contact_info["website_links"] = total_urls if total_urls else "null"

        user_data["contact_info"] = json.dumps(contact_info)
        user_data["links"] = relations

        return user_data

    def _scrape_contact_info(self, text: str):
        contact_info = {"email": 'null', "phone": 'null', "website_links": 'null'}
        if not text:
            return contact_info
        # filter out anything above the ascii range (emojis, etc) and replace with spaces
        basic_text = ''.join([c if ord(c) < 128 else ' ' for c in text])
        for method in contact_regex:
            results = re.findall(contact_regex[method], basic_text)
            if not results:
                continue
            else:
                contact_info[method] = ','.join([res for res in results])
        return contact_info

    def _get_relations(self, user_id):
        relations = list()
        website_urls = list()
        response = self._make_request("https://api-v2.soundcloud.com/users/soundcloud:users:{}/web-profiles"
                                  "?client_id=lCenpOwq2Y1o7PEv3qhRyNSEqI7Xfx1H"
                                  "&app_version=1536151217&app_locale=en".format(user_id))

        for profile in response:
            if profile["network"] == "personal":
                website_urls.append(profile["url"])

            if profile["network"] == "facebook":
                name = self._get_url_screen_name(profile["url"])
                if not name.isdigit():
                    uri = "facebook␟page␟{}".format(name)
                    relations.append(uri)

            if profile["network"] == "instagram":
                name = self._get_url_screen_name(profile["url"])
                uri = "instagram␟user␟{}".format(name)
                relations.append(uri)

            if profile["network"] == "twitter":
                name = self._get_url_screen_name(profile["url"])
                uri = "twitter␟{}␟{}".format("user" if name.isdigit() else "screen_name", name.lower())
                relations.append(uri)

            if profile["network"] == "youtube":
                name = self._get_url_screen_name(profile["url"])
                uri = "youtube␟{}␟{}".format("channel" if "channel" in profile["url"] else "user", name)
                relations.append(uri)

        return relations, website_urls

    def _get_url_screen_name(self, url):
        url = url.replace("/", " ")
        url = url.replace("?ref=ts", "")
        url = url.replace("?ref=hl", "")
        url = url.replace("?ref=page_internal", "")
        url = url.replace("featured", "")
        url = url.replace("?view_as=subscriber", "")
        url = url.rstrip()
        return url.split(" ")[-1]

    def _check_youtube_url(self, url):
        r = requests.get(url, headers=self.headers)
        return True if r.status_code == 404 else False

    def fetch(self):
        self.log.info('Making request to Soundcloud for daily creators export')
        self._get_tracks()
        return self
