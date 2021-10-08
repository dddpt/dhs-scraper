import requests as r
import re
from lxml import html
from functools import reduce
import math
import copy

import json
import pandas as pd

from utils import get_attributes_string
from time import sleep

BULK_DOWNLOAD_COOL_DOWN = 0.1 # seconds
DHS_ARTICLE_TEXT_REPR_NB_CHAR = 100 # nb char of text displayed in DhsArticle representation

# %%

article_id_version_regex = re.compile(r"/(\w+)?/?articles/(.+?)/(\d{4}-\d{2}-\d{2})?")


class DhsArticle:
    def __init__(self, language, id, version, name=None, url=None):
        self.name=name
        self.id = id
        if version:
            self.version = version
        else:
            print("No version for DHSArticle: "+name)
        self.url=url if url else DhsArticle.get_url_from_id(language, id, version)
        if language:
            self.language = language
        else:
            print("No language for DHSArticle: "+name)
        self.page = None
        self.text = None
        self.tags = None


    def download_page(self):
        if not self.page:# and counter<5:
            self.page = r.get(self.url)
        return self.page


    def get_text_and_tags(self, drop_page=False):
        self.download_page()
        pagetree = html.fromstring(self.page.content)
        self.text = reduce(lambda s,el: s+el.text_content()+"\n\n", pagetree.cssselect(".hls-article-text-unit p"), "")[0:-2]
        self.tags = [{"tag":el.text_content(),"url":el.xpath("@href")[0]} for el in pagetree.cssselect(".hls-service-box-right a")]
        if drop_page:
            self.page = None
    def __str__(self):
        odict = self.__dict__.copy()
        if "text" in odict and odict["text"] and len(odict["text"])>DHS_ARTICLE_TEXT_REPR_NB_CHAR:
            odict["text"] = odict["text"][0:DHS_ARTICLE_TEXT_REPR_NB_CHAR]+" [...]"
        odict["page"] = "loaded" if self.page else "not loaded"
        if "tags" in odict and odict["tags"] is not None:
            odict["tags"] = [t["tag"] for t in odict["tags"]]
        return get_attributes_string("DhsArticle", odict)
    def __repr__(self):
        return self.__str__()

    def to_language(self, new_language):
        """Returns a new DhsArticle with new language"""
        return DhsArticle(new_language, self.id, self.version)

    def to_json(self, *args, **kwargs):
        opage = self.page
        self.page=None
        jsonstr =  json.dumps(self.__dict__, *args, **kwargs)
        self.page = opage
        return jsonstr
    @staticmethod
    def from_json(json_dict):
        """Parses a DhsArticle from a dict obtained from json.loads()"""
        article = DhsArticle(json_dict["language"], json_dict["id"], json_dict["version"], json_dict["name"], json_dict["url"])
        article.text = json_dict["text"]
        article.tags = json_dict["tags"]
        return article

    @staticmethod
    def get_url_from_id(id, language = None, version=None):
        if id.startswith("dhs-"):
            id = id[4:]
        url = "/articles/"+id
        if language is not None:
            url = "/"+language+url
        if version is not None:
            url += "/"+version
        return "https://hls-dhs-dss.ch"+url


    @staticmethod
    def get_language_id_version_from_url(url):
        """returns (language, id, version) tuple from given dhs article url"""
        article_id_version_match = article_id_version_regex.search(url)
        if article_id_version_match:
            return article_id_version_match.groups()
        else:
            return (None, None, None)


    @staticmethod
    def from_url(url, name=None):
        article_id_version_match = article_id_version_regex.search(url)
        if article_id_version_match:
            language = article_id_version_match.group(1)
            id = article_id_version_match.group(2)
            version = article_id_version_match.group(3)
            return DhsArticle(language, id, version, name, url)
        else:
            raise Exception("DhsArticle.from_url(): missing id from DhsArticle url: "+url)
            


    @staticmethod
    def scrape_articles_from_dhs_search(search_url, rows_per_page=100, nb_articles_max=None):
        """returns a list of DHS articles' names & URLs from a DHS search url

        search_url is an url corresponding to a search in the DHS search interface
        search_url should end with "&firstIndex=" to browse through the search results
        search_url example:
        "https://hls-dhs-dss.ch/fr/search/category?text=*&sort=score&sortOrder=desc&collapsed=true&r=1&rows=100&f_hls.lexicofacet_string=2%2F006800.009500.009600.&f_hls.lexicofacet_string=2%2F006800.009500.009700.&f_hls.lexicofacet_string=2%2F006800.009500.009800.&f_hls.lexicofacet_string=2%2F006800.009500.009900.&f_hls.lexicofacet_string=2%2F006800.009500.010000.&f_hls.lexicofacet_string=2%2F006800.009500.010100.&f_hls.lexicofacet_string=2%2F006800.009500.010200.&f_hls.lexicofacet_string=2%2F006800.009500.010300.&firstIndex="
        
        returns a list of DhsArticle
        """
        articles_page_url = search_url+"0"
        articles_page = r.get(articles_page_url)
        tree = html.fromstring(articles_page.content)
        articles_count = int(tree.cssselect(".hls-search-header__count")[0].text_content())
        if nb_articles_max is not None:
            articles_count = nb_articles_max
        articles_list = [None]*articles_count
        for search_page_number in range(0,math.ceil(articles_count/rows_per_page)+1):
            print("Getting search results, firstIndex=",rows_per_page*search_page_number, "\nurl = ",articles_page_url)
            search_results = tree.cssselect(".search-result a")
            for i,c in enumerate(search_results):
                article_index = search_page_number*rows_per_page+i
                if article_index>=articles_count:
                    break
                ctitle = c.cssselect(".search-result__title")
                print(f"len(ctitle): {len(ctitle)}, ctitle: {ctitle }")
                cname = ctitle[0].text_content().strip()
                # search-result__title
                page_url = c.xpath("@href")[0]
                article = DhsArticle.from_url("https://hls-dhs-dss.ch"+page_url, cname)
                articles_list[article_index] = article
                sleep(BULK_DOWNLOAD_COOL_DOWN)
            articles_page_url = search_url+str(search_page_number*rows_per_page)
            articles_page = r.get(articles_page_url)
            tree = html.fromstring(articles_page.content)
        return articles_list


    @staticmethod
    def scrape_all_articles(language="fr", rows_per_page=20, nb_articles_max=None):
        """Scrapes all articles from DHS"""
        alphabet_url_basis = f"https://hls-dhs-dss.ch/{language}/search/alphabetic?text=*&sort=hls.title_sortString&sortOrder=asc&collapsed=true&r=1&rows={rows_per_page}&f_hls.letter_string="
        firstindex_arg_basis = "&firstIndex="
        articles_by_letter = {}
        for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
            print("Downloading articles starting with letter: "+letter)
            url = alphabet_url_basis+letter+firstindex_arg_basis
            articles_by_letter[letter] = DhsArticle.scrape_articles_from_dhs_search(url, rows_per_page, nb_articles_max)
        return articles_by_letter

