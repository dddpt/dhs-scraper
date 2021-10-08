import requests as r
import re
from lxml import html
from functools import reduce
import math
import copy

import json
import pandas as pd

from utils import get_attributes_string


# %%

article_id_version_regex = re.compile(r"articles/(.+?)/(.*?)")

class DhsArticle:
    def __init__(self, url, name=None):
        self.url=url
        self.name=name
        article_id_version_match = article_id_version_regex.search(self.url)
        if article_id_version_match:
            self.id = "dhs-"+article_id_version_match.group(1)
            version = article_id_version_match.group(2)
            if version:
                self.version = version
            else:
                print("No version for DHSArticle: "+name)
        else:
            print("No id/version for DHSArticle: "+name)
        self.page = None
        self.text = None
        self.tags = None

    def download_page(self):
        if not self.page:# and counter<5:
            self.page = r.get(self.url)
        return self.page

    def get_text_and_tags(self):
        self.download_page()
        pagetree = html.fromstring(self.page.content)
        self.text = reduce(lambda s,el: s+"\n\n"+el.text_content(), pagetree.cssselect(".hls-article-text-unit p"), "")
        self.tags = [{"tag":el.text_content(),"url":el.xpath("@href")[0]} for el in pagetree.cssselect(".hls-service-box-right a")]
    def __str__(self):
        odict = self.__dict__.copy()
        if "text" in odict and odict["text"] and len(odict["text"])>20:
            odict["text"] = odict["page"][0:20]+" [...]"
        odict["page"] = "loaded" if self.page else "not loaded"
        return get_attributes_string("DhsArticle", odict)
    def __repr__(self):
        return self.__str__()
    @staticmethod
    def get_articles_from_dhs_search(search_url, rows_per_page=100):
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
        articles_list = [None]*articles_count
        for search_page_number in range(0,math.ceil(articles_count/rows_per_page)+1):
            print("Getting search results, firstIndex=",rows_per_page*search_page_number, "\nurl = ",articles_page_url)
            search_results = tree.cssselect(".search-result a")
            for i,c in enumerate(search_results):
                cname = c.text_content().strip()
                page_url = c.xpath("@href")[0]
                print("url for ",cname,": ",page_url)
                article = DhsArticle("https://hls-dhs-dss.ch"+page_url, cname)
                articles_list[search_page_number*rows_per_page+i] = article
            articles_page_url = search_url+str(search_page_number*rows_per_page)
            articles_page = r.get(articles_page_url)
            tree = html.fromstring(articles_page.content)
        return articles_list

