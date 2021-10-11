from functools import reduce
import json
import re
import math
from time import sleep

from lxml import html
import pandas as pd
import requests as r

BULK_DOWNLOAD_COOL_DOWN = 0.5 # seconds
DHS_ARTICLE_TEXT_REPR_NB_CHAR = 100 # nb char of text displayed in DhsArticle representation

# %%

article_id_version_regex = re.compile(r"/(\w+)?/?articles/(.+?)/(\d{4}-\d{2}-\d{2})?")

def get_attributes_string(class_name, object_dict):
    return f"""{class_name}({', '.join([
        f"{str(k)}: {str(v)}"
        for k, v in object_dict.items()
    ])})"""

def download_drop_page(func):
    """decorator to download page before func execution and,
    if asked, drop it just after"""
    def inner(self,drop_page=False, *args, **kwargs):
        self.download_page()
        result = func(self, *args, **kwargs)
        if drop_page:
            self.drop_page()
        return result
    return inner

# %%

class DhsArticle:
    def __init__(self, language=None, id=None, version=None, name=None, url=None):
        """Creates a DhsArticle, must at least have the id or url argument not None
        
        default language is german
        default version is latest
        """
        if (not id) and (not url):
            raise Exception("DhsArticle.__init__(): at least one of id or url must be specified")
        self.name=name
        if (not id) and url:
            language, id, version = DhsArticle.get_language_id_version_from_url(url)
            if not id:
                raise Exception("DhsArticle.__init__(): not a DhsArticle url: "+url)
        self.language = language
        self.id = id
        self.version = version
        self.url = url if url else DhsArticle.get_url_from_id(id, language, version)
        self.page = None
        self.text = None
        self.tags = None


    def download_page(self):
        if not self.page:# and counter<5:
            self.page = r.get(self.url)
            self._pagetree = html.fromstring(self.page.content)
        return self.page
    def drop_page(self):
        self.page = None
        del self._pagetree

    @download_drop_page
    def get_text(self):
        self.text = reduce(lambda s,el: s+el.text_content()+"\n\n", self._pagetree.cssselect(".hls-article-text-unit p"), "")[0:-2]
        return self.text
    @download_drop_page
    def get_tags(self):
        self.tags = [
            {
                "tag":el.text_content(),
                "url":el.xpath("@href")[0]
            } for el in self._pagetree.cssselect(".hls-service-box-right a")
        ]
        return self.tags
    @download_drop_page
    def get_sources(self):
        """
        Sources are organized by section
        content of a source:
        - text (whole text)
        - author (if present)
        - publication (if present)
        - link (if present, url to another website)
        """
        def parse_source(source_element):
            text = source_element.text_content().strip()
            authors = [au.text_content().strip() for au in source_element.cssselect(".au")]
            title = [t.text_content().strip() for t in source_element.cssselect(".tpub")]
            link = source_element.cssselect("a").xpath("@href")
            source = {"text": text}
            if len(authors)>0:
                source["author"] = authors
                if len(authors)>1:
                    print(f"DhsArticle.get_sources(): more than one author for a source dhs-id:{self.id}, source: {text}")
            if len(title)>0:
                source["title"] = title
                if len(title)>1:
                    print(f"DhsArticle.get_sources(): more than one tpub for a source dhs-id:{self.id}, source: {text}")
            if len(link)>0:
                source["link"] = link
                if len(link)>1:
                    print(f"DhsArticle.get_sources(): more than one link for a source dhs-id:{self.id}, source: {text}")
            return source

        self.sources = {
            section_element.cssselect(".panel-title")[0].text_content().strip(): [
                parse_source(source_element)
                for source_element in section_element.cssselect("li")]
            for section_element in self._pagetree.cssselect("#_hls_references .panel")
        }
        return self.sources
    @download_drop_page
    def get_links(self):
        # TODO
        return self.links
    @download_drop_page
    def get_bref(self):
        # TODO
        return self.links
    @download_drop_page
    def get_author_translator(self):
        # TODO
        return self.links


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
        """Returns a json string serialization of this DhsArticle"""
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
    def scrape_articles_from_search_url(search_url, rows_per_page=20, max_nb_articles=None):
        """returns a list of DHS articles' names & URLs from a DHS search url

        search_url is an url corresponding to a search in the DHS search interface
        search_url should end with "&firstIndex=" to browse through the search results
        search_url example:
        "https://hls-dhs-dss.ch/fr/search/category?text=*&sort=score&sortOrder=desc&collapsed=true&r=1&rows=100&f_hls.lexicofacet_string=2%2F006800.009500.009600.&f_hls.lexicofacet_string=2%2F006800.009500.009700.&f_hls.lexicofacet_string=2%2F006800.009500.009800.&f_hls.lexicofacet_string=2%2F006800.009500.009900.&f_hls.lexicofacet_string=2%2F006800.009500.010000.&f_hls.lexicofacet_string=2%2F006800.009500.010100.&f_hls.lexicofacet_string=2%2F006800.009500.010200.&f_hls.lexicofacet_string=2%2F006800.009500.010300.&firstIndex="
        
        returns a generator of DhsArticle
        """
        articles_page_url = search_url+"0"
        articles_page = r.get(articles_page_url)
        tree = html.fromstring(articles_page.content)
        nb_search_pages = int(tree.cssselect(".pagination a:last-child")[0].text_content())
        if max_nb_articles is not None:
            articles_count = max_nb_articles
        for search_page_number in range(0,nb_search_pages+1):
            search_results = tree.cssselect(".search-result a")
            for i,c in enumerate(search_results):
                article_index = search_page_number*rows_per_page+i
                if article_index>=articles_count:
                    break
                ctitle = c.cssselect(".search-result__title")
                cname = ctitle[0].text_content().strip()
                # search-result__title
                page_url = c.xpath("@href")[0]
                article = DhsArticle(url="https://hls-dhs-dss.ch"+page_url, name= cname)
                yield article
            articles_page_url = search_url+str(search_page_number*rows_per_page)
            sleep(BULK_DOWNLOAD_COOL_DOWN)
            articles_page = r.get(articles_page_url)
            tree = html.fromstring(articles_page.content)

    @staticmethod
    def search_for_articles(keywords, rows_per_page=20, max_nb_articles=None):
        if not isinstance(keywords, str):
            keywords = " ".join(keywords)
        search_url = f"https://hls-dhs-dss.ch/fr/search/?sort=score&sortOrder=desc&rows={rows_per_page}&highlight=true&facet=true&r=1&text={keywords}&firstIndex="
        return DhsArticle.scrape_articles_from_search_url(search_url, rows_per_page, max_nb_articles)

    @staticmethod
    def scrape_all_articles(language="fr", rows_per_page=100, max_nb_articles_per_letter=None):
        """Scrapes all articles from DHS"""
        alphabet_url_basis = f"https://hls-dhs-dss.ch/{language}/search/alphabetic?text=*&sort=hls.title_sortString&sortOrder=asc&collapsed=true&r=1&rows={rows_per_page}&f_hls.letter_string="
        firstindex_arg_basis = "&firstIndex="
        for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
            print("Downloading articles starting with letter: "+letter)
            url = alphabet_url_basis+letter+firstindex_arg_basis
            for a in DhsArticle.scrape_articles_from_search_url(url, rows_per_page, max_nb_articles_per_letter):
                yield a


"""
Todo scraping
- liens
    - metagrid
    - notices d'autorité (GND, viaf)
- sources & bibliographie
- en bref
    - dates biographiques
    - Endonyme(s)/Exonyme(s)
    - variantes
    - contexte
- author & translator
- citation suggestion


- RaphyDallèves https://hls-dhs-dss.ch/fr/articles/042263/2003-02-03/
    -> simple persone, SIKART source
- Aa, rivière: https://hls-dhs-dss.ch/fr/articles/008746/2000-09-20/
    -> variantes section
- vaud https://hls-dhs-dss.ch/fr/articles/007395/2017-05-30/
    -> lots of sources, source de texte brut
- ville de zurich https://hls-dhs-dss.ch/fr/articles/000171/2015-01-25/
    -> link as a source
    -> endonymes/exonymes

"""