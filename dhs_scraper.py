from functools import reduce
import json
from os import truncate
import re
from sys import stderr
from traceback import print_exc
from time import sleep

from lxml import html
import pandas as pd
import requests as r

BULK_DOWNLOAD_COOL_DOWN = 0.5 # seconds
DHS_ARTICLE_TEXT_REPR_NB_CHAR = 100 # nb char of text displayed in DhsArticle representation
METAGRID_BASE_URL = "https://api.metagrid.ch/widget/dhs/person/<article_id>.json?lang=<language>&include=true"

# %%

# regex to extract dhs article id, version and language
article_language_id_version_regex = re.compile(r"/(\w+)?/?articles/(.+?)/(\d{4}-\d{2}-\d{2})?")
search_url_text_arg_regex = re.compile(r"\Wtext=(.+?)&")
search_url_alphabet_letter_arg_regex = re.compile(r"\Wf_hls.letter_string=(.+?)&")
biographical_date_bref_row_titles = ["Dates biographiques", "Lebensdaten", "Dati biografici"]

def get_attributes_string(class_name, object_dict):
    """Unimportant utility function to format __str__() and __repr()"""
    return f"""{class_name}({', '.join([
        f"{str(k)}: {str(v)}"
        for k, v in object_dict.items()
    ])})"""

def download_drop_page(func):
    """decorator to download page before func execution and,
    if asked, drop it just after"""
    def inner(self, *args, drop_page=False, **kwargs):
        self.download_page()
        result = func(self, *args, **kwargs)
        if drop_page:
            self.drop_page()
        return result
    return inner

# %%

class DhsArticle:
    def __init__(self, language=None, id=None, version=None, name=None, url=None):
        """Creates a DhsArticle, must at least have either the id or url argument set
        
        default language is german
        default version is latest
        name is optional and is the name of the article in a search results list (not the one on top of the article itself, this one is the title)
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
        self.page=None

    @property
    def url(self):
        return DhsArticle.get_url_from_id(self.id, self.language, self.version)

    def download_page(self):
        if not self.page:# and counter<5:
            self.page = r.get(self.url)
            self._pagetree = html.fromstring(self.page.content)
        return self.page
    def drop_page(self):
        self.page = None
        del self._pagetree

    def is_person(self):
        if "bref" in self.__dict__:
            for b in self.bref:
                if b["title"] in biographical_date_bref_row_titles:
                    return True
            return False
        return None

    #<h1 id="HAa" class="hls-article-title wikigeneratedheader"><span><span class="hls-lemma" locale="fr">Aa</span></span></h1>
    @download_drop_page
    def parse_title(self):
        """Parses title of the article as seen on article page. Adds self.title, and for people, self.given_name and self.family_name
        
        Do not confuse self.title with self.name (name found in search result lists, should be removed?)"""
        title_element = self._pagetree.cssselect(".hls-article-title")[0]
        self.title = " ".join([c.text_content().strip() for c in title_element.getchildren()[0].getchildren()])
        given_name = title_element.cssselect("span[itemprop=givenName]")
        if len(given_name)>0:
            self.given_name = given_name[0].text_content().strip()
        family_name = title_element.cssselect("span[itemprop=familyName]")
        if len(family_name)>0:
            self.family_name = family_name[0].text_content().strip()
        return self.title
    @download_drop_page
    def parse_authors_translators(self):
        """dirty parsing of author/translator in self.authors_translators"""
        self.authors_translators = [au.text_content().strip() for au in self._pagetree.cssselect(".hls-article-text-author")]
        return self.authors_translators
    @download_drop_page
    def parse_text(self):
        """parses text of the article and adds it in self.text
        Usually doesn't get data table, only their title"""
        self.text = reduce(lambda s,el: s+el.text_content()+"\n\n", self._pagetree.cssselect(".hls-article-text-unit p"), "")[0:-2]
        return self.text
    @download_drop_page
    def parse_sources(self):
        """Parses sources in self.sources

        self.sources is organized as a dict with section title as keys to a sources list 
        content of a source:
        - text (whole text)
        - author (if present, <span class="au"> tags)
        - tpub (if present <span class="tpub"> tags)
        - journal (if present, elements in <em> tags)
        - link (if present, <a> tags to another website)
        -> each of these elements is in a list (if multiple values)
        """
        def parse_source(source_element):
            text = source_element.text_content().strip()
            authors = [au.text_content().strip() for au in source_element.cssselect(".au")]
            tpub = [t.text_content().strip() for t in source_element.cssselect(".tpub")]
            journal = [j.text_content().strip() for j in source_element.cssselect("em")]
            link =  [a.get("href") for a in source_element.cssselect("a")]
            source = {"text": text}
            if len(authors)>0:
                source["author"] = authors
                if len(authors)>1:
                    print(f"DhsArticle.parse_sources(): more than one author for a source dhs-id:{self.id}, source: {text}")
            if len(tpub)>0:
                source["tpub"] = tpub
                #if len(title)>1:
                #    print(f"DhsArticle.parse_sources(): more than one tpub for a source dhs-id:{self.id}, source: {text}")
            if len(journal)>0:
                source["journal"] = journal
                #if len(journal)>1:
                #    print(f"DhsArticle.parse_sources(): more than one journal for a source dhs-id:{self.id}, source: {text}")
            if len(link)>0:
                source["link"] = link
                if len(link)>1:
                    print(f"DhsArticle.parse_sources(): more than one link for a source dhs-id:{self.id}, source: {text}")
            return source
        def get_section_title(section_element):
            section_title = section_element.cssselect(".panel-title")
            if len(section_title)>0:
                return section_title[0].text_content().strip()
            else:
                return "default"
        self.sources = {
            get_section_title(section_element): [
                parse_source(source_element)
                for source_element in section_element.cssselect("li")]
            for section_element in self._pagetree.cssselect("#_hls_references .panel")
        }
        return self.sources
    @download_drop_page
    def parse_notice_links(self):
        """parses "notices d'autorités" links, mostly GND links, in self.notice_links"""
        # notices d'autorités:
        self.notice_links = [
            {
                "title":el.text_content(),
                "url":el.get("href")
            } for el in self._pagetree.cssselect(".hls-service-box-left a")
        ]
        return self.notice_links
    @download_drop_page
    def parse_metagrid(self):
        """Adds self.metagrid_id and metagrid_links properties
        
        self.metagrid_id is a str
        self.metagrid_links is of the form returned by the metagrid api, see example: https://api.metagrid.ch/widget/dhs/person/3848.json?lang=de&include=true
        """
        # metagrid links
        #<div id="hls-service-box-metagrid" articleId="17791" style="display: none;" class="hls-service-box-subtitle">
        metagrid_div = self._pagetree.cssselect("#hls-service-box-metagrid")
        if len(metagrid_div)>0:
            attr = {k:v for k,v in metagrid_div[0].items()}
            self.metagrid_id = metagrid_div[0].get("articleid")
            language = self.language if self.language else "de"
            metagrid_url = METAGRID_BASE_URL.replace("<article_id>", self.metagrid_id).replace("<language>", language)
            metagrid_resp = r.get(metagrid_url)
            if metagrid_resp.status_code==200:
                self.metagrid_links = metagrid_resp.json()
            else:
                print(f"DhsArticle.parse_metagrid() failure to get metagrid links for dhs article {self.id} with metagrid url: {metagrid_url}")
                self.metagrid_links = []
        else:
            self.metagrid_id = None
            self.metagrid_links = []
        return self.metagrid_links
    @download_drop_page
    def parse_bref(self):
        """Parses the "En bref" section of a DHS article in self.bref, ONLY WORKS ON FR ARTICLES

        self.bref is a list
        Content of a bref row:
        - title
        - text
        - link ( if present)
        - birth and death (if title=="Dates biographiques")
        """
        def parse_bref_row(bref_row):
            bref_row_dict = {}
            bref_row_dict["title"] = bref_row.cssselect(".hls-service-box-table-title")[0].text_content().strip()
            bref_row_dict["text"] = bref_row.cssselect(".hls-service-box-table-text")[0].text_content().strip()
            link = bref_row.cssselect(".hls-service-box-table-text a")
            if len(link)>0:
                bref_row_dict["link"] = [l.get("href") for l in link]
            if bref_row_dict["title"] in biographical_date_bref_row_titles:
                birth_span = bref_row.cssselect(".hls-service-box-table-text span[itemProp=birthDate]")
                if len(birth_span)>0:
                    bref_row_dict["birth"] = birth_span[0].text_content().strip()
                death_span = bref_row.cssselect(".hls-service-box-table-text span[itemProp=deathDate]")
                if len(death_span)>0:
                    bref_row_dict["death"] = death_span[0].text_content().strip()
            return bref_row_dict
        bref_box = self._pagetree.cssselect(".hls-service-box-right .hls-service-box-element:first-child")
        if len(bref_box)>0:
            bref_title = bref_box[0].cssselect(".hls-service-box-title")
            if len(bref_title)>0 and bref_title[0].text_content().strip() in ["En bref","Kurzinformationen","Scheda informativa"]:
                self.bref = [parse_bref_row(b) for b in bref_box[0].cssselect("tr")]
        if ("bref" not in self.__dict__) or not self.bref:
            self.bref=[]
        return self.bref
    @download_drop_page
    def parse_tags(self):
        """Parses tags in a list of dict with "tag" and "url" keys"""
        tags_box = self._pagetree.cssselect(".hls-service-box-right .hls-service-box-element:last-child")
        if len(tags_box)>0:
            tags_title = tags_box[0].cssselect(".hls-service-box-title")
            if len(tags_title)>0 and tags_title[0].text_content().strip() in ["Indexation thématique","Systematik","Classificazione"]:
                self.tags = [
                    DhsTag(el.text_content(), el.get("href"))
                    for el in tags_box[0].cssselect("a")
                ]
            else:
                self.tags=[]
        else:
            self.tags=[]
        return self.tags
    @download_drop_page
    def parse_article(self):
        """Calls all the parse_XX functions"""
        self.parse_title()
        self.parse_authors_translators()
        self.parse_text()
        self.parse_sources()
        self.parse_metagrid()
        self.parse_notice_links()
        self.parse_bref()
        self.parse_tags()

    def __str__(self):
        odict = self.__dict__.copy()
        if "text" in odict and odict["text"] and len(odict["text"])>DHS_ARTICLE_TEXT_REPR_NB_CHAR:
            odict["text"] = odict["text"][0:DHS_ARTICLE_TEXT_REPR_NB_CHAR]+" [...]"
        odict["page"] = "loaded" if self.page else "not loaded"
        if "tags" in odict and odict["tags"] is not None:
            odict["tags"] = [t.tag for t in odict["tags"]]
        return get_attributes_string("DhsArticle", odict)
    def __repr__(self):
        return self.__str__()
    def __eq__(self, other):
        if type(other) is type(self):
            return (self.language, self.id, self.version)==(other.language, other.id, other.version)
        return False
    def __hash__(self):
        return hash((self.language, self.id, self.version))

    def to_language(self, new_language):
        """Returns a new DhsArticle with new language"""
        if new_language not in ["fr", "de", "it"]:
            raise Exception(f"DhsArticle.to_language(): invalid target language: {new_language}, must be one of de, fr, it")
        return DhsArticle(new_language, self.id, self.version)

    def to_json(self, *args, **kwargs):
        """Returns a json string serialization of this DhsArticle"""
        json_dict = self.__dict__.copy()
        del json_dict["page"]
        del json_dict["_pagetree"]
        json_dict["url"] = self.url
        if "tags" in json_dict: 
            json_dict["tags"] = [t.to_json(as_dict=True) for t in self.tags]
        jsonstr =  json.dumps(json_dict, *args, **kwargs)
        return jsonstr
    @staticmethod
    def from_json(json_dict):
        """Parses a DhsArticle from a dict obtained from json.loads()"""
        article = DhsArticle(json_dict["language"], json_dict["id"], json_dict["version"], json_dict["name"])
        if "tags" in json_dict:
            article.tags = [DhsTag.from_json(jt) for jt in json_dict["tags"]]
        done_props = {"language", "id", "version", "name", "url", "tags"}
        for k,v in json_dict.items():
            if k not in done_props:
                article.__dict__[k] = v
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
        article_id_version_match = article_language_id_version_regex.search(url)
        if article_id_version_match:
            return article_id_version_match.groups()
        else:
            return (None, None, None)


    @staticmethod
    def scrape_articles_from_search_url(search_url, rows_per_page=20, max_nb_articles=None,
                    parse_articles=False, force_language = None, skip_duplicates=True, already_visited_ids=None):
        """returns a list of DHS articles' names & URLs from a DHS search url

        rows_per_page is the value of the "rows" argument in the search_url, by default 20, better to set it to a 100
        parse_articles decides whether to load the page's content or not
        force_language must either be falsy or one of "fr", "de", "it"

        search_url is an url corresponding to a search in the DHS search interface
        search_url should end with "&firstIndex=" to browse through the search results
        search_url example:
        "https://hls-dhs-dss.ch/fr/search/category?text=*&sort=score&sortOrder=desc&collapsed=true&r=1&rows=100&f_hls.lexicofacet_string=2%2F006800.009500.009600.&f_hls.lexicofacet_string=2%2F006800.009500.009700.&f_hls.lexicofacet_string=2%2F006800.009500.009800.&f_hls.lexicofacet_string=2%2F006800.009500.009900.&f_hls.lexicofacet_string=2%2F006800.009500.010000.&f_hls.lexicofacet_string=2%2F006800.009500.010100.&f_hls.lexicofacet_string=2%2F006800.009500.010200.&f_hls.lexicofacet_string=2%2F006800.009500.010300.&firstIndex="
        
        returns a generator of DhsArticle
        """
        if not already_visited_ids:
            already_visited_ids=set()

        # extacting info on url for logging
        search_text_match = search_url_text_arg_regex.search(search_url)
        search_text = ("search query: "+search_text_match.group(1)+" ") if search_text_match else ""
        az_letter_match = search_url_alphabet_letter_arg_regex.search(search_url)
        az_letter = ("alphabet letter: "+az_letter_match.group(1)+" ") if az_letter_match else ""

        # getting first page for nb of pages
        articles_page_url = search_url+"0"
        articles_page = r.get(articles_page_url)
        tree = html.fromstring(articles_page.content)
        pagination_last = tree.cssselect(".pagination a:last-child")
        nb_search_pages = int(pagination_last[0].text_content()) if len(pagination_last)>0 else 1
        
        # iterating over pages
        for search_page_number in range(0,nb_search_pages):
            print(f"Loading search page nb {search_page_number} for "+search_text+az_letter)
            if search_page_number!=0:
                # get the new page
                articles_page_url = search_url+str(search_page_number*rows_per_page)
                sleep(BULK_DOWNLOAD_COOL_DOWN)
                articles_page = r.get(articles_page_url)
                tree = html.fromstring(articles_page.content)
            if max_nb_articles is not None and search_page_number*rows_per_page>=max_nb_articles:
                break
            search_results = tree.cssselect(".search-result a")
            for i,c in enumerate(search_results):
                article_index = search_page_number*rows_per_page+i
                if max_nb_articles is not None and article_index>=max_nb_articles:
                    break
                ctitle = c.cssselect(".search-result__title")
                cname = ctitle[0].text_content().strip()
                # search-result__title
                page_url = c.get("href")
                article = DhsArticle(url="https://hls-dhs-dss.ch"+page_url, name= cname)
                if (not skip_duplicates) or article.id not in already_visited_ids:
                    if force_language:
                        article.language = force_language
                    if parse_articles:
                        sleep(BULK_DOWNLOAD_COOL_DOWN)
                        try:
                            article.parse_article()
                        except Exception as e:
                            print(f"ERROR PARSING ARTICLE WITH DHS-ID: {article.id}", file=stderr)
                            print_exc(file=stderr)
                    already_visited_ids.add(article.id)
                    yield article
                else:
                    print(f"DhsArticle.scrape_articles_from_search_url() skipping duplicate {article.id}, name: {article.name}")

    @staticmethod
    def search_for_articles(keywords, language="fr", **kwargs):
        if not isinstance(keywords, str):
            keywords = " ".join(keywords)
        search_url = f"https://hls-dhs-dss.ch/{language}/search/?sort=score&sortOrder=desc&rows=100&highlight=true&facet=true&r=1&text={keywords}&firstIndex="
        return DhsArticle.scrape_articles_from_search_url(search_url, rows_per_page=100, **kwargs)

    @staticmethod
    def scrape_all_articles(language="fr", max_nb_articles_per_letter=None, skip_duplicates=True, already_visited_ids=None, **kwargs):
        """Scrapes all articles from DHS"""
        if not already_visited_ids:
            already_visited_ids=set()
        alphabet_url_basis = f"https://hls-dhs-dss.ch/{language}/search/alphabetic?text=*&sort=hls.title_sortString&sortOrder=asc&collapsed=true&r=1&rows=100&f_hls.letter_string="
        firstindex_arg_basis = "&firstIndex="
        for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ": # ABCDEFGHIJKLMNOPQRSTUVWXYZ
            print("Downloading articles starting with letter: "+letter)
            url = alphabet_url_basis+letter+firstindex_arg_basis
            for a in DhsArticle.scrape_articles_from_search_url(
                        url, 
                        rows_per_page=100,
                        max_nb_articles= max_nb_articles_per_letter,
                        already_visited_ids = already_visited_ids,
                        **kwargs):
                    yield a

    @staticmethod
    def scrape_to_jsonl(jsonl_filepath, articles_generator, buffer_size=100):
        """Scrapes articles to a jsonl file from an articles generator
        
        Uses a buffer to avoid disk usage"""
        buffer = [None]*buffer_size
        with open(jsonl_filepath, "a") as jsonl_file:
            for i, a in enumerate(articles_generator):
                if i!=0 and i%buffer_size==0:
                    print(f"BUFFER OUT i: {i} \nbuffer[0]:{buffer[0]}\nbuffer[-1]:{buffer[-1]}")
                    jsonl_file.write("\n".join(buffer)+"\n")
                print(f"article {a.name}, i: {i}")
                buffer[i%buffer_size]= a.to_json(ensure_ascii=False)
            print(f"FINAL BUFFER OUT final i: {i}, i%buffer_size: {i%buffer_size} \nbuffer[0]:{buffer[0]}\nbuffer[{i%buffer_size}]:{buffer[i%buffer_size]}")
            jsonl_file.write("\n".join(buffer[0:((i%buffer_size)+1)])+"\n")

    @staticmethod
    def get_already_visited_ids(jsonl_filepath):
        """Returns a set containing the ids of all the articles present in the given jsonl
        
        Useful to relaunch a scrape after an interruption."""
        article_id_regex = re.compile(r'^.+?"id": "(\d+)"')
        with open(jsonl_filepath, "r") as jsonl_file:
            return set(article_id_regex.search(line).group(1) for line in jsonl_file if len(line)>0)

    @staticmethod
    def load_articles_from_jsonl(jsonl_filepath):
        """Loads articles from a .jsonl file with one json DhsArticle per line"""
        def load_article(line, i = 0):
            try:
                return DhsArticle.from_json(json.loads(line.strip()))
            except Exception as e:
                print(f"Exception loading DhsArticle from {i}th line:\n{line}")
                import time
                time.sleep(1)
                raise e
        with open(jsonl_filepath, "r") as jsonl_file:
            articles = list(load_article(line,i) for i,line in enumerate(jsonl_file) if len(line)>0)
        return articles

class DhsTag:
    """Defines a DHS tag with properties "tag" and "url"
    
    Implements equality and hash based on "tag" property, not url.
    """
    def __init__(self, tag, url):
        self.tag = tag
        self. url =  url
    def get_levels(self):
        return [l.strip() for l in self.tag.split("/")]
    def get_level(self, level, default_to_last=False):
        levels = self.get_levels()
        if level<len(levels):
            return levels[level]
        elif default_to_last:
            return levels[-1]
        return None
    @property
    def ftag(self):
        """returns last tag level"""
        return self.get_levels()[-1]
    def __hash__(self) -> int:
        return hash(self.tag)
    def __eq__(self, other):
        if type(other) is type(self):
            return other.tag==self.tag
        #elif isinstance(other, str): # dangerous
        #    return other==self.tag
        return False
    def __str__(self):
        return f'DhsTag("{self.tag}")'
    def __repr__(self):
        return self.__str__()
    def to_json(self, as_dict=False):
        if as_dict:
            return self.__dict__.copy()
        else:
            return json.dumps(self.__dict__)
    @staticmethod
    def from_json(json_dict):
        return DhsTag(json_dict["tag"], json_dict["url"])