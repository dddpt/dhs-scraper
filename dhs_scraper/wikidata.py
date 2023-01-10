from __future__ import annotations
from csv import DictReader
from os import path
from typing import TYPE_CHECKING
from warnings import warn

if TYPE_CHECKING:
    from ..DhsArticle import DhsArticle


script_folder = path.dirname(__file__)

DEFAULT_WIKIDATA_LINKS_FILE = path.join(script_folder, "wikidata_dhs_wikipedia_articles_gndid_instanceof.csv")
WIKIDATA_QUERY_FILE = path.join(script_folder, "wikidata_dhs_wikipedia_articles_gndid.sparql")

LOADED_WIKIDATA_LINKS_CSVS = set()
WIKIDATA_LINKS = dict()

WIKIDATA_URL_KEY = "item"

SPARQL_DOWNLOAD_DISCLAIMER = \
    f"A prerequisite is to have manually downloaded the result of the sparql query in file '{WIKIDATA_QUERY_FILE}' at 'https://query.wikidata.org/' " + \
    f"as a csv at this location '{DEFAULT_WIKIDATA_LINKS_FILE}' (or provide to this function the location of your csv file as function argument)."

def load_wikidata_links(wikidata_links_file = DEFAULT_WIKIDATA_LINKS_FILE):
    """Loads the csv links in a dictionary of the form dhsid->list(linked wikidata_wikipedia entities)\n\n""" + SPARQL_DOWNLOAD_DISCLAIMER
    if wikidata_links_file not in LOADED_WIKIDATA_LINKS_CSVS:
        if not path.exists(wikidata_links_file):
            raise Exception(
                f"dhs_scraper.wikidata.load_wikidata_links() wikidata_links_file at location '{wikidata_links_file}' not found.\n"+
                SPARQL_DOWNLOAD_DISCLAIMER
            )
        with open(wikidata_links_file) as f:
            reader = DictReader(f)
            for r in reader:
                links_list = WIKIDATA_LINKS.get(r["dhsid"])
                if links_list is not None:
                    links_list.append(r)
                else:
                    WIKIDATA_LINKS[r["dhsid"]] = [r]
        LOADED_WIKIDATA_LINKS_CSVS.add(wikidata_links_file)
    return WIKIDATA_LINKS

def get_wikidata_links_from_dhs_id(dhs_id, wikidata_links_file = DEFAULT_WIKIDATA_LINKS_FILE):
    load_wikidata_links(wikidata_links_file)
    wiki_links = WIKIDATA_LINKS.get(dhs_id)
    if wiki_links is not None:
        return wiki_links
    else:
        return []

def get_wikidata_main_link_from_dhs_id(dhs_id, language:str, wikidata_links_file = DEFAULT_WIKIDATA_LINKS_FILE):
    load_wikidata_links(wikidata_links_file)
    wiki_links = get_wikidata_links_from_dhs_id(dhs_id)
    wikipedia_page_title_key = "name"+language
    wd_url_wk_title = (None,None)
    for l in wiki_links:
        if l[wikipedia_page_title_key] is not None and l[wikipedia_page_title_key] not in ["", "null"]:
            wd_url_wk_title = (l[WIKIDATA_URL_KEY], l[wikipedia_page_title_key])
            break
        else: 
            wd_url_wk_title = (l[WIKIDATA_URL_KEY], None)
    return wd_url_wk_title

def add_wikidata_wikipedia_to_text_links(dhs_article:DhsArticle, wikidata_links_file = DEFAULT_WIKIDATA_LINKS_FILE):
    """adds "wiki_links" attribute to dhs_article.text_links, also adds one as main "wikidata_url" and "wikipedia_page_title".
    
    Adds all the wikidata entities that have a wikidata P902 property pointing to the given dhs_id to "wiki_links".
    For the "wikidata_url" and "wikipedia_page_title" attributes, takes the first entries that has a wikipedia_page_title in given language.
    """
    load_wikidata_links(wikidata_links_file)
    if "text_links" in dhs_article.__dict__:
        for block_links in dhs_article.text_links:
            for link in block_links:
                lng, dhsid, v = dhs_article.get_language_id_version_from_url(link["href"])
                wiki_links = get_wikidata_links_from_dhs_id(dhsid)
                link["wiki_links"] = wiki_links
                wikidata_url, wikipedia_page_title = get_wikidata_main_link_from_dhs_id(dhsid, dhs_article.language)
                link["wikidata_url"] = wikidata_url
                link["wikipedia_page_title"] = wikipedia_page_title

    else:
        warn("dhs_scraper.wikidata.dhs_article_add_wikidata_wikipedia_links() trying to add links to a DhsArticle having no text_links: skipping.")
    return dhs_article


