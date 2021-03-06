# %%

from dhs_scraper import DhsArticle, stream_to_jsonl


# %% 

# get an article from its unique triple identifier: language, dhs id and version.
# If no version given, it is assumed to be the latest one
# Here, this corresponds to the page: https://hls-dhs-dss.ch/fr/articles/029462/2016-11-23/
schneckenbundgericht = DhsArticle("fr", "029462", "2016-11-23")
schneckenbundgericht.language
schneckenbundgericht.id
schneckenbundgericht.version

# %%

# possible to create it directly from url too.
georges = DhsArticle(url="https://hls-dhs-dss.ch/fr/articles/044820/2011-12-08/")

# %%

# A DhsArticle initially contains only its language, id, version data
# to load the actual content of the article, use parse_article()
schneckenbundgericht.parse_article()
schneckenbundgericht.page_content # whole html page content obtained by a request to the article's url, can be dropped immediatly by adding drop_page=True argument to parse_article()
schneckenbundgericht.title # title of the article
schneckenbundgericht.text_blocks # text blocks of the article, with their corresponding html tag
schneckenbundgericht.text # text of the article, text blocks concatenated with "\n\n"
schneckenbundgericht.text_links # links contained in the article text, organized per text element, see parse_text_links() doc
schneckenbundgericht.bref # list of elements in the "En bref"/"Kurzinformationen"/"Scheda informativa" section of an article
schneckenbundgericht.authors_translators # authors/translators of the article
schneckenbundgericht.sources # sources from "Sources et bibliographie"/"Quellen und Literatur"/"Riferimenti bibliografici" section
schneckenbundgericht.metagrid_id # id on the metagrid network, see https://metagrid.ch/
schneckenbundgericht.metagrid_links # links from metagrid to other databases 
schneckenbundgericht.notice_links # links from section "Notices d'autorité"/"Normdateien"/"Controllo di autorità"
schneckenbundgericht.tags # internal DHS links from section "Indexation thématique"/"Systematik"/"Classificazione"
schneckenbundgericht.initial # initial of the article subject used in the article text (in article text "Zurich" is referred to with "Z." in text), can be None

# %%

# articles about people have extra fields that get parsed:
# - given name and family name (from title)
# - birth and death date (from bref section)
georges.parse_article()
if georges.is_person():
    georges.given_name
    georges.family_name
    georges.birth_date
    georges.death_date

# Things that aren't parsed but we would like to add:
# - in-text links
# - italic text
# - data tables
# - images/media and their captions
# - section titles

# %%

# Do a naïve search in the DHS, here for "bronschhofen".
# You can give a single string or a list of strings
bronschhofen_articles_search = list(DhsArticle.search_for_articles(
    "bronschofen"
))

# %%

# Loading 13 articles from a search url (here: all ecclesiastic entries)
# # do not forget the &firstIndex= ending for the url
ecclesiastic_entries = list(DhsArticle.scrape_articles_from_search_url(
    "https://hls-dhs-dss.ch/fr/search/category?text=*&sort=score&sortOrder=desc&collapsed=true&r=1&rows=20&firstIndex=0&f_hls.lexicofacet_string=1%2F006800.009500.&firstIndex=",
    max_nb_articles=13
))

# %%

# Download and parse all the article's elements for the bronschhofen articles
for a in bronschhofen_articles_search:
    a.parse_article() 

# %%


# Stream search results to a jsonl file
instruments_craftsmen_file = "instruments_craftsmen.jsonl"
instruments_craftsmen = stream_to_jsonl(instruments_craftsmen_file,DhsArticle.scrape_articles_from_search_url(
    "https://hls-dhs-dss.ch/fr/search/category?text=*&sort=score&sortOrder=desc&collapsed=true&r=1&rows=20&f_hls.lexicofacet_string=3%2F000100.132500.134600.135000.&firstIndex="
))

# Load articles back from a jsonl file
jazzpeople_file = "jazzpeople.jsonl"
jazzpeople = DhsArticle.load_articles_from_jsonl(jazzpeople_file)

# %%

# Scrape the whole french DHS and stream the articles on-the-fly to a jsonl file
# If the output jsonl file already contains some articles, makes sure no duplicates are taken
# The `jsonl_articles_content_file` file must already exist.
if False:
    language="fr"
    jsonl_articles_content_file = f"dhs_all_articles_{language}.jsonl"
    already_visited_ids_content = set(DhsArticle.get_articles_ids(jsonl_articles_content_file))
    stream_to_jsonl(
        jsonl_articles_content_file,
        DhsArticle.scrape_all_articles(
            language=language,
            parse_articles = False,
            force_language = language,
            skip_duplicates = True,
            already_visited_ids = already_visited_ids_content
        ),
        buffer_size=100
    )

