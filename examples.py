# %%

from dhs_scraper import DhsArticle, stream_to_jsonl


# %% 

# get an article from its unique triple identifier: fr, dhs id and version.
# If no version given, it is assumed to be the latest one
# Here, this corresponds to the page: https://hls-dhs-dss.ch/fr/articles/029462/2016-11-23/
schneckenbundgericht = DhsArticle("fr", "029462", "2016-11-23")

# %%

# possible to create it directly from url too.
schmerikon = DhsArticle(url="https://hls-dhs-dss.ch/fr/articles/001373/2011-08-10/")

# %%

# A DhsArticle initially contains only its language, id, version data
# to load the actual content of the article, use parse_article()
schneckenbundgericht.parse_article()

# %%

# Do a naïve search in the DHS, here for "bronschhofen".
# You can give a single string or a list of strings
bronschhofen_articles_search = DhsArticle.search_for_articles(
    "bronschofen"
)

# %%

# Loading 13 articles from a search url (here: all ecclesiastic entries)
# # do not forget the &firstIndex= ending for the url
ecclesiastic_entries = DhsArticle.scrape_articles_from_search_url(
    "https://hls-dhs-dss.ch/fr/search/category?text=*&sort=score&sortOrder=desc&collapsed=true&r=1&rows=20&firstIndex=0&f_hls.lexicofacet_string=1%2F006800.009500.&firstIndex=",
    max_nb_articles=13
)

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
    already_visited_ids_content = DhsArticle.get_already_visited_ids(jsonl_articles_content_file)
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

