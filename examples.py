# %%

from dhs_scraper import DhsArticle


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
# to load the actual content of the article, use get_text_and_tags()
schneckenbundgericht.get_text_and_tags()

# %%

# Do a naïve search in the DHS, here for "bronschhofen".
# You can give a single string or a list of strings
bronschhofen_articles_search = DhsArticle.search_for_articles(
    "bronschofen"
)

# %%

# Loading 13 articles from a search url (here: all ecclesiastic entries)
# # do not forget the &firstIndex= ending for the url
ecclesiastic_entries = DhsArticle.scrape_articles_from_search_url(#scrape_all_articles(
    "https://hls-dhs-dss.ch/fr/search/category?text=*&sort=score&sortOrder=desc&collapsed=true&r=1&rows=20&firstIndex=0&f_hls.lexicofacet_string=1%2F006800.009500.&firstIndex=",
    nb_articles_max=13
)

# %%

# Download the text for all the bronschhofen articles
for a in bronschhofen_articles_search:
    a.get_text_and_tags() 
# %%
