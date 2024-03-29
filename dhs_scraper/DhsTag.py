import json



class DhsTag:
    """Defines a DHS tag with properties "tag" and "url"
    
    Implements equality and hash based on "tag" property, not url.
    """
    def __init__(self, tag, url=None):
        self.tag = tag
        self.url =  url
        self.facet = url.split("=")[1] if url is not None else None
    def get_levels(self):
        return [l.strip() for l in self.tag.split("/")]
    def get_level(self, level, default_to_last=False):
        levels = self.get_levels()
        if level<len(levels):
            return levels[level]
        elif default_to_last:
            return levels[-1]
        return None
    def get_facet_levels(self):
        """Hack: some tags have 1 more component in their facet than levels?
        Hacked to dump the first component if to many facet components"""
        original_facet = [f for f in self.facet.split(".") if f!=""]
        return original_facet[(len(original_facet)-len(self.get_levels())):]
    def get_facet_level(self, level, default_to_last=False):
        facet_levels = self.get_facet_levels()
        if level<len(facet_levels):
            return facet_levels[level]
        elif default_to_last:
            return facet_levels[-1]
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
    @staticmethod
    def get_articles_per_tag(articles):
        utags = set(t for a in articles for t in a.tags)
        articles_tags = [(a,set(a.tags)) for a in articles]
        return [(t, [a for a, atags in articles_tags if t in atags]) for t in utags]





# =========================== TAG TREE ===========================

class tag_tree:

    @staticmethod
    def create_empty_node(name, parent=None, facet=None, full_name=None):
        return {
            "name": name,
            "full_name": full_name,
            "parent": parent,
            "facet": facet,
            "children": []
        }

    @staticmethod
    def get_tag_tree_nodes(tag_tree_root, dhs_tag, missing_behaviour=None):
        levels = dhs_tag.get_levels()
        facet_levels = dhs_tag.get_facet_levels()
        current_node = tag_tree_root
        tag_tree_nodes = [None]*len(levels)
        for i,l in enumerate(levels):
            child_node = None
            for c in current_node["children"]:
                if c["name"]==l:
                    child_node=c
                    break
            if child_node is None:
                if missing_behaviour is None:
                    return tag_tree_nodes
                if missing_behaviour == "create":
                    full_name=None
                    if (i+1)==len(levels):
                        full_name=dhs_tag.tag
                    child_node = tag_tree.create_empty_node(l, current_node["name"], ".".join(facet_levels[0:(i+1)]), full_name)
                    current_node["children"].append(child_node)
                if missing_behaviour == "error":
                    raise Exception("get_tag_tree_node() non-existent tag tree node for level "+l+" of tag "+dhs_tag) 
            tag_tree_nodes[i]=child_node
            current_node=child_node
        return tag_tree_nodes
    @staticmethod
    def get_tag_tree_node(tag_tree_root, dhs_tag, missing_behaviour=None):
        tag_tree_nodes = tag_tree.get_tag_tree_nodes(tag_tree_root, dhs_tag, missing_behaviour)
        return tag_tree_nodes[-1]
    @staticmethod
    def build_tag_tree(tags):
        root_node = tag_tree.create_empty_node("root")
        for t in tags:
            tag_tree.get_tag_tree_node(root_node, t, "create")
        return root_node

    @staticmethod
    def traverse_depth_first(node, recursive_function, **recursive_function_kwargs):
        """Applies recursive_function on all nodes of the tag_tree depth first
        
        recursive_function should take two arguments: the current node, and a list
        containing the result of recursive_function on the nodes' children (an empty list if node has no children)."""
        children_result = [
            tag_tree.traverse_depth_first(n, recursive_function, **recursive_function_kwargs) for n in node["children"]
        ] if "children" in node else []
        return recursive_function(node, children_result, **recursive_function_kwargs)


    @staticmethod
    def stats_articles_count(node):
        return [len(node["articles"])] if "articles" in node else [0]

    @staticmethod
    def stats_aggregator_lists(own_statistics, children_statistics):
        return [
            x+sum(cs[i] for cs in children_statistics) for i, x in enumerate(own_statistics)
        ]

    @staticmethod
    def stats_articles_by_category_proportions_curry(article_ids_by_category, categories):
        """Returns a function to return the count of articles by category
        
        Tobe used as stat_func inside tag_tree.recursive_node_statistics()
        """
        def articles_by_category_proportions(node):        
            return {
                c: set([a for a in node["articles"] if a.id in article_ids_by_category[c]])
                for c in categories
            }
        return articles_by_category_proportions

    @staticmethod
    def stats_aggregator_articles_by_category_proportions(own_statistics, children_statistics):
        return {
            c: articles_set.union(*[a_by_c[c] for a_by_c in children_statistics]) for c, articles_set in own_statistics.items()
        }

    @staticmethod
    def compute_nodes_statistics(tag_tree_root, stat_func=None, stat_aggregator_func=None):
        """Computes 3 statistics for the current node: "statistics", "children_statistics", "total_statistics" """
        if stat_func is None:
            stat_func = tag_tree.stats_articles_count
        if stat_aggregator_func is None:
            stat_aggregator_func = tag_tree.stats_aggregator_lists
        def recursive_node_statistics(node, children_statistics):
            node["children_statistics"] = children_statistics
            node["statistics"] = stat_func(node)
            node["total_statistics"] = stat_aggregator_func(node["statistics"], children_statistics)
            return node["total_statistics"]
        return tag_tree.traverse_depth_first(tag_tree_root, recursive_node_statistics)


    
    @staticmethod
    def add_articles_to_tag_tree(tag_tree_root, articles=None, articles_per_tag=None):
        """Adds an "articles" entry to each node of the tag tree, containing the list of DhsArticle having the tag
        
        Note that articles will only be added to the final node corresponding to the tag.
        For example, Laax has the tag "Entités politiques / Commune", it will only be added to the "Commune" tag tree node.""
        """
        if articles is None and articles_per_tag is None:
            raise Exception("tag_tree.add_articles_to_tag_tree() one of articles or articles_per_tag must be non-None.")
        if articles_per_tag is None:
            articles_per_tag = DhsTag.get_articles_per_tag(articles)
        def add_empty_articles_list(node, cr):
            node["articles"]=[]
        tag_tree.traverse_depth_first(tag_tree_root, add_empty_articles_list)
        for t, articles in articles_per_tag:
            tag_node = tag_tree.get_tag_tree_node(tag_tree_root, t)
            if tag_node is None:
                raise Exception("tag_tree.add_articles_to_tag_tree() non-existent tag tree node for tag: "+t.__str__()) 
            tag_node["articles"]=[a for a in articles]

    def modify_node_property(tag_tree_root, property, modifier_func):
        """Replaces each node[property] by modifier_func(node[property])

        Modifies the tag_tree_root in place, returns a revert function that, if called, reverts the
        tag_tree to its original state with each node[property] containing the original content of node[property]
        The revert function assumes the tree structure didn't change (no node added or removed).
        """
        original_articles_sequential = []
        def apply_to_node(node, cr_unused):
            original_articles_sequential.append(node[property])
            node[property] = modifier_func(node[property])
        tag_tree.traverse_depth_first(tag_tree_root, apply_to_node)
        def revert_node(node, cr_unused):
            node[property] = original_articles_sequential.pop(0)
        def revert():
            tag_tree.traverse_depth_first(tag_tree_root, revert_node)
        return revert