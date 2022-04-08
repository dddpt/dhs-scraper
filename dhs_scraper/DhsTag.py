import json


def tag_tree_create_empty_node(name, parent=None):
    return {
        "name": name,
        "parent": parent,
        "children": []
    }

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

    @staticmethod
    def get_tag_tree_node(tag_tree_root, dhs_tag, missing_behaviour=None):
        levels = dhs_tag.get_levels()
        current_node=tag_tree_root
        for l in levels:
            child_node = None
            for c in current_node["children"]:
                if c["name"]==l:
                    child_node=c
                    break
            if child_node is None:
                if missing_behaviour is None:
                    return None
                if missing_behaviour == "create":
                    child_node = tag_tree_create_empty_node(l, current_node["name"])
                    current_node["children"].append(child_node)
                if missing_behaviour == "error":
                    raise Exception("get_tag_tree_node() non-existent tag tree node for level "+l+" of tag "+dhs_tag) 
            current_node = child_node
        return current_node

    @staticmethod
    def build_tag_tree(tags):
        root_node = tag_tree_create_empty_node("root")
        for t in tags:
            DhsTag.get_tag_tree_node(root_node, t, "create")
        return root_node

    @staticmethod
    def get_articles_per_tag(articles):
        utags = set(t for a in articles for t in a.tags)
        return [(t, [a for a in articles if t in a.tags]) for t in utags]

    @staticmethod
    def add_articles_ids_to_tag_tree(tag_tree, articles=None, articles_per_tag=None):
        if articles is None and articles_per_tag is None:
            raise Exception("DhsTag.add_articles_ids_to_tag_tree() one of articles or articles_per_tag must be non-None.")
        if articles_per_tag is None:
            articles_per_tag = DhsTag.get_articles_per_tag(articles)
        for t, articles in articles_per_tag:
            tag_node = DhsTag.get_tag_tree_node(tag_tree, t)
            if tag_node is None:
                raise Exception("add_articles_ids_to_tag_tree() non-existent tag tree node for tag: "+t) 
            tag_node["articles_ids"]=[a.id for a in articles]