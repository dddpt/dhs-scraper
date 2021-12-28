

from lxml.etree import iselement

def lxml_depth_first_iterator(element, iteration_criterion):
    """iterate depth-first over an lxml element, yielding elements according to iteration_criterion()

    Considers both text and Element nodes.

    yields:
    - all descendants d of element for which iteration_criterion(d) returns True
    """
    for node in element.xpath("child::node()"):
        if iteration_criterion(node):
            yield node
        elif iselement(node):
            for n in lxml_depth_first_iterator(node, iteration_criterion):
                yield n


def is_text_or_link(node):
    if (not iselement(node)) or (node.tag=="a"):
        return True
    return False


def get_attributes_string(class_name, object_dict):
    """Unimportant utility function to format __str__() and __repr()"""
    return f"""{class_name}({', '.join([
        f"{str(k)}: {str(v)}"
        for k, v in object_dict.items()
    ])})"""


def stream_to_jsonl(jsonl_filepath, jsonable_iterable, buffer_size=100, **to_json_kwargs):
    """Saves jsonables to a jsonl file from an iterable/generator
    
    A jsonable is an object with a to_json() method
    Useful to stream scraped articles to a jsonl on-the-fly and not keep them in memory.
    Uses a buffer to avoid disk usage"""
    buffer = [None]*buffer_size
    with open(jsonl_filepath, "a") as jsonl_file:
        empty=True
        for i, a in enumerate(jsonable_iterable):
            empty=False
            if i!=0 and i%buffer_size==0:
                jsonl_file.write("\n".join(buffer)+"\n")
            buffer[i%buffer_size]= a.to_json(ensure_ascii=False, **to_json_kwargs)
        if not empty:
            jsonl_file.write("\n".join(buffer[0:((i%buffer_size)+1)])+"\n")