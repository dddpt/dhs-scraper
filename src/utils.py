

from lxml.etree import iselement

def lxml_depth_first_iterator(element, iteration_criterion):
    """iterate depth-first over an lxml element, yielding elements according to iteration_criterion()

    Considers both text and Element nodes.

    yields:
    - all descendants d of element for which iteration_criterion(d) returns True
    """
    for node in element.xpath("child::node()"):
        print(f"lxml_dpi() node: {node}, iteration_criterion(node): {iteration_criterion(node)}, iselement(node): {iselement(node)}")
        if iteration_criterion(node):
            yield node
        elif iselement(node):
            for n in lxml_depth_first_iterator(node, iteration_criterion):
                yield n

def is_text_or_link(node):
    if (not iselement(node)) or (node.tag=="a"):
        return True
    return False