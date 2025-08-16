import xml.etree.ElementTree as ET

def local(tag: str) -> str:
    if tag and tag[0] == "{":
        return tag.split("}", 1)[1]
    return tag if tag is not None else ""
    
def find_first(parent, name):
    if parent is None:
        return None
    for child in parent:
        if local(child.tag) == name:
            return child
    return None

def find_all(parent, name):
    if parent is None:
        return []
    return [child for child in parent if local(child.tag) == name]

def find_text(parent, name, default=""):
    node = find_first(parent, name)
    if node is not None and node.text:
        return node.text.strip()
    return default

def find_text_lang(parent, name, lang="en", default=""):
    if parent is None:
        return None
    for child in parent:
        if local(child.tag) == name and (child.get("lang") is None or child.get("lang") == lang):
            return (child.text or "").strip()
    return default