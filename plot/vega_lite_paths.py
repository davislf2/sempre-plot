#!/usr/bin/env python3.6

"""Script to compute all possible paths through a JSON object conforming to the Vega-lite specification.

Args:
    schema_path (str): path to the JSON Schema file, e.g. "./vega-lite-v2.json"
    out_path (str): path to save the computed paths, e.g. "./vega-lite-paths.txt"
    filter: elements that should not appear in extracted paths

- The out file contains one path per line, where the elements of the path are separated by tabs.
- There should be roughly 30k paths.
- paths involving arrays will have an "items" element, indicating that subsequent keys are properties of an
  element of that array.
- TODO: need to handle circular references properly
"""

import json, csv
from jsonschema import RefResolver
from collections import deque, namedtuple, OrderedDict
import argparse


# parse command line args
arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--schema_path', default='vega-lite-v2.json')
arg_parser.add_argument('--out_path', default='visualize/data/vegalite')
arg_parser.add_argument('--filter', default=('vconcat', 'hconcat', 'layer', 'spec', 'repeat', 'condition', 'selection', 'data', 'facet'))

args = arg_parser.parse_args()

# load Vega Lite schema and reference resolver
with open(args.schema_path) as f:
    schema = json.load(f)
resolver = RefResolver.from_schema(schema)
resolve = lambda ref: resolver.resolve(ref)[1]


# represents a node in the schema
class Node(namedtuple('Node', ['schema', 'full_path'])):

    @property
    def path(self):
        # remove "anyOf"s and references
        return ('vegalite',) + tuple(key for key in self.full_path if not (key.startswith("anyOf[") or key.startswith("#/") or key=='items'))
    @property
    def pathstr(self):
        return '.'.join(self.path)

    @property
    def full_path_nice(self):
        return tuple(key if not key.startswith('#') else key[key.rfind('/'):] for key in self.full_path if not (key.startswith("anyOf[") or key=='items'))

    @property
    def description(self):
        if "description" in self.schema:
            return self.schema["description"].replace(u'\xa0', ' ')
        else:
            return ''

    @property
    def name(self):
        return self.path[-1]

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        return self.path == other.path

    def __lt__(self, other):
        return'.'.join(self.full_path) < '.'.join(other.full_path)

    def __repr__(self):
        return '.'.join(self.path) + '\t' + '.'.join(self.full_path)

def children(node):
    """Return the children of a given node.

    Args:
        node (Node)

    Returns:
        new_nodes (list[Node])
    """
    schema, full_path = node.schema, node.full_path
    child_nodes = []

    if "anyOf" in schema:
        for i, s in enumerate(schema["anyOf"]):
            child_nodes.append(Node(s, full_path + ["anyOf[{}]".format(i)]))

    # jump through reference
    if "$ref" in schema:
        ref = schema["$ref"]
        # avoid circular references
        if not (ref in full_path):
            child_nodes.append(Node(resolve(ref), full_path + [ref]))

    # arrays have "items"
    if "items" in schema:
        child_nodes.append(Node(schema["items"], full_path + ["items"]))

    # objects have "properties"
    if "properties" in schema:
        for key, s in sorted(schema["properties"].items()):
            child_nodes.append(Node(s, full_path + [key]))

    return child_nodes

def real_children(node):
    """
    changes need to happen in real path, and not just full path
    """

    queue = deque(children(node))
    child_nodes = set()

    while len(queue) > 0:
        c = queue.pop()
        if c.path != node.path:
            child_nodes.add(c)
        else:
            queue.extend(children(c))

    return child_nodes


# explore all paths, DFS
def write_all_paths():
    nodes = set()
    seed = Node(schema, [])
    queue = deque([seed])

    while len(queue) > 0:
        state = queue.pop()
        nodes.add(state)
        new_states = children(state)
        queue.extend(reversed(new_states))

    print(nodes)

    nodes = sorted(nodes)
    if args.filter:
        nodes = [node for node in nodes if all([p not in args.filter for p in node.path])]

    with open(args.out_path + '.csv', 'w') as csvfile:
        fieldnames = ['id', 'full_path', 'description']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for node in nodes:
            print(node)
            writer.writerow({'id': node.pathstr})
write_all_paths()

dependencies = OrderedDict()

def update_set(d, k, v):
    if k in d:
        d[k].update(v)
    else:
        d[k] = set(v)

def expand(node):
    tree = {}
    tree['name'] = node.name
    tree['description'] = node.description
    child_nodes = real_children(node)
    filtered_children = [c for c in child_nodes if all([p not in args.filter for p in c.path])]

    update_set(dependencies, node.pathstr, [c.pathstr for c in filtered_children])

    if len(child_nodes) == 0:
        tree['size'] = 1
        return tree
    tree['children'] = [expand(c) for c in filtered_children]
    tree['size'] = sum(c['size'] for c in tree['children'])
    return tree


with open(args.out_path + '.json', 'w') as jsonfile:
    json.dump(expand(Node(schema, [])), jsonfile)

with open(args.out_path + '.dep.json', 'w') as jsonfile:
    json.dump([{"name": k, "imports": list(v)} for k,v in dependencies.items()], jsonfile)
