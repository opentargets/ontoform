# This code recursively traverses a JSON structure, finds all arrays at any
# level of nesting, and sorts them in place.
def post_recurse(f): def r: (f | select(. != null) | r), .; r;
def post_recurse: post_recurse(.[]?);
(post_recurse | arrays) |= sort
