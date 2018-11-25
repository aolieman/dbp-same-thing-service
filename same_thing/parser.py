import re

#######
# NTriples parsing
#######

iri_pattern = r'<(?:[^\x00-\x1F<>"{}|^`\\]|\\u[0-9A-Fa-f]{4}|\\U[0-9A-Fa-f]{8})*>'
literal_capture_pattern = rf'"(?P<value>.+)"\^\^{iri_pattern}'
literal_int_pattern = rf'(?:"\d+"\^\^{iri_pattern})'
ntriple_pattern = (rf'(?P<subject>{iri_pattern})\s*'
                   rf'(?P<predicate>{iri_pattern})\s*'
                   rf'(?P<object>{iri_pattern}|{literal_int_pattern})\s*\.')

literal_capture_re = re.compile(literal_capture_pattern)
literal_int_re = re.compile(literal_int_pattern)
ntriple_re = re.compile(ntriple_pattern)


def parse_triple(ntriple_line):
    match = ntriple_re.search(ntriple_line)
    if match:
        subj, pred, obj = match.groups()
        subj = subj.strip('<>')
        pred = pred.strip('<>')
        if obj.startswith('<'):
            obj = obj.strip('<>')
        elif literal_int_re.match(obj):
            value = literal_capture_re.match(obj).group('value')
            obj = int(value)

        return subj, pred, obj
