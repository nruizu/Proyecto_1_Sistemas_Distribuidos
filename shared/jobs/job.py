# job.py (cliente)
import re
WORD = re.compile(r"\w+", flags=re.UNICODE)

def map(line):
    for w in WORD.findall(line.lower()):
        yield (w, 1)

def reduce(key, values):
    return sum(values)
